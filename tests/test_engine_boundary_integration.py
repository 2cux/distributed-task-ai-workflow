"""Public engine boundaries using real SQLite, consumers and producer processes."""

from collections import Counter
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from threading import Event, Lock
import time
from concurrent.futures import ThreadPoolExecutor
import unittest

from workflow_engine import RetryPolicy, Task, TaskEngine, TaskStatus


class EngineBoundaryIntegrationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix='.boundary-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'tasks.db'
        self.workers = []
        self.gates = []
        self.addCleanup(self.drain)

    def drain(self):
        for gate in self.gates:
            gate.set()
        for worker in self.workers:
            self.assertTrue(worker.shutdown(5), 'consumer did not drain')

    def gate(self):
        gate = Event()
        self.gates.append(gate)
        return gate

    def engine(self, handler, **options):
        return TaskEngine(database_path=self.path,
                          task_registry={'handler': handler}, **options)

    def consumer(self, engine, identity):
        worker = engine.create_worker(identity, heartbeat_interval=.05,
                                      heartbeat_timeout=5, poll_interval=.01)
        self.workers.append(worker)
        worker.start()
        return worker

    def eventually(self, predicate):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail('integration condition did not complete within 10 seconds')

    def assert_history(self, engine, key, attempts, retries):
        events = engine.get_events(key)
        self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))
        counts = Counter(e.event_type.value for e in events)
        self.assertEqual(counts['SUBMITTED'], 1)
        self.assertEqual(counts['STARTED'], attempts)
        self.assertEqual(counts['RETRY_SCHEDULED'], retries)
        saved = engine.get_task(key)
        self.assertEqual((saved.attempt_count, saved.retry_count), (attempts, retries))
        self.assertEqual(saved.events, tuple(events))
        self.assertEqual(events[-1].status, saved.status)

    def test_batch_start_and_registered_consumer_share_ownership_and_retries(self):
        entered, release = Event(), self.gate()
        calls, active, peak = Counter(), Counter(), Counter()
        lock = Lock()

        def handler(key):
            with lock:
                calls[key] += 1
                attempt = calls[key]
                active[key] += 1
                peak[key] = max(peak[key], active[key])
            try:
                if key == 'blocked':
                    entered.set()
                    if not release.wait(10):
                        raise TimeoutError('test gate')
                elif attempt == 1:
                    raise ValueError('transient')
                return {'key': key, 'attempt': attempt}
            finally:
                with lock:
                    active[key] -= 1

        producer = self.engine(handler, retry_policy=RetryPolicy(1, initial_delay=.02))
        keys = ['blocked'] + [f'job-{i}' for i in range(16)]
        producer.submit_many(Task(key, handler, id=key, args=(key,),
                                  priority=100 if key == 'blocked' else 0) for key in keys)
        # Both entry points have a local snapshot of the same pending tasks.
        batch = self.engine(handler, max_workers=3)
        worker = self.consumer(self.engine(handler), 'continuous')
        self.assertTrue(entered.wait(5))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(batch.start)
            try:
                self.eventually(lambda: all(producer.get_task(k).status is TaskStatus.SUCCESS
                                           for k in keys[1:]))
                self.assertEqual(producer.get_task('blocked').status, TaskStatus.RUNNING)
            finally:
                release.set()
            future.result(timeout=5)
        self.eventually(lambda: producer.get_task('blocked').status is TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        # The producer still holds stale queue entries; draining them must not replay work.
        producer.start()
        reopened = self.engine(handler)
        for key in keys:
            attempts = 1 if key == 'blocked' else 2
            self.assertEqual((calls[key], peak[key], active[key]), (attempts, 1, 0))
            self.assert_history(reopened, key, attempts, attempts - 1)
            self.assertEqual(reopened.get_task(key).result, {'key': key, 'attempt': attempts})
        self.assertEqual(reopened.pending_count, 0)
        self.assertIsNone(worker.last_error)

    def test_live_independent_producers_deduplicate_while_consumers_execute(self):
        calls = Counter()
        lock = Lock()

        def handler(value):
            with lock:
                calls[value] += 1
            time.sleep(.003)
            return abs(value)

        engine = self.engine(handler)
        for i in range(2):
            self.consumer(self.engine(handler), f'consumer-{i}')
        script = '''
import json, sys
from workflow_engine import Task, TaskEngine, IdempotencyConflict
engine = TaskEngine(database_path=sys.argv[1], task_registry={'handler': abs})
for repeat in range(3):
    for i in range(32):
        engine.submit(Task('absolute', abs, id=f'job-{i}', args=(-i,)))
try:
    engine.submit(Task('absolute', abs, id='job-0', args=(999,)))
except IdempotencyConflict:
    print(json.dumps({'submissions': 96, 'conflict_rejected': True}))
else:
    raise AssertionError('definition conflict was accepted')
'''
        env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
        processes = []

        def cleanup_processes():
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)

        self.addCleanup(cleanup_processes)
        for _ in range(2):
            processes.append(subprocess.Popen([sys.executable, '-c', script, str(self.path)],
                                             env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, stderr.decode(errors='replace'))
            self.assertEqual(json.loads(stdout), {'submissions': 96, 'conflict_rejected': True})
        self.eventually(lambda: all(engine.get_task(f'job-{i}') is not None and
                                   engine.get_task(f'job-{i}').status is TaskStatus.SUCCESS
                                   for i in range(32)))
        self.drain()
        self.assertEqual(calls, Counter({-i: 1 for i in range(32)}))
        reopened = self.engine(handler)
        for i in range(32):
            self.assert_history(reopened, f'job-{i}', 1, 0)
            self.assertEqual(reopened.get_task(f'job-{i}').result, i)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(), [('ok',)])

    def test_invalid_results_do_not_replay_effects_or_block_healthy_and_retrying_jobs(self):
        calls = Counter()
        lock = Lock()

        def handler(kind):
            with lock:
                calls[kind] += 1
            if kind == 'object':
                return object()
            if kind == 'nan':
                return {'value': float('nan')}
            if kind == 'failure':
                raise ValueError('business failure')
            return {'ok': True}

        engine = self.engine(handler, retry_policy=RetryPolicy(2))
        kinds = ('object', 'nan', 'failure', 'healthy')
        engine.submit_many(Task(k, handler, id=k, args=(k,)) for k in kinds)
        worker = self.consumer(engine, 'mixed-results')
        self.eventually(lambda: all(engine.get_task(k).status in (TaskStatus.SUCCESS, TaskStatus.FAILED)
                                   for k in kinds))
        self.assertTrue(worker.shutdown(5))
        reopened = self.engine(handler, retry_policy=RetryPolicy(10))
        for kind in kinds:
            attempts = 3 if kind == 'failure' else 1
            self.assertEqual(calls[kind], attempts)
            self.assert_history(reopened, kind, attempts, attempts - 1)
            saved = reopened.get_task(kind)
            self.assertEqual(saved.status, TaskStatus.SUCCESS if kind == 'healthy' else TaskStatus.FAILED)
            if kind in ('object', 'nan'):
                self.assertEqual(saved.events[-1].details['error_type'], 'PersistenceError')
                self.assertIsNone(saved.result)
            history = reopened.get_events(kind)
            reopened.submit(Task(kind, handler, id=kind, args=(kind,)))
            self.assertEqual(reopened.get_events(kind), history)
        self.assertEqual(reopened.start(), [])
        self.assertEqual(calls, Counter(object=1, nan=1, failure=3, healthy=1))
        self.assertIsNone(worker.last_error)

    def test_incremental_history_across_running_retry_success_and_restart(self):
        entered = [Event(), Event()]
        releases = [self.gate(), self.gate()]
        calls = 0

        def handler():
            nonlocal calls
            index = calls
            calls += 1
            entered[index].set()
            if not releases[index].wait(10):
                raise TimeoutError('test gate')
            if index == 0:
                raise ValueError('retry once')
            return {'done': True}

        engine = self.engine(handler, retry_policy=RetryPolicy(1))
        task = engine.submit(Task('incremental', handler, id='incremental'))
        collected = engine.get_events(task.id)
        worker = self.consumer(engine, 'history')
        self.assertTrue(entered[0].wait(5))
        collected += engine.get_events(task.id, after_sequence=collected[-1].sequence)
        self.assertEqual(collected[-1].event_type.value, 'STARTED')
        releases[0].set()
        self.assertTrue(entered[1].wait(5))
        collected += engine.get_events(task.id, after_sequence=collected[-1].sequence)
        self.assertEqual([e.event_type.value for e in collected[-3:]],
                         ['FAILED', 'RETRY_SCHEDULED', 'STARTED'])
        self.assertEqual(engine.get_events(task.id, after_sequence=collected[-1].sequence), [])
        releases[1].set()
        self.eventually(lambda: engine.get_task(task.id).status is TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        reopened = self.engine(handler)
        collected += reopened.get_events(task.id, after_sequence=collected[-1].sequence)
        self.assertEqual(collected, reopened.get_events(task.id))
        self.assert_history(reopened, task.id, 2, 1)
        self.assertEqual(collected[3].details['error_message'], 'retry once')
        self.assertEqual(collected[-1].details['result'], {'done': True})
        self.assertEqual(calls, 2)
        self.assertIsNone(worker.last_error)
