"""Real OS-process competition and abrupt exit, separate from thread tests."""
from pathlib import Path
from contextlib import closing
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from workflow_engine import (Task, TaskEngine, TaskStatus, RetryPolicy, PersistenceError,
                             WorkerRegistry, SQLiteTaskStore)


class MultiWorkerProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.task-tests-')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'tasks.db'
        self.env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}

    def eventually(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail('process integration condition did not become true before deadline')

    def test_force_kill_after_business_effect_recovers_without_duplicate_effect(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE business_effects (business_key TEXT PRIMARY KEY, value INTEGER NOT NULL)')
        def handler(key):
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('INSERT OR IGNORE INTO business_effects VALUES (?,7)', (key,))
            return 7
        engine = TaskEngine(database_path=self.path, task_registry={'handler': handler}, retry_policy=RetryPolicy(1))
        crashed = engine.submit(Task('business', handler, id='crashed-task', args=('stable-business-key',), idempotent=True))
        script = '''
from contextlib import closing
import sqlite3, sys, time
from workflow_engine import TaskEngine
path = sys.argv[1]
def handler(key):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('INSERT OR IGNORE INTO business_effects VALUES (?,7)', (key,))
    while True:
        time.sleep(1)  # Business effect committed, outcome not committed.
engine = TaskEngine(database_path=path, task_registry={'handler': handler})
worker = engine.create_worker('victim', heartbeat_interval=.03, heartbeat_timeout=.3)
worker.start()
while True:
    time.sleep(1)
'''
        process = subprocess.Popen([sys.executable, '-c', script, str(self.path)], env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        def effect_exists():
            with engine._store._connection() as db:
                return db.execute("SELECT 1 FROM business_effects WHERE business_key='stable-business-key'").fetchone() is not None
        self.eventually(effect_exists)
        self.assertIsNone(process.poll())
        healthy = engine.create_worker('healthy', heartbeat_interval=.03, heartbeat_timeout=2, poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(healthy.shutdown(5)))
        healthy.start()
        another = engine.submit(Task('other-business', handler, args=('another-key',)))
        self.eventually(lambda: engine.get_task(another.id).status is TaskStatus.SUCCESS)
        record = engine.worker_registry.get('victim')
        self.assertEqual(engine.get_task(crashed.id).attempt_count, 1)
        process.kill()
        process.communicate(timeout=5)
        self.assertIsNotNone(process.returncode)
        def suspected():
            engine.worker_registry.detect_overdue()
            return engine.worker_registry.get_session(record.session_id).state == 'SUSPECT'
        self.eventually(suspected)
        engine.worker_registry.fence('victim', record.session_id)
        with self.assertRaises(PersistenceError):
            engine.recover_worker(record.session_id)
        engine.worker_registry.confirm_stopped('victim', record.session_id,
            evidence=f'supervisor killed and waited for process {process.pid}, exit={process.returncode}')
        engine.recover_worker(record.session_id)
        self.eventually(lambda: engine.get_task(crashed.id).status is TaskStatus.SUCCESS)
        self.assertTrue(healthy.shutdown(5))
        saved = engine.get_task(crashed.id)
        self.assertEqual((saved.attempt_count, saved.retry_count, saved.result), (2, 1, 7))
        with engine._store._connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM business_effects WHERE business_key='stable-business-key'").fetchone()[0], 1)
        starts = [e for e in saved.events if e.event_type.value == 'STARTED']
        self.assertEqual([e.details['worker_session'] for e in starts], [record.session_id, healthy.session_id])
        self.assertIsNone(healthy.last_error)

    def test_crash_after_committed_retry_does_not_lose_or_double_charge_retry(self):
        def handler():
            return 42
        engine = TaskEngine(database_path=self.path, task_registry={'handler': handler},
                            retry_policy=RetryPolicy(1, initial_delay=.15))
        task = engine.submit(Task('retry', handler, id='retry-crash'))
        script = '''
import os, sys
from workflow_engine import SQLiteTaskStore
from workflow_engine.persistence import PersistentExecutor
def handler():
    raise ValueError('transient before committed retry')
store = SQLiteTaskStore(sys.argv[1], {'handler': handler})
worker = store.workers.register('retry-victim')
task = store.claim_next(worker.session_id, 'retry-token')
PersistentExecutor(store, None, None).execute_claimed(task, 'retry-token')
os._exit(23)  # RETRYING committed; no in-memory enqueue or shutdown happened.
'''
        process = subprocess.run([sys.executable, '-c', script, str(self.path)], env=self.env,
                                 capture_output=True, timeout=10)
        self.assertEqual(process.returncode, 23, process.stderr.decode(errors='replace'))
        before = engine.get_task(task.id)
        self.assertEqual((before.status, before.attempt_count, before.retry_count), (TaskStatus.RETRYING, 1, 1))
        # No takeover is needed: the failed attempt committed a ready retry and
        # released ownership before the process died.
        replacement = engine.create_worker('retry-replacement', heartbeat_interval=.03, heartbeat_timeout=2)
        self.addCleanup(lambda: self.assertTrue(replacement.shutdown(5)))
        replacement.start()
        self.eventually(lambda: engine.get_task(task.id).status is TaskStatus.SUCCESS)
        self.assertTrue(replacement.shutdown(5))
        current = engine.get_task(task.id)
        self.assertEqual((current.attempt_count, current.retry_count, current.result), (2, 1, 42))
        self.assertEqual(sum(e.event_type.value == 'RETRY_SCHEDULED' for e in current.events), 1)

    def test_independent_processes_claim_each_attempt_exactly_once(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        tasks = [engine.submit(Task('absolute', abs, id=f'job-{i}', args=(-i,))) for i in range(20)]
        script = '''
import sys, time
from workflow_engine import TaskEngine, TaskStatus
engine = TaskEngine(database_path=sys.argv[1], task_registry={'abs': abs})
worker = engine.create_worker(sys.argv[2], heartbeat_interval=.03, heartbeat_timeout=2, poll_interval=.01)
worker.start()
deadline = time.monotonic()+8
while time.monotonic() < deadline:
    if all(engine.get_task('job-'+str(i)).status is TaskStatus.SUCCESS for i in range(20)):
        break
    time.sleep(.01)
else:
    raise RuntimeError('queue did not drain')
assert worker.shutdown(3)
assert worker.last_error is None, worker.last_error
'''
        processes = [subprocess.Popen([sys.executable, '-c', script, str(self.path), worker_id],
                                     env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                     for worker_id in ('process-a', 'process-b')]
        def cleanup():
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)
        self.addCleanup(cleanup)
        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, stderr.decode(errors='replace'))
        for task in tasks:
            current = engine.get_task(task.id)
            self.assertEqual((current.status, current.attempt_count), (TaskStatus.SUCCESS, 1))
            starts = [e for e in current.events if e.event_type.value == 'STARTED']
            self.assertEqual(len(starts), 1)
            self.assertIsNotNone(starts[0].details['worker_session'])
        self.assertEqual(len(engine.worker_registry.list_workers()), 2)

    def test_abrupt_exit_requires_fence_then_observed_process_exit_before_replay(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs}, retry_policy=RetryPolicy(1))
        task = engine.submit(Task('absolute', abs, id='crash', args=(-7,), idempotent=True))
        script = '''
import os, sys
from workflow_engine import SQLiteTaskStore
store = SQLiteTaskStore(sys.argv[1], {'abs': abs})
worker = store.workers.register('crashed', heartbeat_timeout=30)
task = store.claim_next(worker.session_id, 'crashed-token')
assert task is not None
abs(*task.args)  # Effects occurred, but no outcome transaction was committed.
os._exit(19)
'''
        process = subprocess.run([sys.executable, '-c', script, str(self.path)], env=self.env,
                                 capture_output=True, timeout=10)
        self.assertEqual(process.returncode, 19, process.stderr.decode(errors='replace'))
        record = engine.worker_registry.get('crashed')
        observed = WorkerRegistry(self.path, clock=lambda: record.last_heartbeat_at+31)
        observed.detect_overdue()
        with self.assertRaises(PersistenceError):
            engine.recover_worker(record.session_id)
        observed.fence('crashed', record.session_id)
        with self.assertRaises(PersistenceError):
            engine.recover_worker(record.session_id)
        observed.confirm_stopped('crashed', record.session_id,
                                 evidence=f'subprocess exited, returncode={process.returncode}')
        engine.recover_worker(record.session_id)
        store = SQLiteTaskStore(self.path, {'abs': abs})
        replacement = store.workers.register('replacement')
        claimed = store.claim_next(replacement.session_id, 'new-token')
        self.assertEqual((claimed.attempt_count, claimed.retry_count), (2, 1))
        claimed.result = abs(*claimed.args)
        claimed.status = TaskStatus.SUCCESS
        store.finish(claimed, 'new-token')
        self.assertEqual(engine.get_task(task.id).result, 7)
        self.assertEqual(engine.recover_worker(record.session_id), [])


if __name__ == '__main__':
    unittest.main()
