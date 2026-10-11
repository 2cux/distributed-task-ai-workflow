"""Cross-component workloads, fault injection and transaction races.

Use actual stores, executors, registries and background consumers. Events/barriers
control execution order; injected failures target SQLite writes or control I/O.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, Lock
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from workflow_engine import (IdempotencyConflict, PersistenceError, RetryPolicy,
    SQLiteTaskStore, StaleExecutionError, Task, TaskEngine, TaskStatus,
    WorkerRegistry, WorkerRegistryError, WorkerSessionMismatch, WorkerStatus)
from workflow_engine import WorkerMonitor


def eventually(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError('integration condition did not become true before deadline')


class IntegrationFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.task-tests-')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'tasks.db'
        self.workers = []
        self.releases = []
        self.addCleanup(self.cleanup_workers)

    def cleanup_workers(self):
        for release in self.releases:
            release.set()
        failures = []
        for worker in self.workers:
            try:
                if not worker.shutdown(5):
                    failures.append(f'{worker.worker_id} did not stop')
            except Exception as error:
                failures.append(f'{worker.worker_id}: {error}')
        self.assertEqual(failures, [], 'background consumers must clean up even when a test fails')

    def engine(self, handler=abs, **kwargs):
        return TaskEngine(database_path=self.path, task_registry={'handler': handler}, **kwargs)

    def worker(self, engine, worker_id):
        worker = engine.create_worker(worker_id, heartbeat_interval=0.03,
                                      heartbeat_timeout=2, poll_interval=0.01)
        self.workers.append(worker)
        worker.start()
        return worker

    def gate(self):
        release = Event()
        self.releases.append(release)
        return release

    def stop_workers(self):
        for worker in self.workers:
            worker.stop()
        for worker in self.workers:
            self.assertTrue(worker.join(5))

    def assert_history(self, task):
        events = task.events
        self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(sum(e.event_type.value == 'STARTED' for e in events), task.attempt_count)
        self.assertEqual(sum(e.event_type.value == 'RETRY_SCHEDULED' for e in events), task.retry_count)


class MultiWorkerWorkloadIntegrationTests(IntegrationFixture):
    def test_observers_never_read_partial_attempt_result_or_event_commits(self):
        counts = Counter()
        lock = Lock()
        def handler(key):
            with lock:
                counts[key] += 1
                attempt = counts[key]
            time.sleep(.005)
            if attempt == 1:
                raise ValueError('first attempt fails')
            return key
        engine = self.engine(handler, retry_policy=RetryPolicy(1, initial_delay=.01))
        tasks = [engine.submit(Task(str(i), handler, args=(str(i),))) for i in range(12)]
        done = Event()
        def observe():
            snapshots = 0
            while not done.is_set():
                for task in tasks:
                    snapshot = engine.get_task(task.id)
                    last = snapshot.events[-1]
                    self.assertEqual((snapshot.status, snapshot.attempt_count, snapshot.retry_count),
                                     (last.status, last.attempt_count, last.retry_count))
                    self.assert_history(snapshot)
                    if snapshot.status is TaskStatus.SUCCESS:
                        self.assertEqual(snapshot.result, task.args[0])
                        self.assertEqual(last.event_type.value, 'SUCCEEDED')
                    snapshots += 1
            return snapshots
        with ThreadPoolExecutor(max_workers=2) as pool:
            observers = [pool.submit(observe) for _ in range(2)]
            try:
                for i in range(3):
                    self.worker(self.engine(handler), f'worker-{i}')
                eventually(lambda: all(engine.get_task(t.id).status is TaskStatus.SUCCESS for t in tasks))
            finally:
                done.set()
            self.assertTrue(all(f.result(timeout=5) > 0 for f in observers))
        self.stop_workers()
        self.assertEqual(set(counts.values()), {2})

    def test_mixed_success_failure_retry_schedule_and_restart_with_four_consumers(self):
        calls, active, peaks = Counter(), Counter(), Counter()
        lock = Lock()
        def handler(key, failures):
            with lock:
                calls[key] += 1
                attempt = calls[key]
                active[key] += 1
                peaks[key] = max(peaks[key], active[key])
            try:
                time.sleep(0.003)
                if attempt <= failures:
                    raise ValueError('transient')
                return {'key': key, 'attempt': attempt}
            finally:
                with lock:
                    active[key] -= 1
        producer = self.engine(handler, retry_policy=RetryPolicy(2, initial_delay=0.02))
        for i in range(4):
            # Different engine defaults must not change persisted retry budgets.
            self.worker(self.engine(handler, retry_policy=RetryPolicy(0)), f'worker-{i}')
        cases = []
        for i in range(48):
            failures = i % 4
            budget = 0 if i % 7 == 0 else 2
            task = producer.submit(Task(str(i), handler, id=f'job-{i}', args=(str(i), failures),
                                        max_retries=budget, priority=i % 5,
                                        scheduled_at=time.time() + 0.08 if i % 6 == 0 else None))
            cases.append((task.id, failures, budget))
        eventually(lambda: all(producer.get_task(key).status in (TaskStatus.SUCCESS, TaskStatus.FAILED)
                               for key, _, _ in cases))
        self.stop_workers()
        reopened = self.engine(handler)
        for key, failures, budget in cases:
            current = reopened.get_task(key)
            expected_attempts = min(failures + 1, budget + 1)
            self.assertEqual(current.attempt_count, expected_attempts)
            self.assertEqual(calls[key.removeprefix('job-')], expected_attempts)
            self.assertEqual(peaks[key.removeprefix('job-')], 1)
            self.assertEqual(current.status, TaskStatus.SUCCESS if failures <= budget else TaskStatus.FAILED)
            self.assert_history(current)
            if current.scheduled_at is not None:
                from datetime import datetime
                started = next(e for e in current.events if e.event_type.value == 'STARTED')
                self.assertGreaterEqual(datetime.fromisoformat(started.timestamp).timestamp(), current.scheduled_at)
        self.assertEqual(reopened.pending_count, 0)
        self.assertTrue(all(w.last_error is None for w in self.workers))

    def test_concurrent_duplicate_submissions_before_during_and_after_execution(self):
        entered, release = Event(), self.gate()
        calls = 0
        def handler():
            nonlocal calls
            calls += 1
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test gate')
            return 'one effect'
        engines = [self.engine(handler) for _ in range(4)]
        def submit(index):
            return engines[index % 4].submit(Task('deduplicate', handler, id='business-key'))
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual({t.id for t in pool.map(submit, range(16))}, {'business-key'})
        for i, engine in enumerate(engines):
            self.worker(engine, f'worker-{i}')
        self.assertTrue(entered.wait(3))
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertTrue(all(t.status is TaskStatus.RUNNING for t in pool.map(submit, range(16))))
        with self.assertRaises(IdempotencyConflict):
            engines[0].submit(Task('different-definition', handler, id='business-key'))
        release.set()
        eventually(lambda: engines[0].get_task('business-key').status is TaskStatus.SUCCESS)
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertTrue(all(t.status is TaskStatus.SUCCESS for t in pool.map(submit, range(16))))
        self.stop_workers()
        self.assertEqual(calls, 1)
        self.assert_history(engines[0].get_task('business-key'))

    def test_due_priority_fifo_and_late_high_priority_submission(self):
        entered, release, order = Event(), self.gate(), []
        def handler(key):
            if key == 'gate':
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('test gate')
            else:
                order.append(key)
            return key
        engine = self.engine(handler)
        engine.submit(Task('gate', handler, args=('gate',)))
        worker = self.worker(engine, 'a')
        self.assertTrue(entered.wait(3))
        for key, priority in [('low', 0), ('high-1', 5), ('high-2', 5), ('late-high', 10)]:
            engine.submit(Task(key, handler, id=key, args=(key,), priority=priority))
        engine.submit(Task('future', handler, id='future', args=('future',), priority=100,
                           scheduled_at=time.time() + 60))
        release.set()
        eventually(lambda: engine.get_task('low').status is TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(3))
        self.assertEqual(order, ['late-high', 'high-1', 'high-2', 'low'])
        self.assertEqual(engine.get_task('future').attempt_count, 0)
        self.assertEqual(engine.pending_count, 1)

    def test_stop_leaves_delayed_retry_for_replacement_with_original_budget(self):
        calls = 0
        def handler():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError('first attempt')
            return calls
        producer = self.engine(handler, retry_policy=RetryPolicy(1, initial_delay=0.5))
        task = producer.submit(Task('retry', handler))
        old = self.worker(producer, 'reused-id')
        eventually(lambda: producer.get_task(task.id).status is TaskStatus.RETRYING)
        before = producer.get_task(task.id)
        self.assertTrue(old.shutdown(3))
        replacement = self.worker(self.engine(handler, retry_policy=RetryPolicy(0)), 'reused-id')
        self.assertNotEqual(old.session_id, replacement.session_id)
        eventually(lambda: producer.get_task(task.id).status is TaskStatus.SUCCESS)
        self.assertTrue(replacement.shutdown(3))
        current = producer.get_task(task.id)
        self.assertEqual((calls, current.attempt_count, current.retry_count), (2, 2, 1))
        self.assertEqual(before.retry_count, current.retry_count)
        self.assert_history(current)

    def test_soft_timeout_retry_does_not_overlap_and_other_worker_progresses(self):
        entered, release = Event(), self.gate()
        calls, active, peak = 0, 0, 0
        lock = Lock()
        def handler(key):
            nonlocal calls, active, peak
            if key == 'healthy':
                return 'healthy'
            with lock:
                calls += 1
                number = calls
                active += 1
                peak = max(peak, active)
            try:
                if number == 1:
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError('test gate')
                return number
            finally:
                with lock:
                    active -= 1
        producer = self.engine(handler, retry_policy=RetryPolicy(1))
        slow = producer.submit(Task('slow', handler, args=('slow',), timeout=0.02))
        a = self.worker(producer, 'a')
        self.assertTrue(entered.wait(3))
        b = self.worker(self.engine(handler), 'b')
        healthy = producer.submit(Task('healthy', handler, args=('healthy',)))
        eventually(lambda: producer.get_task(healthy.id).status is TaskStatus.SUCCESS)
        # Keep the first business invocation alive past its timeout.
        first = producer.worker_registry.get('a').last_heartbeat_at
        eventually(lambda: producer.worker_registry.get('a').last_heartbeat_at > first + 0.06)
        self.assertEqual(producer.get_task(slow.id).status, TaskStatus.RUNNING)
        self.assertEqual(calls, 1)
        release.set()
        eventually(lambda: producer.get_task(slow.id).status is TaskStatus.SUCCESS)
        self.stop_workers()
        current = producer.get_task(slow.id)
        self.assertEqual((calls, peak, current.attempt_count, current.retry_count), (2, 1, 2, 1))
        self.assertTrue(any(e.details.get('error_type') == 'TaskTimeoutError' for e in current.events))


class MultiWorkerFaultIntegrationTests(IntegrationFixture):
    def test_consumer_start_failure_and_cleanup_write_failure_leave_safe_local_lifecycle(self):
        from threading import Thread
        engine = self.engine()
        task = engine.submit(Task('absolute', abs, args=(-7,)))
        worker = engine.create_worker('startup-failure')
        with engine._store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_start_cleanup BEFORE INSERT ON worker_session_events
                WHEN NEW.state='STOPPING' BEGIN SELECT RAISE(ABORT,'cleanup unavailable'); END""")
        original_start = Thread.start
        def start(thread):
            if thread.name == 'worker-startup-failure':
                raise RuntimeError('consumer startup failed')
            return original_start(thread)
        with patch('workflow_engine.reliable_worker.Thread.start', new=start):
            with self.assertRaisesRegex(RuntimeError, 'consumer startup failed'):
                worker.start()
        self.assertEqual(worker.status, WorkerStatus.STOPPED)
        self.assertTrue(worker.join(0))
        self.assertIsInstance(worker.last_error, sqlite3.IntegrityError)
        eventually(lambda: not worker._heartbeat_thread.is_alive())
        self.assertEqual(engine.get_task(task.id).attempt_count, 0)
        self.assertEqual(engine.worker_registry.get_session(worker.session_id).state, 'ACTIVE')
        with self.assertRaises(PersistenceError):
            engine.recover_worker(worker.session_id)
        with engine._store._transaction() as db:
            db.execute('DROP TRIGGER reject_start_cleanup')

    def test_monitor_start_failure_remains_safely_shutdownable(self):
        engine = self.engine()
        monitor = WorkerMonitor(engine.worker_registry)
        with patch('workflow_engine.reliable_worker.Thread.start', side_effect=RuntimeError('no threads')):
            with self.assertRaisesRegex(RuntimeError, 'no threads'):
                monitor.start()
        self.assertTrue(monitor.shutdown(0))

    def test_completion_transaction_failure_quarantines_effect_and_other_worker_continues(self):
        calls = Counter()
        def handler(key):
            calls[key] += 1
            return key
        engine = self.engine(handler, retry_policy=RetryPolicy(2))
        task = engine.submit(Task('unsafe', handler, id='unsafe', args=('unsafe',)))
        store = engine._store
        with store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_result BEFORE INSERT ON task_events
                WHEN NEW.task_id='unsafe' AND NEW.event_type='SUCCEEDED'
                BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END""")
        old = self.worker(engine, 'old')
        self.assertTrue(old.join(5))
        self.assertIsInstance(old.last_error, sqlite3.IntegrityError)
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        self.assertEqual(engine.worker_registry.get_session(old.session_id).state, 'STOPPED')
        fresh = self.worker(self.engine(handler), 'fresh')
        healthy = engine.submit(Task('healthy', handler, id='healthy', args=('healthy',)))
        eventually(lambda: engine.get_task(healthy.id).status is TaskStatus.SUCCESS)
        engine.recover_worker(old.session_id)
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.FAILED)
        self.stop_workers()
        self.assertEqual(calls, {'unsafe': 1, 'healthy': 1})
        self.assertFalse(any(e.event_type.value == 'SUCCEEDED' for e in engine.get_events(task.id)))

    def test_heartbeat_io_failure_stops_new_claims_but_does_not_claim_drain_complete(self):
        entered, release, calls = Event(), self.gate(), []
        def handler(key):
            calls.append(key)
            if key == 'blocked':
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('test gate')
            return key
        engine = self.engine(handler)
        task = engine.submit(Task('blocked', handler, id='blocked', args=('blocked',)))
        old = self.worker(engine, 'old')
        self.assertTrue(entered.wait(3))
        with patch.object(engine.worker_registry, 'heartbeat', side_effect=OSError('control-plane unavailable')):
            eventually(lambda: isinstance(old.last_error, OSError))
        engine.submit(Task('next', handler, id='next', args=('next',)))
        self.assertFalse(old.join(0.02))
        self.assertNotEqual(engine.worker_registry.get_session(old.session_id).state, 'STOPPED')
        with self.assertRaises(PersistenceError):
            engine.recover_worker(old.session_id)
        fresh = self.worker(self.engine(handler), 'fresh')
        eventually(lambda: engine.get_task('next').status is TaskStatus.SUCCESS)
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        release.set()
        self.assertTrue(old.join(3))
        self.stop_workers()
        self.assertEqual(Counter(calls), {'blocked': 1, 'next': 1})

    def test_failed_stop_confirmation_remains_unrecoverable_until_supervisor_confirms(self):
        engine = self.engine()
        old = self.worker(engine, 'old')
        with engine._store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_stop BEFORE INSERT ON worker_session_events
                WHEN NEW.state='STOPPED' BEGIN SELECT RAISE(ABORT,'stop evidence unavailable'); END""")
        self.assertTrue(old.shutdown(3))
        self.assertEqual(old.status, WorkerStatus.STOPPED)
        self.assertEqual(engine.worker_registry.get_session(old.session_id).state, 'STOPPING')
        self.assertIsInstance(old.last_error, sqlite3.IntegrityError)
        with self.assertRaises(PersistenceError):
            engine.recover_worker(old.session_id)
        with engine._store._transaction() as db:
            db.execute('DROP TRIGGER reject_stop')
        engine.worker_registry.confirm_stopped('old', old.session_id, evidence='supervisor verified worker.join returned true')
        self.assertEqual(engine.recover_worker(old.session_id), [])

    def test_fence_event_failure_rolls_back_permissions_and_task_outcome_still_commits(self):
        store = SQLiteTaskStore(self.path, {'handler': abs})
        registry = WorkerRegistry(self.path, clock=lambda: self.now)
        self.now = 100
        store.workers = registry
        session = registry.register('a', heartbeat_timeout=1)
        store.submit(Task('absolute', abs, args=(-7,)))
        task = store.claim_next(session.session_id, 'token')
        self.now = 102
        registry.detect_overdue()
        with store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_fence BEFORE INSERT ON worker_session_events
                WHEN NEW.state='FENCED' BEGIN SELECT RAISE(ABORT,'audit unavailable'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            registry.fence('a', session.session_id)
        self.assertEqual(registry.get_session(session.session_id).state, 'SUSPECT')
        task.status, task.result = TaskStatus.SUCCESS, 7
        store.finish(task, 'token')
        self.assertEqual(store.get(task.id).result, 7)
        self.assertFalse(any(e['state'] == 'FENCED' for e in registry.get_session_events(session.session_id)))


class MultiWorkerRaceIntegrationTests(IntegrationFixture):
    def race(self, *actions):
        barrier = Barrier(len(actions))
        def run(action):
            barrier.wait(timeout=5)
            try:
                return action()
            except (WorkerRegistryError, StaleExecutionError) as error:
                return error
        with ThreadPoolExecutor(max_workers=len(actions)) as pool:
            return list(pool.map(run, actions))

    def prepare(self, index):
        store = SQLiteTaskStore(self.path, {'handler': abs})
        registry = WorkerRegistry(self.path, clock=lambda: self.now)
        store.workers = registry
        self.now = 100
        session = registry.register(f'worker-{index}', heartbeat_timeout=1)
        store.submit(Task('absolute', abs, id=f'job-{index}', args=(-7,), idempotent=True), 1)
        task = store.claim_next(session.session_id, f'token-{index}')
        self.now = 102
        registry.detect_overdue()
        return store, registry, session, task

    def test_heartbeat_and_fence_race_has_one_valid_winner(self):
        for i in range(12):
            store, registry, session, task = self.prepare(i)
            heartbeat, fence = self.race(lambda: registry.heartbeat(session.worker_id, session.session_id),
                                         lambda: registry.fence(session.worker_id, session.session_id))
            state = registry.get_session(session.session_id).state
            if state == 'ACTIVE':
                self.assertFalse(isinstance(heartbeat, Exception))
                self.assertIsInstance(fence, WorkerRegistryError)
            else:
                self.assertEqual(state, 'FENCED')
                self.assertIsInstance(heartbeat, WorkerSessionMismatch)
                self.assertFalse(isinstance(fence, Exception))
            self.assertEqual(store.get(task.id).status, TaskStatus.RUNNING)
            with self.assertRaises(PersistenceError):
                store.recover_worker(session.session_id)

    def test_finish_and_fence_race_never_records_a_revoked_completion(self):
        for i in range(12):
            store, registry, session, task = self.prepare(i)
            task.status, task.result = TaskStatus.SUCCESS, 7
            finish, fence = self.race(lambda: store.finish(task, f'token-{i}'),
                                     lambda: registry.fence(session.worker_id, session.session_id))
            current = store.get(task.id)
            if isinstance(finish, StaleExecutionError):
                self.assertEqual(current.status, TaskStatus.RUNNING)
                self.assertFalse(any(e.event_type.value == 'SUCCEEDED' for e in current.events))
            else:
                self.assertIsNone(finish)
                self.assertEqual((current.status, current.result), (TaskStatus.SUCCESS, 7))
            self.assertFalse(isinstance(fence, Exception))
            registry.confirm_stopped(session.worker_id, session.session_id, evidence='test has no live invocation')
            self.assertEqual(len(store.recover_worker(session.session_id)), int(isinstance(finish, StaleExecutionError)))
            # Finish pending recovered attempts so the next iteration claims its own task.
            recovered = store.get(task.id)
            if recovered.status is TaskStatus.RETRYING:
                next_session = registry.register(f'replacement-{i}')
                claimed = store.claim_next(next_session.session_id, 'new-token')
                claimed.status, claimed.result = TaskStatus.SUCCESS, 7
                store.finish(claimed, 'new-token')

    def test_multiple_coordinators_recover_same_session_once_while_new_id_starts(self):
        store, registry, session, task = self.prepare(0)
        registry.fence(session.worker_id, session.session_id)
        registry.confirm_stopped(session.worker_id, session.session_id, evidence='test invocation stopped')
        engines = [self.engine() for _ in range(4)]
        outcomes = self.race(*(lambda e=e: e.recover_worker(session.session_id) for e in engines),
                              lambda: registry.register(session.worker_id))
        self.assertEqual(sum(len(batch) for batch in outcomes[:-1]), 1)
        self.assertEqual(store.get(task.id).retry_count, 1)
        replacement = outcomes[-1]
        self.assertNotEqual(replacement.session_id, session.session_id)
        stale_updates = self.race(lambda: registry.heartbeat(session.worker_id, session.session_id),
                                  lambda: registry.request_stop(session.worker_id, session.session_id))
        self.assertTrue(all(isinstance(e, WorkerSessionMismatch) for e in stale_updates))
        self.assertEqual(registry.get(session.worker_id).session_id, replacement.session_id)


if __name__ == '__main__':
    unittest.main()
