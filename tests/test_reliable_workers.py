"""Runtime integration tests independent of the deterministic takeover tests."""
from pathlib import Path
from threading import Event, Lock
import tempfile
import time
import unittest
from unittest.mock import patch

from workflow_engine import (Task, TaskEngine, TaskStatus, RetryPolicy, WorkerMonitor,
                             WorkerStatus, StaleExecutionError)


def eventually(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError('condition did not become true before deadline')


class ReliableWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.task-tests-')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'tasks.db'

    def engine(self, handler=abs, **kwargs):
        return TaskEngine(database_path=self.path, task_registry={'handler': handler}, **kwargs)

    def worker(self, engine, worker_id):
        worker = engine.create_worker(worker_id, heartbeat_interval=0.02,
                                      heartbeat_timeout=0.5, poll_interval=0.01)
        self.addCleanup(lambda: worker.shutdown(5))
        worker.start()
        return worker

    def test_workers_see_submissions_from_another_engine_after_start(self):
        calls = []
        lock = Lock()
        def handler(number):
            with lock:
                calls.append(number)
            return number * 2
        a = self.engine(handler)
        b = self.engine(handler)
        workers = [self.worker(a, 'a'), self.worker(b, 'b')]
        producer = self.engine(handler)
        tasks = [producer.submit(Task(str(i), handler, args=(i,))) for i in range(30)]
        eventually(lambda: all(producer.get_task(t.id).status is TaskStatus.SUCCESS for t in tasks))
        for worker in workers:
            self.assertTrue(worker.shutdown(3))
        self.assertEqual(sorted(calls), list(range(30)))
        self.assertEqual(producer.pending_count, 0)
        self.assertEqual(sum(w.processed_count for w in workers), 30)
        for task in tasks:
            current = producer.get_task(task.id)
            self.assertEqual(current.attempt_count, 1)
            self.assertEqual(current.result, task.args[0] * 2)
            start = next(e for e in current.events if e.event_type.value == 'STARTED')
            self.assertIn(start.details['worker_session'], [w.session_id for w in workers])

    def test_long_running_task_keeps_heartbeating_and_stop_waits_for_drain(self):
        entered, release = Event(), Event()
        def handler():
            entered.set()
            release.wait(5)
            return 42
        engine = self.engine(handler)
        task = engine.submit(Task('long', handler, timeout=0.05))
        worker = self.worker(engine, 'a')
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        first = engine.worker_registry.get('a').last_heartbeat_at
        eventually(lambda: engine.worker_registry.get('a').last_heartbeat_at > first + worker.heartbeat_timeout)
        self.assertEqual(engine.worker_registry.get_session(worker.session_id).state, 'ACTIVE')
        self.assertFalse(worker.shutdown(0.01))
        self.assertNotEqual(engine.worker_registry.get_session(worker.session_id).state, 'STOPPED')
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        release.set()
        self.assertTrue(worker.join(3))
        self.assertEqual(worker.status, WorkerStatus.STOPPED)
        self.assertEqual(engine.worker_registry.get_session(worker.session_id).state, 'STOPPED')

    def test_fenced_live_invocation_is_not_replayed_until_runtime_drains(self):
        entered, release = Event(), Event()
        active = 0
        peak = 0
        calls = 0
        lock = Lock()
        def handler():
            nonlocal active, peak, calls
            with lock:
                active += 1
                calls += 1
                peak = max(peak, active)
                number = calls
            entered.set()
            release.wait(5)
            with lock:
                active -= 1
            return number
        engine = self.engine(handler, retry_policy=RetryPolicy(1))
        task = engine.submit(Task('controlled', handler, idempotent=True))
        old = self.worker(engine, 'old')
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        # A control-plane partition ends the heartbeat thread, not the callable.
        with patch.object(engine.worker_registry, 'heartbeat', side_effect=OSError('partition')):
            eventually(lambda: old.last_error is not None)
        record = engine.worker_registry.get_session(old.session_id)
        with patch.object(engine.worker_registry, '_clock', return_value=record.last_heartbeat_at + 1):
            engine.worker_registry.detect_overdue()
            engine.worker_registry.fence('old', old.session_id)
        fresh = self.worker(engine, 'fresh')
        with self.assertRaises(RuntimeError):
            engine.recover_worker(old.session_id)
        self.assertEqual(calls, 1)
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        release.set()
        self.assertTrue(old.join(3))
        self.assertIsInstance(old.last_error, StaleExecutionError)
        engine.recover_worker(old.session_id)
        eventually(lambda: engine.get_task(task.id).status is TaskStatus.SUCCESS)
        self.assertTrue(fresh.shutdown(3))
        self.assertEqual((calls, peak), (2, 1))
        self.assertEqual(engine.get_task(task.id).result, 2)
        self.assertEqual(fresh.processed_count, 1)

    def test_persisted_retry_is_picked_up_without_local_requeue(self):
        count = 0
        def handler():
            nonlocal count
            count += 1
            if count == 1:
                raise ValueError('retry')
            return count
        engine = self.engine(handler, retry_policy=RetryPolicy(1, initial_delay=0.03))
        task = engine.submit(Task('retry', handler))
        self.worker(engine, 'a')
        eventually(lambda: engine.get_task(task.id).status is TaskStatus.SUCCESS)
        current = engine.get_task(task.id)
        self.assertEqual((current.attempt_count, current.retry_count, current.result), (2, 1, 2))

    def test_control_plane_stop_drains_without_error_or_claiming_pending_task(self):
        entered, release = Event(), Event()

        def handler():
            entered.set()
            release.wait(5)
            return 42

        engine = self.engine(handler)
        first = engine.submit(Task('running', handler))
        worker = self.worker(engine, 'controlled-stop')
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        pending = engine.submit(Task('pending', handler))
        engine.worker_registry.request_stop(worker.worker_id, worker.session_id)
        self.assertFalse(worker.join(.01))
        self.assertEqual(engine.get_task(first.id).status, TaskStatus.RUNNING)
        release.set()
        self.assertTrue(worker.join(3))
        self.assertIsNone(worker.last_error)
        self.assertEqual(engine.get_task(first.id).result, 42)
        self.assertEqual(engine.get_task(pending.id).attempt_count, 0)
        self.assertEqual(engine.get_task(pending.id).status, TaskStatus.PENDING)
        self.assertEqual(engine.worker_registry.get_session(worker.session_id).state, 'STOPPED')

    def test_standalone_monitor_only_marks_suspicion(self):
        engine = self.engine()
        record = engine.worker_registry.register('abandoned', heartbeat_timeout=0.01)
        monitor = WorkerMonitor(engine.worker_registry, interval=0.01)
        self.addCleanup(lambda: monitor.shutdown(2))
        monitor.start()
        eventually(lambda: engine.worker_registry.get_session(record.session_id).state == 'SUSPECT')
        self.assertIsNone(engine.worker_registry.get_session(record.session_id).stopped_evidence)
        self.assertIsNone(monitor.last_error)

    def test_worker_lifecycle_and_configuration_validation(self):
        engine = self.engine()
        for kwargs in ({'heartbeat_interval': True}, {'poll_interval': float('nan')},
                       {'heartbeat_interval': 10, 'heartbeat_timeout': 10}):
            with self.assertRaises((TypeError, ValueError)):
                engine.create_worker('a', **kwargs)
        worker = engine.create_worker('a')
        worker.stop()
        self.assertTrue(worker.join(0))
        with self.assertRaises(RuntimeError):
            worker.start()
        self.assertEqual(engine.worker_registry.list_workers(), [])

    def test_thread_start_failure_leaves_a_stopped_session_and_no_attempt(self):
        engine = self.engine()
        task = engine.submit(Task('absolute', abs, args=(-7,)))
        worker = engine.create_worker('a')
        with patch('workflow_engine.reliable_worker.Thread.start', side_effect=RuntimeError('thread unavailable')):
            with self.assertRaises(RuntimeError):
                worker.start()
        self.assertTrue(worker.join(0))
        self.assertEqual(engine.worker_registry.get_session(worker.session_id).state, 'STOPPED')
        self.assertEqual(engine.get_task(task.id).attempt_count, 0)


if __name__ == '__main__':
    unittest.main()
