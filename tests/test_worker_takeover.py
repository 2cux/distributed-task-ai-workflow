"""Independent, deterministic protocol tests; no heartbeat sleeps required."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest

from workflow_engine import (Task, TaskStatus, RetryPolicy, SQLiteTaskStore, WorkerRegistry,
    WorkerRegistryError, WorkerNotReady, WorkerSessionMismatch, PersistenceError,
    StaleExecutionError, UncertainExecutionError)


class WorkerTakeoverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.task-tests-')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'tasks.db'
        self.store = SQLiteTaskStore(self.path, {'abs': abs})
        self.now = 100
        self.registry = WorkerRegistry(self.path, clock=lambda: self.now)
        self.store.workers = self.registry
        self.a = self.registry.register('a', heartbeat_timeout=10)

    def claim(self, *, idempotent=True, retries=2, task_id='job'):
        task = self.store.submit(Task('absolute', abs, id=task_id, args=(-7,), idempotent=idempotent), retries)
        return self.store.claim_next(self.a.session_id, 'old-token')

    def fence(self):
        self.now = 111
        self.registry.detect_overdue()
        self.registry.fence('a', self.a.session_id)

    def confirm(self):
        self.registry.confirm_stopped('a', self.a.session_id, evidence='supervisor observed process exit and joined')

    def test_suspicion_preserves_attempt_and_accepts_completion(self):
        old = self.claim()
        self.now = 110  # Exact deadline has not expired.
        self.assertEqual(self.registry.detect_overdue(), [])
        self.now = 111
        self.assertEqual(len(self.registry.detect_overdue()), 1)
        self.assertEqual(self.registry.detect_overdue(), [])
        with self.assertRaises(PersistenceError):
            self.store.recover_worker(self.a.session_id)
        with self.assertRaises(WorkerNotReady):
            self.store.claim_next(self.a.session_id, 'other')
        self.assertEqual(self.store.get(old.id).status, TaskStatus.RUNNING)
        old.result = 7
        old.status = TaskStatus.SUCCESS
        self.store.finish(old, 'old-token')
        self.assertEqual(self.store.get(old.id).result, 7)
        self.assertEqual(self.store.get(old.id).attempt_count, 1)

    def test_resumed_heartbeat_cancels_suspicion_and_stale_fence(self):
        self.now = 111
        self.registry.detect_overdue()
        self.registry.heartbeat('a', self.a.session_id)
        with self.assertRaises(WorkerRegistryError):
            self.registry.fence('a', self.a.session_id)
        task = self.claim()
        self.assertIsNotNone(task)

    def test_fenced_worker_cannot_commit_heartbeat_claim_or_recover(self):
        old = self.claim()
        self.fence()
        self.registry.fence('a', self.a.session_id)  # Repeated fencing is safe.
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.heartbeat('a', self.a.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            self.store.claim_next(self.a.session_id, 'new-token')
        old.status = TaskStatus.SUCCESS
        old.result = 99
        with self.assertRaises(StaleExecutionError):
            self.store.finish(old, 'old-token')
        with self.assertRaises(PersistenceError):
            self.store.recover_worker(self.a.session_id)
        with self.assertRaises(PersistenceError):
            self.store.recover_interrupted()  # Offline API cannot bypass stages.
        self.assertEqual(self.store.get(old.id).attempt_count, 1)
        self.assertFalse(any(e.event_type.value == 'SUCCEEDED' for e in self.store.get_events(old.id)))

    def test_recovery_is_scoped_idempotent_and_old_result_cannot_overwrite(self):
        old = self.claim()
        self.fence()
        b = self.registry.register('b')
        other = self.store.submit(Task('other', abs, id='other', args=(-8,)))
        self.store.claim_next(b.session_id, 'b-token')
        self.confirm()
        with ThreadPoolExecutor(max_workers=2) as pool:
            recovered = list(pool.map(lambda _: self.store.recover_worker(self.a.session_id), range(2)))
        self.assertEqual(sum(map(len, recovered)), 1)
        self.assertEqual(self.store.get(old.id).retry_count, 1)
        self.assertEqual(self.store.get(other.id).status, TaskStatus.RUNNING)
        # Reusing the human-readable ID produces a new independent incarnation.
        new_session = self.registry.register('a')
        new = self.store.claim_next(new_session.session_id, 'new-token')
        self.assertEqual(new.attempt_count, 2)
        new.status = TaskStatus.SUCCESS
        new.result = 7
        self.store.finish(new, 'new-token')
        old.status = TaskStatus.SUCCESS
        old.result = 999
        with self.assertRaises(StaleExecutionError):
            self.store.finish(old, 'old-token')
        self.assertEqual(self.store.get(old.id).result, 7)
        self.assertEqual(self.store.recover_worker(self.a.session_id), [])
        self.assertEqual(len(self.registry.list_sessions()), 3)

    def test_nonidempotent_and_exhausted_tasks_are_quarantined(self):
        for idempotent, retries, task_id in [(False, 2, 'unsafe'), (True, 0, 'exhausted')]:
            self.claim(idempotent=idempotent, retries=retries, task_id=task_id)
        self.fence()
        self.confirm()
        recovered = self.store.recover_worker(self.a.session_id)
        self.assertEqual(len(recovered), 2)
        for task in recovered:
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.retry_count, 0)
            self.assertIsInstance(self.store.get(task.id).error, UncertainExecutionError)

    def test_direct_stop_cannot_bypass_drain_and_confirmation_needs_evidence(self):
        self.claim()
        with self.assertRaises(WorkerRegistryError):
            self.registry.stop('a', self.a.session_id)
        with self.assertRaises(WorkerRegistryError):
            self.confirm()
        self.fence()
        with self.assertRaises(ValueError):
            self.registry.confirm_stopped('a', self.a.session_id, evidence=' ')
        self.assertEqual(self.registry.get_session(self.a.session_id).state, 'FENCED')

    def test_outcome_event_failure_rolls_back_recovery_and_budget(self):
        self.claim()
        self.fence()
        self.confirm()
        with self.store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_recovery BEFORE INSERT ON task_events
                WHEN NEW.event_type='INTERRUPTED' BEGIN SELECT RAISE(ABORT,'blocked'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.recover_worker(self.a.session_id)
        self.assertEqual(self.store.get('job').status, TaskStatus.RUNNING)
        self.assertEqual(self.store.get('job').retry_count, 0)

    def test_overdue_claim_blocked_even_before_monitor_runs(self):
        self.store.submit(Task('absolute', abs, args=(-7,)))
        self.now = 111
        with self.assertRaises(WorkerNotReady):
            self.store.claim_next(self.a.session_id, 'token')

    def test_restart_preserves_fence_and_confirmed_recovery(self):
        self.claim()
        self.fence()
        reopened = SQLiteTaskStore(self.path, {'abs': abs})
        with self.assertRaises(PersistenceError):
            reopened.recover_worker(self.a.session_id)
        self.confirm()
        self.assertEqual(reopened.recover_worker(self.a.session_id)[0].status, TaskStatus.RETRYING)

    def test_delayed_recovery_preserves_persisted_backoff(self):
        self.store.submit(Task('absolute', abs, id='job', args=(-7,), idempotent=True), 2,
                          retry_policy=RetryPolicy(2, initial_delay=60))
        self.store.claim_next(self.a.session_id, 'token')
        self.fence()
        self.confirm()
        recovered = self.store.recover_worker(self.a.session_id)[0]
        self.assertIsNotNone(recovered.retry_at)
        b = self.registry.register('b')
        self.assertIsNone(self.store.claim_next(b.session_id, 'b-token'))

    def test_worker_crashes_during_graceful_drain_can_still_be_detected(self):
        self.claim()
        self.registry.request_stop('a', self.a.session_id)
        self.now = 111
        self.registry.detect_overdue()
        self.assertEqual(self.registry.get_session(self.a.session_id).state, 'SUSPECT')
        self.registry.heartbeat('a', self.a.session_id)
        self.assertEqual(self.registry.get_session(self.a.session_id).state, 'STOPPING')
        with self.assertRaises(WorkerSessionMismatch):
            self.store.claim_next(self.a.session_id, 'token')
        self.now = 122
        self.registry.detect_overdue()
        self.registry.fence('a', self.a.session_id)
        self.confirm()
        self.assertEqual(len(self.store.recover_worker(self.a.session_id)), 1)

    def test_registered_claim_event_failure_rolls_back_attempt_and_owner(self):
        self.store.submit(Task('absolute', abs, id='job', args=(-7,)))
        with self.store._transaction() as db:
            db.execute("""CREATE TRIGGER reject_claim BEFORE INSERT ON task_events
                WHEN NEW.event_type='STARTED' BEGIN SELECT RAISE(ABORT,'blocked'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.claim_next(self.a.session_id, 'token')
        task = self.store.get('job')
        self.assertEqual((task.status, task.attempt_count), (TaskStatus.PENDING, 0))

    def test_registry_from_pull_request_schema_is_upgraded_without_losing_identity(self):
        path = Path(self.tmp.name) / 'legacy.db'
        with sqlite3.connect(path) as db:
            db.execute("""CREATE TABLE worker_registry (worker_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('ACTIVE','STOPPED')),
                registered_at REAL NOT NULL, last_heartbeat_at REAL NOT NULL, stopped_at REAL)""")
            db.execute("INSERT INTO worker_registry VALUES ('legacy','old-session','ACTIVE',100,100,NULL)")
        db.close()
        migrated = WorkerRegistry(path, clock=lambda: 111)
        self.assertEqual(migrated.get('legacy').session_id, 'old-session')
        self.assertEqual(migrated.get_session('old-session').state, 'ACTIVE')
        migrated.heartbeat('legacy', 'old-session')
        self.assertEqual(migrated.get_session('old-session').last_heartbeat_at, 111)


if __name__ == '__main__':
    unittest.main()
