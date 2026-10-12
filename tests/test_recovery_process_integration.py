"""Independent OS-process regressions for the durable recovery coordinator.

Run alone with ``python -m unittest discover -s tests -p
test_recovery_process_integration.py``. No test depends on another test's DB.
"""

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from workflow_engine import PersistenceError, RetryPolicy, Task, TaskEngine, TaskStatus


class RecoveryProcessIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix='.recovery-process-tests-')
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.path = self.directory / 'tasks.db'
        self.env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE effects (business_key TEXT PRIMARY KEY, value INTEGER NOT NULL)')

        def effect(key):
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('INSERT OR IGNORE INTO effects VALUES (?, 7)', (key,))
            return 7

        self.registry = {'effect': effect}

    def eventually(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail('recovery condition did not become true before deadline')

    def crash_registered_worker(self, *, initial_delay=0, idempotent=True):
        engine = TaskEngine(database_path=self.path, task_registry=self.registry,
                            retry_policy=RetryPolicy(1, initial_delay=initial_delay))
        task = engine.submit(Task('effect', self.registry['effect'], id='crashed-task',
                                  args=('business-key',), idempotent=idempotent))
        process = subprocess.run([sys.executable, '-c', '''
from contextlib import closing
import os, sqlite3, sys, time
from workflow_engine import TaskEngine
path = sys.argv[1]
def effect(key):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('INSERT OR IGNORE INTO effects VALUES (?, 7)', (key,))
    os._exit(23)  # Effect committed, task outcome not committed.
engine = TaskEngine(database_path=path, task_registry={'effect': effect})
engine.create_worker('crashed-worker', host_id='lost-host', heartbeat_interval=.02,
                     heartbeat_timeout=.25, poll_interval=.01).start()
time.sleep(10)
''', str(self.path)], env=self.env, capture_output=True, timeout=12)
        self.assertEqual(process.returncode, 23, process.stderr.decode(errors='replace'))
        session = engine.worker_registry.get('crashed-worker').session_id
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        return engine, task, session, process

    def test_process_exit_requires_confirmation_before_any_replay(self):
        engine, task, session, process = self.crash_registered_worker()
        recovery = engine.create_recovery_system(interval=.01)

        def incident_open():
            recovery.tick()
            return any(i.task_id == task.id for i in recovery.list_incidents())

        self.eventually(incident_open)
        incident = next(i for i in recovery.list_incidents() if i.task_id == task.id)
        self.assertEqual((incident.kind, incident.session_id, incident.host_id),
                         ('WAIT_STOP', session, 'lost-host'))
        for _ in range(3):
            self.assertNotIn(task.id, recovery.tick().recovered_task_ids)
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count),
                         (TaskStatus.RUNNING, 1, 0))
        reopened = TaskEngine(database_path=self.path, task_registry=self.registry)
        restored = reopened.create_recovery_system()
        self.assertEqual(next(i.id for i in restored.list_incidents() if i.task_id == task.id), incident.id)
        restored.confirm_session_stopped(session, operator='integration-supervisor',
            evidence=f'waited for child process exit, returncode={process.returncode}')
        report = restored.tick()
        self.assertIn(task.id, report.recovered_task_ids)
        worker = reopened.create_worker('replacement', host_id='healthy-host', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        self.eventually(lambda: reopened.get_task(task.id).status == TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        saved = reopened.get_task(task.id)
        self.assertEqual((saved.attempt_count, saved.retry_count, saved.result), (2, 1, 7))
        self.assertFalse(any(i.task_id == task.id for i in restored.list_incidents()))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM effects').fetchone()[0], 1)

    def test_background_verifier_recovers_only_observed_exit_and_preserves_backoff(self):
        engine, task, session, process = self.crash_registered_worker(initial_delay=.5)
        observations = []

        def verified_stop(record):
            observations.append(record.session_id)
            return f'joined subprocess exit={process.returncode}' if process.returncode == 23 else None

        recovery = engine.create_recovery_system(interval=.01, stop_verifier=verified_stop)
        self.addCleanup(lambda: self.assertTrue(recovery.shutdown(5)))
        recovery.start()
        self.eventually(lambda: engine.get_task(task.id).status == TaskStatus.RETRYING)
        self.assertTrue(recovery.shutdown(5))
        self.assertIsNone(recovery.last_error)
        saved = engine.get_task(task.id)
        self.assertIn(session, observations)
        self.assertEqual((saved.attempt_count, saved.retry_count), (1, 1))
        self.assertGreater(saved.retry_at, time.time())
        # The stored delay survives a different engine and coordinator process.
        reopened = TaskEngine(database_path=self.path, task_registry=self.registry)
        self.assertEqual(reopened.get_task(task.id).retry_at, saved.retry_at)
        self.assertEqual(reopened.create_recovery_system().tick().recovered_task_ids, ())
        worker = reopened.create_worker('replacement', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        self.eventually(lambda: reopened.get_task(task.id).status == TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        saved = reopened.get_task(task.id)
        self.assertEqual((saved.attempt_count, saved.retry_count), (2, 1))
        self.assertEqual(sum(e.event_type.value == 'RETRY_SCHEDULED' for e in saved.events), 1)

    def test_nonidempotent_crash_requires_verified_manual_result_without_another_attempt(self):
        engine, task, session, process = self.crash_registered_worker(idempotent=False)
        recovery = engine.create_recovery_system()
        engine.worker_registry.request_stop('crashed-worker', session)
        recovery.confirm_session_stopped(session, operator='integration-supervisor',
                                        evidence=f'child joined with exit {process.returncode}')
        self.assertIn(task.id, recovery.tick().recovered_task_ids)
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.FAILED, 1, 0))
        incident = next(i for i in recovery.list_incidents() if i.task_id == task.id)
        self.assertEqual((incident.kind, incident.session_id, incident.host_id),
                         ('TASK_FAILURE', session, 'lost-host'))
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='retry', operator='auditor',
                                  reason='unsafe replay even with explicit extra budget', extra_retries=1)
        with closing(sqlite3.connect(self.path)) as db:
            verified = db.execute('SELECT value FROM effects WHERE business_key=?', ('business-key',)).fetchone()[0]
        recovery.resolve_task(incident.id, action='succeed', operator='auditor',
                              reason='verified committed external effect by stable business key', result=verified)
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count, saved.result),
                         (TaskStatus.SUCCESS, 1, 0, 7))
        self.assertFalse(recovery.list_incidents())
        self.assertEqual(recovery.tick().recovered_task_ids, ())
        self.assertTrue(any(event['operator'] == 'auditor' for event in recovery.get_events()))

    def test_independent_coordinator_processes_recover_one_attempt_once(self):
        engine, task, session, process = self.crash_registered_worker()
        recovery = engine.create_recovery_system()
        engine.worker_registry.request_stop('crashed-worker', session)
        recovery.confirm_session_stopped(session, operator='integration-supervisor',
                                        evidence=f'child joined with exit {process.returncode}')
        barrier = self.directory / 'release-coordinators'
        script = '''
import json, pathlib, sys, time
from workflow_engine import TaskEngine
def effect(key): return 7
engine = TaskEngine(database_path=sys.argv[1], task_registry={'effect': effect})
recovery = engine.create_recovery_system()
pathlib.Path(sys.argv[3]).write_text('ready')
deadline = time.monotonic() + 8
while not pathlib.Path(sys.argv[2]).exists():
    if time.monotonic() > deadline: raise RuntimeError('barrier timed out')
    time.sleep(.005)
report = recovery.tick()
print(json.dumps({'recovered': report.recovered_task_ids, 'errors': report.errors}))
'''
        ready_paths = [self.directory / f'ready-{i}' for i in range(2)]
        coordinators = [subprocess.Popen(
            [sys.executable, '-c', script, str(self.path), str(barrier), str(ready)],
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for ready in ready_paths]

        def cleanup():
            for child in coordinators:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)

        self.addCleanup(cleanup)
        self.eventually(lambda: all(ready.exists() for ready in ready_paths))
        barrier.write_text('release')
        reports = []
        for child in coordinators:
            stdout, stderr = child.communicate(timeout=12)
            self.assertEqual(child.returncode, 0, stderr.decode(errors='replace'))
            reports.append(json.loads(stdout))
        self.assertEqual(sum(report['recovered'].count(task.id) for report in reports), 1)
        self.assertTrue(all(not report['errors'] for report in reports))
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.RETRYING, 1, 1))
        self.assertEqual(sum(e.event_type.value == 'INTERRUPTED' for e in saved.events), 1)
        self.assertEqual(sum(e.event_type.value == 'RETRY_SCHEDULED' for e in saved.events), 1)

    def test_recovery_event_failure_rolls_back_attempt_budget_and_can_retry_scan(self):
        engine, task, session, process = self.crash_registered_worker()
        recovery = engine.create_recovery_system()
        engine.worker_registry.request_stop('crashed-worker', session)
        recovery.confirm_session_stopped(session, operator='integration-supervisor',
                                        evidence=f'child joined with exit {process.returncode}')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""CREATE TRIGGER reject_recovery BEFORE INSERT ON task_events
                WHEN NEW.event_type='INTERRUPTED' BEGIN SELECT RAISE(ABORT,'journal unavailable'); END""")
        report = recovery.tick()
        self.assertTrue(report.errors)
        self.assertNotIn(task.id, report.recovered_task_ids)
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.RUNNING, 1, 0))
        self.assertFalse(any(e.event_type.value == 'INTERRUPTED' for e in saved.events))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER reject_recovery')
        self.assertIn(task.id, recovery.tick().recovered_task_ids)
        self.assertEqual(engine.get_task(task.id).retry_count, 1)
        self.assertEqual(recovery.tick().recovered_task_ids, ())

    def test_durable_queue_refresh_preserves_authoritative_priority_and_caller_observation(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        high = engine.submit(Task('high', abs, args=(-7,), priority=10))
        low = engine.submit(Task('low', abs, args=(-2,), priority=0))
        high.priority = -100
        high.args = (-99,)
        processed = engine.start()
        self.assertEqual([task.id for task in processed], [high.id, low.id])
        self.assertEqual((high.status, high.result, high.priority), (TaskStatus.SUCCESS, 7, 10))

    def test_legacy_process_exit_needs_exact_attempt_confirmation_and_refreshes_existing_engine_queue(self):
        engine = TaskEngine(database_path=self.path, task_registry=self.registry, retry_policy=RetryPolicy(1))
        task = engine.submit(Task('effect', self.registry['effect'], id='legacy-crash',
                                  args=('business-key',), idempotent=True))
        process = subprocess.run([sys.executable, '-c', '''
from contextlib import closing
import os, sqlite3, sys
from workflow_engine import TaskEngine
path = sys.argv[1]
def effect(key):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('INSERT OR IGNORE INTO effects VALUES (?, 7)', (key,))
    os._exit(23)
TaskEngine(database_path=path, task_registry={'effect': effect}).start()
''', str(self.path)], env=self.env, capture_output=True, timeout=12)
        self.assertEqual(process.returncode, 23, process.stderr.decode(errors='replace'))
        # Both the old engine and a new one may have queued an older snapshot.
        reopened = TaskEngine(database_path=self.path, task_registry=self.registry)
        recovery = reopened.create_recovery_system()
        recovery.tick()
        incident = next(i for i in recovery.list_incidents() if i.task_id == task.id)
        self.assertEqual((incident.kind, incident.session_id, incident.attempt_count), ('WAIT_STOP', None, 1))
        with self.assertRaises(PersistenceError):
            recovery.confirm_task_stopped(task.id, attempt_count=2, operator='supervisor',
                                          evidence='joined child but specified stale attempt')
        self.assertEqual((engine.get_task(task.id).status, engine.get_task(task.id).retry_count),
                         (TaskStatus.RUNNING, 0))
        recovery.confirm_task_stopped(task.id, attempt_count=1, operator='supervisor',
                                      evidence=f'joined legacy subprocess exit={process.returncode}')
        self.assertEqual(engine.get_task(task.id).retry_count, 1)
        processed = engine.start()
        self.assertTrue(processed)
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count, saved.result),
                         (TaskStatus.SUCCESS, 2, 1, 7))
        with self.assertRaises(PersistenceError):
            recovery.confirm_task_stopped(task.id, attempt_count=1, operator='supervisor',
                                          evidence='stale duplicate confirmation after success')
        self.assertFalse(recovery.list_incidents())
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM effects').fetchone()[0], 1)

    def test_engines_created_before_and_after_recovery_refresh_their_durable_retry_snapshot(self):
        original, task, session, process = self.crash_registered_worker()
        before = TaskEngine(database_path=self.path, task_registry=self.registry)
        recovery = original.create_recovery_system()
        original.worker_registry.request_stop('crashed-worker', session)
        recovery.confirm_session_stopped(session, operator='supervisor',
                                        evidence=f'joined crashed subprocess exit={process.returncode}')
        self.assertIn(task.id, recovery.tick().recovered_task_ids)
        after = TaskEngine(database_path=self.path, task_registry=self.registry)
        # The first engine had no ready tasks when constructed; start refreshes it.
        self.assertTrue(before.start())
        saved = before.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.SUCCESS, 2, 1))
        # The second engine queued a retry which another engine has since completed.
        # Its stale snapshot must never cause a third business invocation.
        after.start()
        self.assertEqual((after.get_task(task.id).attempt_count, after.get_task(task.id).retry_count), (2, 1))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM effects').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
