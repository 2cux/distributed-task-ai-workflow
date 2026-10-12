"""Real HTTP and process boundaries; workers never share the service's SQLite file."""
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from workflow_engine import (SQLiteTaskStore, Task, TaskStatus, WorkerRegistry,
                             StaleExecutionError, PersistenceError)
from workflow_engine.recovery import RecoverySystem
from workflow_engine.remote import (RecoveryService, RemoteTaskStore, RemoteWorker,
                                    RemoteAuthenticationError, RemoteTransportError)


class RemoteRecoveryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix='.remote-recovery-tests-')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'central.db'
        self.token = 'integration-secret-for-localhost-only'
        self.now = 100.0

    def service(self, registry):
        self.store = SQLiteTaskStore(self.path, registry)
        self.store.workers = WorkerRegistry(self.path, clock=lambda: self.now)
        self.endpoint = RecoveryService(self.store, self.token, request_timeout=.5).start()
        self.addCleanup(self.endpoint.shutdown)
        return self.endpoint

    def remote_store(self, registry, token=None):
        return RemoteTaskStore(self.endpoint.url, registry, token or self.token, request_timeout=.5)

    def worker(self, registry, worker_id='remote', **options):
        worker = RemoteWorker(self.endpoint.url, registry, self.token, worker_id,
            host_id='remote-host', heartbeat_interval=.03, heartbeat_timeout=1,
            poll_interval=.01, request_timeout=.5, **options)
        self.addCleanup(self.cleanup_worker, worker)
        return worker

    @staticmethod
    def cleanup_worker(worker):
        try:
            worker.stop()
        except RemoteTransportError:
            pass
        worker.join(5)

    def wait_for(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('condition did not become true before deadline')
            time.sleep(.01)

    def test_remote_worker_executes_local_handler_and_central_retry_policy(self):
        # The service's callable is a registry contract; only the remote worker executes it.
        def server_action():
            raise AssertionError('server must never execute remote business code')

        calls = []

        def worker_action():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('transient remote failure')
            return {'host': 'remote-host', 'value': 9}

        self.service({'action': server_action})
        self.store.submit(Task('remote-action', server_action, id='remote-success'), 1)
        worker = self.worker({'action': worker_action})
        worker.start()
        self.wait_for(lambda: self.store.get('remote-success').status is TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        saved = self.store.get('remote-success')
        self.assertEqual(saved.result, {'host': 'remote-host', 'value': 9})
        self.assertEqual((saved.attempt_count, saved.retry_count, len(calls)), (2, 1, 2))
        self.assertIsNone(worker.last_error)
        self.assertEqual(self.store.workers.get_session(worker.session_id).host_id, 'remote-host')
        self.assertEqual(self.store.workers.get_session(worker.session_id).state, 'STOPPED')

    def test_exited_remote_subprocess_is_replayed_only_after_supervisor_join(self):
        self.service({'action': abs})
        self.store.submit(Task('remote-crash', abs, id='crashed', args=(-7,), idempotent=True), 1)
        script = '''
import os, sys, time
from workflow_engine.remote import RemoteWorker
def action(value):
    os._exit(37)
worker = RemoteWorker(sys.argv[1], {'action': action}, sys.argv[2], 'process-worker',
    host_id='host-outside-server', heartbeat_interval=.03, heartbeat_timeout=1, poll_interval=.01)
worker.start()
time.sleep(15)
'''
        process = subprocess.run([sys.executable, '-c', script, self.endpoint.url, self.token],
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src')),
            capture_output=True, timeout=10)
        self.assertEqual(process.returncode, 37, process.stderr.decode(errors='replace'))
        session = self.store.workers.list_sessions()[0]
        self.assertEqual(session.host_id, 'host-outside-server')
        self.now = 102
        recovery = RecoverySystem(self.store)
        recovery.tick()
        self.assertEqual(self.store.get('crashed').status, TaskStatus.RUNNING)
        self.assertEqual(self.store.get('crashed').retry_count, 0)
        recovery.stop_verifier = lambda observed: (
            'parent joined subprocess with observed exit code 37'
            if observed.session_id == session.session_id and process.returncode == 37 else None)
        self.assertEqual(recovery.tick().recovered_task_ids, ('crashed',))
        self.assertEqual(recovery.tick().recovered_task_ids, ())
        worker = self.worker({'action': abs}, worker_id='replacement')
        worker.start()
        self.wait_for(lambda: self.store.get('crashed').status is TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        saved = self.store.get('crashed')
        self.assertEqual((saved.result, saved.attempt_count, saved.retry_count), (7, 2, 1))

    def test_network_partition_cannot_replay_a_still_running_business_call(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def action():
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test invocation was not released')
            return 8

        self.service({'action': action})
        self.store.submit(Task('partition', action, id='partition', idempotent=True), 1)
        worker = self.worker({'action': action})
        worker.start()
        self.assertTrue(entered.wait(3))
        self.endpoint.shutdown()
        self.wait_for(lambda: worker.last_error is not None)
        self.assertIsInstance(worker.last_error, RemoteTransportError)
        self.now = 102
        recovery = RecoverySystem(self.store, stop_verifier=lambda session: None)
        for _ in range(3):
            self.assertEqual(recovery.tick().recovered_task_ids, ())
        self.assertEqual(self.store.get('partition').status, TaskStatus.RUNNING)
        self.assertEqual((self.store.get('partition').attempt_count,
                          self.store.get('partition').retry_count), (1, 0))
        self.assertFalse(worker.join(.01))
        # Only after the callable exits can the local supervisor attest drain.
        release.set()
        self.assertTrue(worker.join(5))
        recovery.stop_verifier = lambda session: 'supervisor joined drained remote worker thread'
        self.assertEqual(recovery.tick().recovered_task_ids, ('partition',))
        self.assertEqual(self.store.get('partition').retry_count, 1)

    def test_authentication_and_registry_mismatch_do_not_claim_tasks(self):
        self.service({'action': abs})
        self.store.submit(Task('safe', abs, id='safe', args=(-2,)))
        with self.assertRaises(RemoteAuthenticationError):
            self.remote_store({'action': abs}, 'wrong-secret').workers.register('intruder')
        self.assertEqual(self.store.workers.list_sessions(), [])
        with self.assertRaises(PersistenceError) as raised:
            self.remote_store({'different': abs}).workers.register('incomplete-deployment')
        self.assertNotIsInstance(raised.exception, RemoteAuthenticationError)
        self.assertEqual(self.store.workers.list_sessions(), [])
        self.assertEqual(self.store.get('safe').attempt_count, 0)

    def test_old_owner_and_forged_retry_state_cannot_override_authoritative_attempt(self):
        self.service({'action': abs})
        self.store.submit(Task('fenced', abs, id='fenced', args=(-4,), idempotent=True), 1)
        remote = self.remote_store({'action': abs})
        first = remote.workers.register('first', heartbeat_timeout=1, host_id='host-one')
        old = remote.claim_next(first.session_id, 'old-owner')
        self.now = 102
        recovery = RecoverySystem(self.store)
        recovery.tick()
        old.status, old.result = TaskStatus.SUCCESS, 999
        with self.assertRaises(StaleExecutionError):
            remote.finish(old, 'old-owner')
        recovery.confirm_session_stopped(first.session_id, operator='supervisor', evidence='joined old process')
        recovery.tick()
        second = remote.workers.register('second', heartbeat_timeout=1, host_id='host-two')
        current = remote.claim_next(second.session_id, 'current-owner')
        self.assertEqual(current.attempt_count, 2)
        with self.assertRaises(StaleExecutionError):
            remote.finish(old, 'old-owner')
        # Mutable client counters and retry configuration cannot add budget.
        current.status, current.error = TaskStatus.FAILED, RuntimeError('failed again')
        current.status = TaskStatus.RETRYING
        current.retry_count, current._persisted_retry_limit = 999, 999
        current.kwargs['injected'] = 'must not alter server definition'
        remote.finish(current, 'current-owner')
        saved = self.store.get('fenced')
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.FAILED, 2, 1))
        self.assertEqual(saved.kwargs, {})
        remote.workers.request_stop('second', second.session_id)
        remote.workers.confirm_stopped('second', second.session_id, evidence='no invocation started in protocol test')

    def test_nonlocal_plaintext_bind_requires_explicit_secured_transport(self):
        store = SQLiteTaskStore(self.path, {'action': abs})
        with self.assertRaises(ValueError):
            RecoveryService(store, self.token, host='0.0.0.0')

    def test_manual_extra_budget_is_honored_by_remote_executor_after_restart(self):
        calls = []

        def action():
            calls.append(1)
            if len(calls) <= 2:
                raise RuntimeError('two failures before reconciliation completes')
            return 12

        self.service({'action': action})
        self.store.submit(Task('manual-budget', action, id='manual-budget',
                               max_retries=0, idempotent=True))
        first = self.worker({'action': action}, worker_id='before-grant')
        first.start()
        self.wait_for(lambda: self.store.get('manual-budget').status is TaskStatus.FAILED)
        self.assertTrue(first.shutdown(5))
        recovery = RecoverySystem(self.store)
        recovery.tick()
        incident = recovery.list_incidents()[0]
        recovery.resolve_task(incident.id, action='retry', extra_retries=2,
                              operator='operator', reason='explicitly grant two safe retries')
        # A fresh remote process sees the immutable original max_retries=0 and
        # must honor the separately persisted operator grant of two retries.
        replacement = self.worker({'action': action}, worker_id='after-grant')
        replacement.start()
        self.wait_for(lambda: self.store.get('manual-budget').status is TaskStatus.SUCCESS)
        self.assertTrue(replacement.shutdown(5))
        saved = self.store.get('manual-budget')
        self.assertEqual((saved.result, saved.attempt_count, saved.retry_count), (12, 3, 2))
        with self.store._connection() as db:
            import json
            definition = json.loads(db.execute('SELECT definition FROM tasks WHERE id=?',
                                               ('manual-budget',)).fetchone()[0])
        self.assertEqual(definition['max_retries'], 0)

    def test_custom_persistence_error_after_business_effect_is_never_replayed(self):
        class BusinessCommitUncertain(PersistenceError):
            pass

        effects = []

        def action():
            effects.append('external-effect')
            raise BusinessCommitUncertain('effect committed; acknowledgement failed')

        self.service({'action': action})
        self.store.submit(Task('uncertain', action, id='uncertain'), 3)
        worker = self.worker({'action': action})
        worker.start()
        self.wait_for(lambda: self.store.get('uncertain').status is TaskStatus.FAILED)
        self.assertTrue(worker.shutdown(5))
        saved = self.store.get('uncertain')
        self.assertEqual((saved.attempt_count, saved.retry_count), (1, 0))
        self.assertEqual(effects, ['external-effect'])
        self.assertIn('BusinessCommitUncertain', str(saved.error))


if __name__ == '__main__':
    unittest.main()
