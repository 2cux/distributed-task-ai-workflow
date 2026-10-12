"""Independent cross-host recovery and incarnation-isolation regressions.

Host fault domains are simulated against one local SQLite journal. Stopped-session
evidence comes from the test supervisor rather than a heartbeat timeout.
"""

from pathlib import Path
import tempfile
import time
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, OutputRef, PersistenceError, RetryPolicy,
                             StaleExecutionError, Task, TaskEngine, TaskStatus, WorkerRegistry,
                             WorkflowStatus)


class RecoveryHostIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix='.recovery-host-tests-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'tasks.db'
        self.engine = TaskEngine(database_path=self.path, task_registry={'abs': abs},
                                 retry_policy=RetryPolicy(2))
        self.now = time.time()
        self.registry = WorkerRegistry(self.path, clock=lambda: self.now)
        self.engine._store.workers = self.registry
        self.recovery = self.engine.create_recovery_system(interval=.01)

    def claim(self, worker_id, host_id, task_id):
        session = self.registry.register(worker_id, host_id=host_id, heartbeat_timeout=10)
        self.engine.submit(Task('absolute', abs, id=task_id, args=(-7,), idempotent=True))
        task = self.engine._store.claim_next(session.session_id, f'{task_id}-owner')
        self.assertEqual(task.id, task_id)
        return session, task

    def eventually(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail('host recovery condition did not become true before deadline')

    def test_failure_committed_during_suspicion_releases_hold_for_scheduled_retry(self):
        session, task = self.claim('slow', 'host-slow', 'grace-retry')
        recovery = self.engine.create_recovery_system(fence_after=30)
        self.now += 11
        recovery.tick()
        self.assertEqual(self.registry.get_session(session.session_id).state, 'SUSPECT')
        self.assertEqual(len(recovery.list_incidents()), 1)
        # The old invocation actually finished. Its ordinary atomic failure /
        # retry commit is authoritative, even before the heartbeat recovers.
        task.status = TaskStatus.FAILED
        task.error = RuntimeError('transient failure after completing invocation')
        RetryPolicy(2).begin_retry(task)
        self.engine._store.finish(task, 'grace-retry-owner')
        recovery.tick()
        self.assertFalse(recovery.list_incidents())
        worker = self.engine.create_worker('replacement', host_id='healthy', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        self.eventually(lambda: self.engine.get_task(task.id).status is TaskStatus.SUCCESS)
        saved = self.engine.get_task(task.id)
        self.assertEqual((saved.result, saved.attempt_count, saved.retry_count), (7, 2, 1))

    def test_host_confirmation_is_scoped_to_explicit_sessions_and_healthy_host_continues(self):
        lost_a, first = self.claim('lost-a', 'host-lost', 'first')
        lost_b, second = self.claim('lost-b', 'host-lost', 'second')
        self.now += 11
        report = self.recovery.tick()
        self.assertFalse(report.recovered_task_ids)
        incidents = {i.task_id: i for i in self.recovery.list_incidents() if i.task_id}
        self.assertEqual({incidents[first.id].kind, incidents[second.id].kind}, {'WAIT_STOP'})
        self.assertEqual({incidents[first.id].host_id, incidents[second.id].host_id}, {'host-lost'})
        self.assertEqual(self.engine.get_task(first.id).retry_count, 0)
        healthy = self.engine.create_worker('healthy', host_id='host-healthy', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(healthy.shutdown(5)))
        healthy.start()
        unrelated = self.engine.submit(Task('healthy-work', abs, args=(-15,)))
        self.eventually(lambda: self.engine.get_task(unrelated.id).status == TaskStatus.SUCCESS)
        self.recovery.confirm_host_stopped('host-lost', session_ids=[lost_a.session_id],
            operator='host-supervisor', evidence='observed and joined only lost-a process')
        report = self.recovery.tick()
        self.assertIn(first.id, report.recovered_task_ids)
        self.assertNotIn(second.id, report.recovered_task_ids)
        self.eventually(lambda: self.engine.get_task(first.id).status == TaskStatus.SUCCESS)
        self.assertEqual((self.engine.get_task(second.id).status, self.engine.get_task(second.id).retry_count),
                         (TaskStatus.RUNNING, 0))
        self.assertNotEqual(self.registry.get_session(lost_b.session_id).state, 'STOPPED')
        self.assertEqual(self.registry.get_session(healthy.session_id).state, 'ACTIVE')
        self.assertTrue(healthy.shutdown(5))
        self.assertEqual((self.engine.get_task(first.id).attempt_count, self.engine.get_task(first.id).retry_count),
                         (2, 1))

    def test_old_host_snapshot_cannot_fence_a_new_worker_incarnation(self):
        old, first = self.claim('same-worker', 'host-a', 'first')
        self.now += 11
        self.recovery.tick()
        self.recovery.confirm_host_stopped('host-a', session_ids=[old.session_id],
            operator='supervisor', evidence='old process was joined before starting replacement')
        self.assertIn(first.id, self.recovery.tick().recovered_task_ids)
        # Reusing a worker identity starts a new session on the same host.
        replacement = self.registry.register('same-worker', host_id='host-a', heartbeat_timeout=30)
        claimed = self.engine._store.claim_next(replacement.session_id, 'replacement-owner')
        self.assertEqual(claimed.id, first.id)
        self.assertNotEqual(replacement.session_id, old.session_id)
        # A delayed host-failure acknowledgement refers only to its old snapshot.
        self.recovery.confirm_host_stopped('host-a', session_ids=[old.session_id],
            operator='supervisor', evidence='duplicate delayed acknowledgement for old incarnation')
        self.recovery.tick()
        self.assertEqual(self.registry.get_session(replacement.session_id).state, 'ACTIVE')
        self.assertEqual(self.engine.get_task(first.id).status, TaskStatus.RUNNING)
        self.assertEqual(self.engine.get_task(first.id).retry_count, 1)
        claimed.result = 7
        claimed.status = TaskStatus.SUCCESS
        self.engine._store.finish(claimed, 'replacement-owner')
        first.result = 999
        first.status = TaskStatus.SUCCESS
        with self.assertRaises(StaleExecutionError):
            self.engine._store.finish(first, 'first-owner')
        self.assertEqual(self.engine.get_task(first.id).result, 7)
        self.assertEqual(self.registry.get('same-worker').session_id, replacement.session_id)

    def test_host_confirmation_rejects_mixed_host_snapshot_atomically(self):
        host_a, first = self.claim('a', 'host-a', 'first')
        host_b, second = self.claim('b', 'host-b', 'second')
        self.now += 11
        self.recovery.tick()
        before = {record.session_id: record.state for record in self.registry.list_sessions()}
        with self.assertRaises(PersistenceError):
            self.recovery.confirm_host_stopped('host-a', session_ids=[host_a.session_id, host_b.session_id],
                operator='supervisor', evidence='invalid snapshot spans two fault domains')
        self.assertEqual({record.session_id: record.state for record in self.registry.list_sessions()}, before)
        for task in (first, second):
            saved = self.engine.get_task(task.id)
            self.assertEqual((saved.status, saved.retry_count), (TaskStatus.RUNNING, 0))

    def test_verifier_declining_stop_never_replays_a_fenced_live_attempt(self):
        session, old = self.claim('slow-live', 'host-a', 'live')
        observations = []

        def not_stopped(record):
            observations.append(record.session_id)
            return None  # Supervisor cannot establish that external calls ended.

        recovery = self.engine.create_recovery_system(interval=.01, stop_verifier=not_stopped)
        self.now += 11
        for _ in range(3):
            self.assertNotIn(old.id, recovery.tick().recovered_task_ids)
        self.assertIn(session.session_id, observations)
        self.assertEqual(self.registry.get_session(session.session_id).state, 'FENCED')
        saved = self.engine.get_task(old.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.RUNNING, 1, 0))
        old.result = 999
        old.status = TaskStatus.SUCCESS
        with self.assertRaises(StaleExecutionError):
            self.engine._store.finish(old, 'live-owner')
        incident = next(i for i in recovery.list_incidents() if i.task_id == old.id)
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='succeed', operator='operator',
                                  reason='result guessed before process stop', result=999)
        self.assertEqual(self.engine.get_task(old.id).status, TaskStatus.RUNNING)
        self.assertEqual(self.engine.get_task(old.id).retry_count, 0)

    def test_invalid_or_failing_stop_verifiers_keep_attempts_quarantined(self):
        def verifier_error(record):
            raise RuntimeError('supervisor connection unavailable')

        for label, verifier in (('exception', verifier_error), ('false', lambda record: False),
                                ('empty', lambda record: '')):
            with self.subTest(verifier=label):
                session, old = self.claim(f'worker-{label}', 'host-a', f'task-{label}')
                self.now += 11
                recovery = self.engine.create_recovery_system(interval=.01, stop_verifier=verifier)
                report = recovery.tick()
                self.assertTrue(report.errors)
                self.assertNotIn(old.id, report.recovered_task_ids)
                self.assertEqual(self.registry.get_session(session.session_id).state, 'FENCED')
                saved = self.engine.get_task(old.id)
                self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.RUNNING, 1, 0))
                incident = next(i for i in recovery.list_incidents() if i.task_id == old.id)
                self.assertEqual((incident.kind, incident.state), ('WAIT_STOP', 'OPEN'))

    def test_heartbeat_return_clears_suspicion_hold_and_later_loss_reopens_same_incident(self):
        for draining in (False, True):
            with self.subTest(draining=draining):
                session, old = self.claim(f'disconnected-{draining}', 'host-a', f'intermittent-{draining}')
                recovery = self.engine.create_recovery_system(interval=.01, fence_after=5)
                if draining:
                    self.registry.request_stop(session.worker_id, session.session_id)
                self.now += 11
                recovery.tick()
                self.assertEqual(self.registry.get_session(session.session_id).state, 'SUSPECT')
                first = next(i for i in recovery.list_incidents() if i.task_id == old.id)
                self.registry.heartbeat(session.worker_id, session.session_id)
                recovery.tick()
                self.assertEqual(self.registry.get_session(session.session_id).state,
                                 'STOPPING' if draining else 'ACTIVE')
                self.assertFalse(any(i.task_id == old.id for i in recovery.list_incidents()))
                closed = next(i for i in recovery.list_incidents(open_only=False) if i.id == first.id)
                self.assertEqual((closed.state, closed.resolution), ('RESOLVED', 'heartbeat resumed'))
                self.now += 11
                recovery.tick()
                reopened = next(i for i in recovery.list_incidents() if i.task_id == old.id)
                self.assertEqual((reopened.id, reopened.state, reopened.attempt_count), (first.id, 'OPEN', 1))
                self.assertEqual(self.registry.get_session(session.session_id).state, 'SUSPECT')
                self.now += 5
                recovery.tick()
                self.assertEqual(self.registry.get_session(session.session_id).state, 'FENCED')
                self.assertEqual((self.engine.get_task(old.id).attempt_count, self.engine.get_task(old.id).retry_count),
                                 (1, 0))

    def test_missing_pending_handler_is_isolated_and_redeployment_releases_its_hold(self):
        original = TaskEngine(database_path=self.path, task_registry={'missing': abs, 'healthy': round},
                              retry_policy=RetryPolicy(1))
        missing = original.submit(Task('missing', abs, id='missing-pending', args=(-11,), priority=100,
                                       idempotent=True))
        available = original.submit(Task('available', round, id='available', args=(2.4,)))
        partial = TaskEngine(database_path=self.path, task_registry={'healthy': round})
        recovery = partial.create_recovery_system(interval=.01)
        recovery.tick()
        incident = next(i for i in recovery.list_incidents() if i.task_id == missing.id)
        self.assertEqual((incident.kind, incident.state, incident.attempt_count), ('RECOVERY_ERROR', 'OPEN', 0))
        worker = partial.create_worker('partial-deployment', host_id='host-healthy', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        self.eventually(lambda: partial.get_task(available.id).status == TaskStatus.SUCCESS)
        self.assertIsNone(worker.last_error)
        self.assertTrue(worker.shutdown(5))
        self.assertEqual(original.get_task(missing.id).attempt_count, 0)
        restored = TaskEngine(database_path=self.path, task_registry={'missing': abs, 'healthy': round})
        restored_recovery = restored.create_recovery_system()
        restored_recovery.tick()
        self.assertFalse(any(i.task_id == missing.id for i in restored_recovery.list_incidents()))
        restored.start()
        self.assertEqual((restored.get_task(missing.id).status, restored.get_task(missing.id).attempt_count,
                          restored.get_task(missing.id).result), (TaskStatus.SUCCESS, 1, 11))

    def test_missing_running_handler_does_not_rollback_another_stopped_attempt_recovery(self):
        original = TaskEngine(database_path=self.path, task_registry={'missing': abs, 'healthy': round},
                              retry_policy=RetryPolicy(1))
        missing = original.submit(Task('missing', abs, id='missing-running', args=(-11,), priority=100,
                                       idempotent=True))
        available = original.submit(Task('available', round, id='available-running', args=(2.4,),
                                         idempotent=True))
        session = original.worker_registry.register('stopped-deployment', host_id='host-a')
        self.assertEqual(original._store.claim_next(session.session_id, 'missing-owner').id, missing.id)
        self.assertEqual(original._store.claim_next(session.session_id, 'healthy-owner').id, available.id)
        original.worker_registry.request_stop(session.worker_id, session.session_id)
        partial = TaskEngine(database_path=self.path, task_registry={'healthy': round})
        recovery = partial.create_recovery_system()
        recovery.confirm_session_stopped(session.session_id, operator='supervisor',
                                         evidence='runtime acknowledged both held invocations stopped')
        report = recovery.tick()
        self.assertIn(available.id, report.recovered_task_ids)
        self.assertNotIn(missing.id, report.recovered_task_ids)
        self.assertTrue(report.errors)
        saved_missing = original.get_task(missing.id)
        self.assertEqual((saved_missing.status, saved_missing.attempt_count, saved_missing.retry_count),
                         (TaskStatus.RUNNING, 1, 0))
        self.assertEqual(partial.get_task(available.id).retry_count, 1)
        incident = next(i for i in recovery.list_incidents() if i.task_id == missing.id)
        self.assertEqual(incident.kind, 'RECOVERY_ERROR')
        restored = TaskEngine(database_path=self.path, task_registry={'missing': abs, 'healthy': round})
        restored_recovery = restored.create_recovery_system()
        self.assertIn(missing.id, restored_recovery.tick().recovered_task_ids)
        restored.start()
        for task in (missing, available):
            saved = restored.get_task(task.id)
            self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.SUCCESS, 2, 1))
        self.assertFalse(restored_recovery.list_incidents())

    def test_success_during_suspicion_grace_closes_incident_and_unblocks_dag(self):
        downstream_calls = []

        def tail(value):
            downstream_calls.append(value)
            return value + 1

        engine = TaskEngine(database_path=self.path, task_registry={'root': abs, 'tail': tail},
                            retry_policy=RetryPolicy(1))
        engine._store.workers = self.registry
        flow = DAGWorkflow((DAGNode('root', 'root', args=(-9,), idempotent=True),
                            DAGNode('tail', 'tail', inputs={'value': OutputRef('root')})), id='grace-success')
        submitted = engine.submit_workflow(flow)
        session = self.registry.register('slow-root', host_id='host-a', heartbeat_timeout=10)
        claimed = engine._store.claim_next(session.session_id, 'slow-root-owner')
        self.assertEqual(claimed.id, submitted.nodes['root'].task_id)
        recovery = engine.create_recovery_system(interval=.01, fence_after=5)
        self.now += 11
        recovery.tick()
        self.assertEqual(self.registry.get_session(session.session_id).state, 'SUSPECT')
        self.assertEqual(engine.get_workflow(flow.id).status, WorkflowStatus.PAUSED)
        incident = next(i for i in recovery.list_incidents() if i.task_id == claimed.id)
        self.assertEqual(incident.kind, 'WAIT_STOP')
        # A missed heartbeat in the grace window must still accept a real result.
        claimed.result = abs(*claimed.args)
        claimed.status = TaskStatus.SUCCESS
        engine._store.finish(claimed, 'slow-root-owner')
        report = recovery.tick()
        self.assertNotIn(claimed.id, report.recovered_task_ids)
        self.assertFalse(recovery.list_incidents())
        self.assertEqual(engine.get_workflow(flow.id).status, WorkflowStatus.RUNNING)
        worker = engine.create_worker('healthy-tail', host_id='host-healthy', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        self.addCleanup(lambda: self.assertTrue(recovery.shutdown(5)))
        recovery.start()
        worker.start()
        self.eventually(lambda: engine.get_workflow(flow.id).status == WorkflowStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        self.assertTrue(recovery.shutdown(5))
        saved = engine.get_workflow(flow.id)
        self.assertEqual((saved.nodes['root'].attempt_count, saved.nodes['root'].retry_count), (1, 0))
        self.assertEqual((saved.nodes['tail'].attempt_count, saved.nodes['tail'].result), (1, 10))
        self.assertEqual(downstream_calls, [9])
        closed = next(i for i in recovery.list_incidents(open_only=False) if i.id == incident.id)
        self.assertEqual(closed.state, 'RESOLVED')

    def test_legacy_worker_recovery_releases_existing_coordinator_hold_before_consumption(self):
        session, old = self.claim('legacy-recovery', 'host-a', 'legacy-recovery-task')
        self.now += 11
        self.recovery.tick()
        incident = next(i for i in self.recovery.list_incidents() if i.task_id == old.id)
        self.assertEqual((incident.kind, incident.state), ('WAIT_STOP', 'OPEN'))
        self.registry.confirm_stopped(session.worker_id, session.session_id,
            evidence='legacy supervisor observed and joined every invocation from old session')
        recovered = self.engine.recover_worker(session.session_id)
        self.assertEqual([task.id for task in recovered], [old.id])
        # The legacy API must remove the coordinator's task hold atomically.
        # Consumption works before any new coordinator tick can repair it.
        self.assertFalse(self.recovery.list_incidents())
        worker = self.engine.create_worker('legacy-replacement', host_id='host-healthy', poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        self.eventually(lambda: self.engine.get_task(old.id).status == TaskStatus.SUCCESS)
        self.assertTrue(worker.shutdown(5))
        saved = self.engine.get_task(old.id)
        self.assertEqual((saved.attempt_count, saved.retry_count, saved.result), (2, 1, 7))
        self.assertEqual(self.engine.recover_worker(session.session_id), [])
        self.assertEqual(self.recovery.tick().recovered_task_ids, ())
        self.assertFalse(self.recovery.list_incidents())
        self.assertEqual(sum(e.event_type.value == 'INTERRUPTED' for e in saved.events), 1)


if __name__ == '__main__':
    unittest.main()
