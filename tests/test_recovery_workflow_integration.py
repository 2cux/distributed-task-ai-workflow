"""Independent DAG/operator recovery regressions against a fresh durable journal."""

from pathlib import Path
import tempfile
import time
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, NodeStatus, OutputRef, PersistenceError,
                             RetryPolicy, Task, TaskEngine, TaskStatus, WorkflowStatus)


class RecoveryWorkflowIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix='.recovery-workflow-tests-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'tasks.db'

    def eventually(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail('workflow recovery condition did not become true before deadline')

    def worker(self, engine, identity):
        worker = engine.create_worker(identity, poll_interval=.01)
        self.addCleanup(lambda: self.assertTrue(worker.shutdown(5)))
        worker.start()
        return worker

    def task_incident(self, recovery, task_id):
        matches = [incident for incident in recovery.list_incidents() if incident.task_id == task_id
                   and incident.kind == 'TASK_FAILURE']
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_input_binding_failure_rejects_deadlocked_retry_and_accepts_verified_result(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs}, retry_policy=RetryPolicy(2))
        flow = DAGWorkflow((DAGNode('root', 'abs', args=(-4,)),
            DAGNode('binding', 'abs', inputs={'x': OutputRef('root', ('missing',))}, idempotent=True),
            DAGNode('tail', 'abs', args=(-9,), depends_on=('binding',))), id='binding-failure')
        submitted = engine.submit_workflow(flow)
        engine.start()
        engine.advance_workflow(flow.id)
        recovery = engine.create_recovery_system()
        recovery.tick()
        binding_id = submitted.nodes['binding'].task_id
        incident = self.task_incident(recovery, binding_id)
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='retry', extra_retries=1,
                                  operator='operator', reason='cannot repair immutable invalid input path')
        self.assertEqual(engine.get_task(binding_id).retry_count, 0)
        recovery.resolve_task(incident.id, action='succeed', result=4,
                              operator='operator', reason='verified result through external reconciliation')
        reopened = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        reopened.start()
        final = reopened.advance_workflow(flow.id)
        self.assertEqual(final.status, WorkflowStatus.SUCCESS)
        self.assertEqual(final.nodes['binding'].attempt_count, 0)
        self.assertEqual(final.nodes['root'].attempt_count, 1)
        self.assertEqual(final.nodes['tail'].result, 9)

    def test_missing_stopped_handler_can_be_reconciled_without_restoring_business_code(self):
        original = TaskEngine(database_path=self.path, task_registry={'abs': abs}, retry_policy=RetryPolicy(1))
        submitted = original.submit(Task('retired-handler', abs, args=(-6,), idempotent=False))
        session = original.worker_registry.register('retired-worker')
        original._store.claim_next(session.session_id, 'old-owner')
        original.worker_registry.request_stop('retired-worker', session.session_id)
        original.worker_registry.confirm_stopped('retired-worker', session.session_id, evidence='supervisor joined retired process')
        retired = TaskEngine(database_path=self.path, task_registry={})
        recovery = retired.create_recovery_system()
        report = recovery.tick()
        self.assertTrue(report.errors)
        incident = recovery.list_incidents()[0]
        self.assertEqual(incident.kind, 'RECOVERY_ERROR')
        recovery.resolve_task(incident.id, action='succeed', result=6, operator='operator',
                              reason='external ledger confirmed completed result for removed handler')
        saved = original.get_task(submitted.id)
        self.assertEqual((saved.status, saved.result, saved.attempt_count, saved.retry_count),
                         (TaskStatus.SUCCESS, 6, 1, 0))
        self.assertFalse(recovery.list_incidents())

    def test_legacy_stop_evidence_survives_handler_recovery_errors_and_another_scan(self):
        original = TaskEngine(database_path=self.path, task_registry={'abs': abs}, retry_policy=RetryPolicy(1))
        task = original.submit(Task('legacy-retired', abs, args=(-8,), idempotent=True))
        original._store.claim(task, 'legacy-owner')
        retired = TaskEngine(database_path=self.path, task_registry={})
        recovery = retired.create_recovery_system()
        recovery.tick()
        recovery.confirm_task_stopped(task.id, attempt_count=1, operator='supervisor',
                                      evidence='joined exact legacy invocation; business calls stopped')
        recovery.tick()  # Preserve the durable proof even when decoding still fails.
        self.assertEqual(recovery.list_incidents()[0].kind, 'RECOVERY_ERROR')
        self.assertEqual(original.get_task(task.id).retry_count, 0)
        restored = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        self.assertIn(task.id, restored.create_recovery_system().tick().recovered_task_ids)
        restored.start()
        saved = restored.get_task(task.id)
        self.assertEqual((saved.status, saved.result, saved.attempt_count, saved.retry_count),
                         (TaskStatus.SUCCESS, 8, 2, 1))

    def test_manual_verified_result_repairs_failed_dag_without_replaying_completed_branches(self):
        calls = []

        def uncertain_effect():
            calls.append('effect')
            raise RuntimeError('external write completed but response could not be verified')

        def independent():
            calls.append('independent')
            return 5

        def tail(value):
            calls.append('tail')
            return value * 2

        registry = {'effect': uncertain_effect, 'independent': independent, 'tail': tail}
        engine = TaskEngine(database_path=self.path, task_registry=registry)
        workflow = DAGWorkflow((DAGNode('effect', 'effect'), DAGNode('independent', 'independent'),
            DAGNode('tail', 'tail', inputs={'value': OutputRef('effect', ('verified',))})), id='operator-dag')
        submitted = engine.submit_workflow(workflow)
        effect_id = submitted.nodes['effect'].task_id
        independent_id = submitted.nodes['independent'].task_id
        original = self.worker(engine, 'initial')
        self.eventually(lambda: engine.get_task(effect_id).status == TaskStatus.FAILED
                        and engine.get_task(independent_id).status == TaskStatus.SUCCESS)
        self.assertTrue(original.shutdown(5))
        # Existing orchestrators may already have materialized FAILED/SKIPPED.
        failed = engine.advance_workflow(workflow.id)
        self.assertEqual(failed.status, WorkflowStatus.FAILED)
        self.assertEqual(failed.nodes['tail'].status, NodeStatus.SKIPPED)
        recovery = engine.create_recovery_system(interval=.01)
        recovery.tick()
        self.assertEqual(engine.get_workflow(workflow.id).status, WorkflowStatus.PAUSED)
        incident = self.task_incident(recovery, effect_id)
        self.assertEqual(incident.workflow_id, workflow.id)
        # Unsafe side effects require external verification; no retry is permitted.
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='retry', operator='auditor', reason='guess replay')
        self.assertEqual(engine.get_task(effect_id).attempt_count, 1)
        recovery.resolve_task(incident.id, action='succeed', operator='auditor',
            reason='queried external ledger and verified completed write', result={'verified': 13})
        recovery.resume_workflow(workflow.id, operator='auditor', reason='verified result unblocks dependencies')
        recovery.start()
        self.addCleanup(lambda: self.assertTrue(recovery.shutdown(5)))
        replacement = self.worker(engine, 'replacement')
        self.eventually(lambda: engine.get_workflow(workflow.id).status == WorkflowStatus.SUCCESS)
        self.assertTrue(replacement.shutdown(5))
        self.assertTrue(recovery.shutdown(5))
        saved = engine.get_workflow(workflow.id)
        self.assertEqual(saved.nodes['tail'].result, 26)
        self.assertEqual(saved.nodes['effect'].attempt_count, 1)
        self.assertEqual(saved.nodes['independent'].attempt_count, 1)
        self.assertEqual(calls.count('effect'), 1)
        self.assertEqual(calls.count('independent'), 1)
        self.assertEqual(calls.count('tail'), 1)
        self.assertFalse(recovery.list_incidents())
        resolved = next(i for i in recovery.list_incidents(open_only=False) if i.id == incident.id)
        self.assertEqual(resolved.state, 'RESOLVED')

    def test_persistent_pause_blocks_claims_and_advance_after_engine_restart(self):
        registry = {'abs': abs}
        engine = TaskEngine(database_path=self.path, task_registry=registry)
        flow = DAGWorkflow((DAGNode('root', 'abs', args=(-3,)),
                            DAGNode('tail', 'abs', args=(-4,), depends_on=('root',))), id='paused')
        submitted = engine.submit_workflow(flow)
        recovery = engine.create_recovery_system()
        recovery.pause_workflow(flow.id, operator='operator', reason='maintenance before dispatch')
        reopened = TaskEngine(database_path=self.path, task_registry=registry)
        resumed_recovery = reopened.create_recovery_system(interval=.01)
        unrelated = reopened.submit(Task('unrelated', abs, args=(-20,)))
        healthy = self.worker(reopened, 'healthy')
        self.eventually(lambda: reopened.get_task(unrelated.id).status == TaskStatus.SUCCESS)
        for _ in range(3):
            self.assertEqual(reopened.advance_workflow(flow.id).status, WorkflowStatus.PAUSED)
            resumed_recovery.tick()
        self.assertEqual(reopened.get_task(submitted.nodes['root'].task_id).attempt_count, 0)
        self.assertEqual(reopened.get_task(submitted.nodes['tail'].task_id).attempt_count, 0)
        self.assertFalse(resumed_recovery.list_incidents())
        resumed_recovery.resume_workflow(flow.id, operator='operator', reason='maintenance complete')
        resumed_recovery.start()
        self.addCleanup(lambda: self.assertTrue(resumed_recovery.shutdown(5)))
        self.eventually(lambda: reopened.get_workflow(flow.id).status == WorkflowStatus.SUCCESS)
        self.assertTrue(healthy.shutdown(5))
        self.assertTrue(resumed_recovery.shutdown(5))
        saved = reopened.get_workflow(flow.id)
        self.assertEqual((saved.nodes['root'].attempt_count, saved.nodes['tail'].attempt_count), (1, 1))

    def test_manual_retry_requires_explicit_extra_budget_and_keeps_attempt_history(self):
        calls = []

        def fail_twice():
            calls.append('attempt')
            if len(calls) <= 2:
                raise RuntimeError('retry budget exhausted before operator repair')
            return 42

        registry = {'action': fail_twice}
        engine = TaskEngine(database_path=self.path, task_registry=registry, retry_policy=RetryPolicy(1))
        flow = DAGWorkflow((DAGNode('action', 'action', idempotent=True, max_retries=1),), id='budget')
        submitted = engine.submit_workflow(flow)
        task_id = submitted.nodes['action'].task_id
        original = self.worker(engine, 'original')
        self.eventually(lambda: engine.get_task(task_id).status == TaskStatus.FAILED)
        self.assertTrue(original.shutdown(5))
        recovery = engine.create_recovery_system(interval=.01)
        recovery.tick()
        incident = self.task_incident(recovery, task_id)
        self.assertEqual(incident.attempt_count, 2)
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='retry', operator='operator', reason='repair without extension')
        self.assertEqual((engine.get_task(task_id).attempt_count, engine.get_task(task_id).retry_count), (2, 1))
        self.assertEqual(self.task_incident(recovery, task_id).id, incident.id)
        recovery.resolve_task(incident.id, action='retry', operator='operator',
                              reason='dependency repaired; authorize one additional retry', extra_retries=1)
        queued = engine.get_task(task_id)
        self.assertEqual((queued.status, queued.attempt_count, queued.retry_count), (TaskStatus.RETRYING, 2, 2))
        recovery.resume_workflow(flow.id, operator='operator', reason='bounded repair authorized')
        recovery.start()
        self.addCleanup(lambda: self.assertTrue(recovery.shutdown(5)))
        replacement = self.worker(engine, 'replacement')
        self.eventually(lambda: engine.get_workflow(flow.id).status == WorkflowStatus.SUCCESS)
        self.assertTrue(replacement.shutdown(5))
        self.assertTrue(recovery.shutdown(5))
        final = engine.get_task(task_id)
        self.assertEqual((final.attempt_count, final.retry_count, final.result), (3, 2, 42))
        self.assertEqual(len(calls), 3)
        self.assertEqual(sum(e.event_type.value == 'STARTED' for e in final.events), 3)
        with self.assertRaises(PersistenceError):
            recovery.resolve_task(incident.id, action='retry', operator='operator', reason='duplicate stale decision')
        self.assertEqual(engine.get_task(task_id).attempt_count, 3)

    def test_operator_final_failure_releases_workflow_and_skips_dependents(self):
        def fail():
            raise RuntimeError('permanent business failure')

        engine = TaskEngine(database_path=self.path, task_registry={'fail': fail, 'abs': abs})
        flow = DAGWorkflow((DAGNode('root', 'fail'),
                            DAGNode('tail', 'abs', args=(-8,), depends_on=('root',))), id='reject')
        submitted = engine.submit_workflow(flow)
        task_id = submitted.nodes['root'].task_id
        worker = self.worker(engine, 'initial')
        self.eventually(lambda: engine.get_task(task_id).status == TaskStatus.FAILED)
        self.assertTrue(worker.shutdown(5))
        recovery = engine.create_recovery_system()
        recovery.tick()
        incident = self.task_incident(recovery, task_id)
        recovery.resolve_task(incident.id, action='fail', operator='operator', reason='confirmed permanent failure')
        recovery.resume_workflow(flow.id, operator='operator', reason='final outcome accepted')
        recovery.tick()
        final = engine.advance_workflow(flow.id)
        self.assertEqual(final.status, WorkflowStatus.FAILED)
        self.assertEqual(final.nodes['tail'].status, NodeStatus.SKIPPED)
        self.assertEqual(final.nodes['tail'].attempt_count, 0)
        # An accepted final failure must not reopen an identical incident every scan.
        self.assertFalse(recovery.list_incidents())
        recovery.tick()
        self.assertFalse(recovery.list_incidents())


if __name__ == '__main__':
    unittest.main()
