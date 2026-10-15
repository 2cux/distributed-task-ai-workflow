"""Independent accounting tests over real DAG execution and durable records."""
from pathlib import Path
import tempfile
import unittest

from workflow_engine import (
    DAGNode, DAGWorkflow, LLM_HANDLER, LLM_OUTPUT_HANDLER, LLMTransientError,
    OutputRef, RetryPolicy, TaskEngine, WorkflowOrchestrator, validate_llm_output,
)


def answer(usage=None, response_id='same-provider-id'):
    return dict(text='42', finish_reason='stop', model='test', response_id=response_id,
                usage=usage if usage is not None else dict(prompt_tokens=7, completion_tokens=3, total_tokens=10))


class LLMUsageTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.llm-usage-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'usage.db'
        self.calls = []

    def engine(self, generate=None, **options):
        def model():
            self.calls.append(1)
            return answer()
        return TaskEngine(database_path=self.path, task_registry={
            LLM_HANDLER: generate or model, LLM_OUTPUT_HANDLER: validate_llm_output,
            'copy': lambda result: result}, **options)

    def run_flow(self, engine, nodes):
        flow = DAGWorkflow(tuple(nodes))
        runner = WorkflowOrchestrator(engine)
        runner.submit(flow)
        return runner.run(flow.id, timeout=5)

    def test_validation_and_tool_copies_are_not_calls_even_without_response_id(self):
        def generate():
            self.calls.append(1)
            return answer(response_id=None)
        engine = self.engine(generate)
        run = self.run_flow(engine, [DAGNode('model', LLM_HANDLER),
            DAGNode('check1', LLM_OUTPUT_HANDLER, inputs={'result': OutputRef('model')}),
            DAGNode('check2', LLM_OUTPUT_HANDLER, inputs={'result': OutputRef('check1')}),
            DAGNode('tool', 'copy', inputs={'result': OutputRef('check2')})])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertEqual(run.llm_usage['total_tokens'], 10)
        self.assertEqual(run.llm_usage['failure_rate'], 0)
        self.assertEqual(run.nodes['tool'].result, run.nodes['model'].result)

    def test_provider_reused_response_ids_do_not_merge_distinct_calls(self):
        run = self.run_flow(self.engine(), [DAGNode('a', LLM_HANDLER), DAGNode('b', LLM_HANDLER)])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(run.llm_usage['call_count'], 2)
        self.assertEqual((run.llm_usage['prompt_tokens'], run.llm_usage['completion_tokens'],
                          run.llm_usage['total_tokens']), (14, 6, 20))

    def test_retries_count_failed_attempts_but_validation_failure_does_not(self):
        def generate():
            self.calls.append(1)
            if len(self.calls) == 1:
                raise LLMTransientError('try again')
            return answer()
        engine = self.engine(generate, retry_policy=RetryPolicy(2))
        run = self.run_flow(engine, [DAGNode('model', LLM_HANDLER),
            DAGNode('invalid', LLM_OUTPUT_HANDLER, kwargs={'expected_text': 'other'},
                    inputs={'result': OutputRef('model')})])
        self.assertEqual(run.status, 'FAILED')
        self.assertEqual(len(self.calls), 2)
        usage = run.llm_usage
        self.assertEqual((usage['call_count'], usage['successful_calls'], usage['failed_calls']), (2, 1, 1))
        self.assertEqual(usage['failure_rate'], .5)
        self.assertEqual(usage['total_tokens'], 10)
        self.assertEqual(usage['unknown_usage_calls'], 1)
        self.assertFalse(usage['usage_complete'])

    def test_partial_usage_is_known_subtotal_and_missing_is_never_fabricated(self):
        results = iter([answer(), answer({'prompt_tokens': 5, 'completion_tokens': None, 'total_tokens': None})])
        run = self.run_flow(self.engine(lambda: next(results)),
                            [DAGNode('a', LLM_HANDLER), DAGNode('b', LLM_HANDLER)])
        self.assertEqual((run.llm_usage['prompt_tokens'], run.llm_usage['completion_tokens'],
                          run.llm_usage['total_tokens']), (12, 3, 10))
        self.assertEqual(run.llm_usage['missing_usage_calls'],
                         dict(prompt_tokens=0, completion_tokens=1, total_tokens=1))

    def test_restart_repeated_reads_and_idempotent_submit_do_not_add_usage(self):
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('model', LLM_HANDLER),))
        engine.submit_workflow(flow)
        original = WorkflowOrchestrator(engine).run(flow.id, timeout=5)
        reopened = self.engine()
        for _ in range(3):
            self.assertEqual(reopened.submit_workflow(flow).llm_usage, original.llm_usage)
            self.assertEqual(reopened.advance_workflow(flow.id).llm_usage, original.llm_usage)
            self.assertEqual(reopened.get_workflow(flow.id).llm_usage, original.llm_usage)
        self.assertEqual(len(self.calls), 1)

    def test_running_and_interrupted_attempts_remain_unknown_usage(self):
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('model', LLM_HANDLER),))
        task_id = engine.submit_workflow(flow).nodes['model'].task_id
        engine._store.claim(engine.get_task(task_id), 'interrupted-owner')
        live = engine.get_workflow(flow.id).llm_usage
        self.assertEqual((live['call_count'], live['running_calls'], live['failed_calls']), (1, 1, 0))
        self.assertIsNone(live['total_tokens'])
        engine._store.recover_interrupted()
        restored = self.engine().get_workflow(flow.id).llm_usage
        self.assertEqual((restored['call_count'], restored['running_calls'], restored['failed_calls']), (1, 0, 1))
        self.assertEqual(restored['failure_rate'], 1)
        self.assertEqual(self.calls, [])

    def test_alias_identity_survives_restart_with_placeholder_registry(self):
        def generate():
            return answer()
        generate.failure_scope = 'llm'
        first = TaskEngine(database_path=self.path, task_registry={'provider.alias': generate})
        run = self.run_flow(first, [DAGNode('model', 'provider.alias')])
        reopened = TaskEngine(database_path=self.path, task_registry={'provider.alias': lambda: None})
        self.assertEqual(reopened.get_workflow(run.id).llm_usage['total_tokens'], 10)

    def test_existing_database_migrates_generation_identity_and_keeps_usage(self):
        engine = self.engine()
        run = self.run_flow(engine, [DAGNode('model', LLM_HANDLER),
            DAGNode('check', LLM_OUTPUT_HANDLER, inputs={'result': OutputRef('model')})])
        # Recreate the pre-feature journal shape; all token/event data already exists.
        with engine._store._transaction() as db:
            db.execute('ALTER TABLE workflow_nodes DROP COLUMN is_llm')
        reopened = self.engine()
        self.assertEqual(reopened.get_workflow(run.id).llm_usage, run.llm_usage)
        self.assertEqual(reopened.get_workflow(run.id).llm_usage['call_count'], 1)

    def test_manual_reconciliation_attaches_verified_usage_to_same_attempt(self):
        def interrupted():
            self.calls.append(1)
            raise RuntimeError('unknown outcome')
        engine = self.engine(interrupted)
        run = self.run_flow(engine, [DAGNode('model', LLM_HANDLER)])
        recovery = engine.create_recovery_system()
        recovery.tick()
        incident = next(item for item in recovery.list_incidents() if item.task_id == run.nodes['model'].task_id)
        recovery.resolve_task(incident.id, action='succeed', operator='test',
                              reason='provider response verified', result=answer())
        usage = engine.get_workflow(run.id).llm_usage
        self.assertEqual((usage['call_count'], usage['successful_calls'], usage['failed_calls']), (1, 1, 0))
        self.assertEqual(usage['total_tokens'], 10)
        self.assertTrue(usage['usage_complete'])
        self.assertEqual(len(self.calls), 1)


if __name__ == '__main__':
    unittest.main()
