"""Independent budget enforcement tests, including competing database consumers."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
from threading import Barrier
import unittest

from workflow_engine import (
    DAGNode, DAGWorkflow, IdempotencyConflict, LLMBudget, LLM_HANDLER, LLM_OUTPUT_HANDLER,
    LLMTransientError, OutputRef, RetryPolicy, TaskEngine, WorkflowOrchestrator, validate_llm_output,
)


class LLMBudgetTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.llm-budget-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'budget.db'
        self.calls = []
        self.usage = dict(prompt_tokens=7, completion_tokens=3, total_tokens=10)
        self.registry = {LLM_HANDLER: self.generate, LLM_OUTPUT_HANDLER: validate_llm_output,
                         'business': lambda: 'executed'}

    def generate(self):
        self.calls.append(1)
        return dict(text='42', finish_reason='stop', usage=self.usage)

    def engine(self, **options):
        return TaskEngine(database_path=self.path, task_registry=self.registry, **options)

    def run_flow(self, budget, nodes=None, **options):
        engine = self.engine(**options)
        flow = DAGWorkflow(tuple(nodes or [DAGNode('a', LLM_HANDLER), DAGNode('b', LLM_HANDLER)]),
                           llm_budget=budget)
        engine.submit_workflow(flow)
        return engine, WorkflowOrchestrator(engine).run(flow.id, timeout=5)

    def test_budget_validation_roundtrip_and_legacy_definition_compatibility(self):
        for value in (-1, True, 1.5, '2', float('inf')):
            for name in ('max_calls', 'max_total_tokens'):
                with self.subTest(value=value, name=name), self.assertRaises(ValueError):
                    LLMBudget(**{name: value})
        flow = DAGWorkflow((DAGNode('a', LLM_HANDLER),), llm_budget=LLMBudget(2, 10))
        self.assertEqual(DAGWorkflow.from_dict(flow.to_dict()), flow)
        old = dict(id='old', nodes=[dict(id='a', handler=LLM_HANDLER)])
        self.assertNotIn('llm_budget', DAGWorkflow.from_dict(old).to_dict())
        for value in (None, [], {'max_calls': True}, {'unknown': 10}):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                DAGWorkflow.from_dict(dict(old, llm_budget=value))

    def test_zero_budget_rejects_before_execution_without_counting_failure_as_call(self):
        for budget in (LLMBudget(max_calls=0), LLMBudget(max_total_tokens=0)):
            with self.subTest(budget=budget):
                engine, run = self.run_flow(budget)
                self.assertEqual(run.status, 'FAILED')
                self.assertEqual(run.llm_usage['call_count'], 0)
                self.assertEqual(run.llm_usage['failed_calls'], 0)
                self.assertEqual(run.llm_usage['total_tokens'], 0)
                self.assertTrue(run.llm_budget['blocked'])
                for node in run.nodes.values():
                    self.assertEqual(node.attempt_count, 0)
                    events = engine.get_events(node.task_id)
                    self.assertEqual(sum(e.event_type == 'BUDGET_EXCEEDED' for e in events), 1)
        self.assertEqual(self.calls, [])

    def test_call_budget_includes_retry_and_remains_enforced_after_restart(self):
        def unavailable():
            self.calls.append(1)
            raise LLMTransientError('service unavailable')
        self.registry[LLM_HANDLER] = unavailable
        engine, run = self.run_flow(LLMBudget(max_calls=1), [DAGNode('a', LLM_HANDLER)],
                                    retry_policy=RetryPolicy(3))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(run.nodes['a'].attempt_count, 1)
        self.assertEqual(run.llm_usage['failed_calls'], 1)
        self.assertEqual(run.llm_budget['remaining_calls'], 0)
        self.assertIn('LLMBudgetExceeded', run.nodes['a'].error)
        reopened = self.engine(retry_policy=RetryPolicy(3))
        self.assertEqual(reopened.advance_workflow(run.id).llm_usage, run.llm_usage)
        self.assertEqual(len(self.calls), 1)

    def test_token_threshold_blocks_next_generation_but_allows_validation(self):
        engine, run = self.run_flow(LLMBudget(max_total_tokens=10), [
            DAGNode('a', LLM_HANDLER),
            DAGNode('check', LLM_OUTPUT_HANDLER, inputs={'result': OutputRef('a')}),
            DAGNode('b', LLM_HANDLER, depends_on=('check',)),
            DAGNode('child', 'business', depends_on=('b',))])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(run.llm_usage['total_tokens'], 10)
        self.assertEqual(run.nodes['check'].status, 'SUCCESS')
        self.assertEqual(run.nodes['b'].status, 'FAILED')
        self.assertEqual(run.nodes['child'].status, 'SKIPPED')
        self.assertEqual(run.llm_budget['reason'], 'max_total_tokens')
        self.assertEqual(run.llm_budget['remaining_total_tokens'], 0)

    def test_single_response_can_cross_token_threshold_and_later_calls_stop(self):
        _, run = self.run_flow(LLMBudget(max_total_tokens=5), max_workers=3)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(run.llm_usage['total_tokens'], 10)
        self.assertEqual(run.llm_budget['remaining_total_tokens'], 0)

    def test_missing_total_usage_blocks_token_budget_but_not_call_only_budget(self):
        self.usage = dict(prompt_tokens=7, completion_tokens=3, total_tokens=None)
        _, blocked = self.run_flow(LLMBudget(max_total_tokens=100))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(blocked.llm_budget['reason'], 'unknown_token_usage')
        self.assertIsNone(blocked.llm_usage['total_tokens'])
        self.assertIsNone(blocked.llm_budget['remaining_total_tokens'])
        _, allowed = self.run_flow(LLMBudget(max_calls=2))
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(allowed.status, 'SUCCESS')

    def test_competing_stores_cannot_claim_more_than_shared_call_limit(self):
        engine = self.engine()
        flow = DAGWorkflow(tuple(DAGNode(str(i), LLM_HANDLER) for i in range(8)),
                           llm_budget=LLMBudget(max_calls=2))
        submitted = engine.submit_workflow(flow)
        stores = [self.engine()._store for _ in flow.nodes]
        barrier = Barrier(len(stores))
        def compete(pair):
            index, store = pair
            task = store.get(submitted.nodes[str(index)].task_id)
            barrier.wait(5)
            return store.claim(task, 'owner-' + str(index))
        with ThreadPoolExecutor(len(stores)) as pool:
            claimed = list(pool.map(compete, enumerate(stores)))
        self.assertEqual(sum(task is not None for task in claimed), 2)
        usage = engine.get_workflow(flow.id).llm_usage
        self.assertEqual((usage['call_count'], usage['running_calls']), (2, 2))
        self.assertEqual(self.calls, [])

    def test_inflight_token_budget_defers_siblings_without_starving_other_workflows(self):
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('a', LLM_HANDLER, priority=10),
                            DAGNode('b', LLM_HANDLER, priority=9)), llm_budget=LLMBudget(max_total_tokens=100))
        engine.submit_workflow(flow)
        other = DAGWorkflow((DAGNode('business', 'business'),))
        engine.submit_workflow(other)
        session = engine.worker_registry.register('consumer')
        first = engine._store.claim_next(session.session_id, 'first')
        self.assertEqual(first.name, 'a')
        second = engine._store.claim_next(session.session_id, 'second')
        self.assertEqual(second.name, 'business')
        live = engine.get_workflow(flow.id)
        self.assertTrue(live.llm_budget['waiting_for_usage'])
        self.assertEqual(live.nodes['b'].attempt_count, 0)
        self.assertEqual(live.nodes['b'].status, 'READY')
        self.assertIsNone(engine._store.claim_next(session.session_id, 'third'))

    def test_parallel_branches_continue_after_usage_releases_token_gate(self):
        _, run = self.run_flow(LLMBudget(max_total_tokens=30), max_workers=4)
        self.assertEqual(run.status, 'SUCCESS')
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(run.llm_usage['total_tokens'], 20)
        self.assertEqual(run.llm_budget['remaining_total_tokens'], 10)

    def test_budget_is_immutable_under_idempotent_workflow_id(self):
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('a', LLM_HANDLER),), llm_budget=LLMBudget(max_calls=1))
        engine.submit_workflow(flow)
        changed = DAGWorkflow(flow.nodes, id=flow.id, llm_budget=LLMBudget(max_calls=2))
        with self.assertRaises(IdempotencyConflict):
            engine.submit_workflow(changed)


if __name__ == '__main__':
    unittest.main()
