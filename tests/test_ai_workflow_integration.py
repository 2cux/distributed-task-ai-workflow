"""Actual HTTP -> model -> JSON -> tool -> model DAG execution and persistence."""
from contextlib import closing
import json
import sqlite3
import unittest

from workflow_engine import (DAGWorkflow, NodeStatus, RetryPolicy, TaskEngine,
                             WorkflowOrchestrator, WorkflowStatus)
from ai_workflow_test_support import MixedWorkflowFixture, MODEL_SECRET, HTTP_SECRET


class MixedWorkflowTests(MixedWorkflowFixture):
    def run_workflow(self, workflow=None):
        self.engine = TaskEngine(database_path=self.database, task_registry=self.registry,
                                 max_workers=2, retry_policy=RetryPolicy(2, initial_delay=.01))
        self.orchestrator = WorkflowOrchestrator(self.engine)
        workflow = workflow or self.workflow()
        self.orchestrator.submit(workflow)
        return self.orchestrator.run(workflow.id, timeout=10)

    def test_complete_chain_binds_data_accounts_usage_and_reopens_without_replay(self):
        workflow = DAGWorkflow.from_dict(self.workflow().to_dict())
        run = self.run_workflow(workflow)
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual([c['path'] for c in self.calls],
                         ['/context', '/v1/chat/completions', '/actions', '/v1/chat/completions'])
        self.assertEqual(self.calls[1]['body']['messages'][1]['content'], 'Explain item-7')
        self.assertEqual(self.calls[3]['body']['messages'][0]['content'], 'Committed item-7')
        self.assertEqual(self.effects, [{'id': 'item-7'}])
        self.assertEqual((run.llm_usage['call_count'], run.llm_usage['total_tokens']), (2, 16))
        self.assertEqual(run.nodes['check'].result['text'], 'Action completed for item-7')
        self.assertEqual([c['authorization'] for c in self.calls],
                         ['Bearer ' + HTTP_SECRET, 'Bearer ' + MODEL_SECRET,
                          'Bearer ' + HTTP_SECRET, 'Bearer ' + MODEL_SECRET])
        # Reopen using fresh handler instances, as a restarted process would.
        from workflow_engine import create_workflow_registry
        registry = create_workflow_registry(self.llm_config, http_config=self.http_config)
        restarted = WorkflowOrchestrator(TaskEngine(database_path=self.database, task_registry=registry))
        self.assertEqual(restarted.submit(workflow), run)
        self.assertEqual(restarted.run(workflow.id, timeout=1), run)
        self.assertEqual(len(self.calls), 4)
        with closing(sqlite3.connect(self.database)) as db:
            dump = '\n'.join(db.iterdump())
        self.assertNotIn(MODEL_SECRET, dump)
        self.assertNotIn(HTTP_SECRET, dump)

    def test_invalid_model_json_retains_answer_and_skips_effects(self):
        self.model_replies.append((200, 'private-invalid-answer', 'stop'))
        run = self.run_workflow()
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual(run.nodes['plan'].status, NodeStatus.SUCCESS)
        self.assertEqual(run.nodes['plan'].result['text'], 'private-invalid-answer')
        self.assertEqual((run.nodes['parse'].attempt_count, run.nodes['parse'].retry_count), (1, 0))
        for name in ('execute', 'summarize', 'check'):
            self.assertEqual(run.nodes[name].status, NodeStatus.SKIPPED)
        self.assertNotIn('private-invalid-answer', run.nodes['parse'].error)
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertEqual(self.effects, [])

    def test_unknown_tool_and_disallowed_origin_cannot_produce_http_effects(self):
        for plan in ({'name': 'unregistered', 'arguments': {}},
                     {'name': 'http.request.v1', 'arguments': {'url': 'http://disallowed.example/actions', 'method': 'POST'}}):
            with self.subTest(plan=plan):
                self.model_replies.append((200, json.dumps(plan), 'stop'))
                run = self.run_workflow()
                self.assertEqual(run.nodes['execute'].status, NodeStatus.FAILED)
                self.assertEqual(run.nodes['execute'].attempt_count, 1)
                self.assertEqual(run.nodes['execute'].retry_count, 0)
                self.assertEqual(run.nodes['summarize'].status, NodeStatus.SKIPPED)
        self.assertEqual(self.effects, [])
        self.assertEqual([c['path'] for c in self.calls].count('/actions'), 0)

    def test_read_retry_does_not_replay_later_model_or_write_nodes(self):
        self.read_statuses.append(503)
        run = self.run_workflow()
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual((run.nodes['fetch'].attempt_count, run.nodes['fetch'].retry_count), (2, 1))
        self.assertEqual(run.llm_usage['call_count'], 2)
        self.assertEqual(len(self.effects), 1)
        self.assertEqual(len(self.calls), 5)

    def test_model_rate_limit_retries_before_tool_dispatch(self):
        self.model_replies.append((429, '', 'stop'))
        run = self.run_workflow(self.workflow(max_calls=3))
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual(run.nodes['plan'].retry_count, 1)
        self.assertEqual(run.llm_usage['call_count'], 3)
        self.assertEqual(run.llm_usage['failed_calls'], 1)
        self.assertEqual(len(self.effects), 1)

    def test_uncertain_write_failure_does_not_repeat_effect_or_call_summary_model(self):
        self.action_status = 503
        run = self.run_workflow()
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual((run.nodes['execute'].attempt_count, run.nodes['execute'].retry_count), (1, 0))
        self.assertEqual(self.effects, [{'id': 'item-7'}])
        self.assertEqual(run.nodes['summarize'].status, NodeStatus.SKIPPED)
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertEqual(self.engine.get_task(run.nodes['execute'].task_id).error.failure_category.value, 'uncertain')

    def test_llm_budget_counts_generation_only_and_gates_second_model(self):
        run = self.run_workflow(self.workflow(max_calls=1))
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual(run.nodes['execute'].status, NodeStatus.SUCCESS)
        self.assertEqual(run.nodes['summarize'].attempt_count, 0)
        self.assertEqual(run.nodes['summarize'].status, NodeStatus.FAILED)
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertEqual(run.llm_usage['total_tokens'], 8)
        self.assertEqual(self.effects, [{'id': 'item-7'}])


if __name__ == '__main__':
    unittest.main()
