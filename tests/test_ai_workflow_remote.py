"""Independent management -> central DAG -> remote worker mixed workflow tests."""
import json
import unittest
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

from workflow_engine import (NodeStatus, RemoteWorker, RetryPolicy, TaskEngine,
                             WorkflowOrchestrator, WorkflowStatus)
from workflow_engine.deployment import contract_registry
from ai_workflow_test_support import MixedWorkflowFixture


class MixedRemoteTests(MixedWorkflowFixture):
    def setUp(self):
        super().setUp()
        self.engine = TaskEngine(database_path=self.database, task_registry=contract_registry(list(self.registry)),
                                 retry_policy=RetryPolicy(2, initial_delay=.01))
        self.orchestrator = WorkflowOrchestrator(self.engine)
        self.token = 'mixed-remote-token'
        self.control = self.engine.create_recovery_service(self.token).start()
        self.addCleanup(self.control.shutdown)
        self.worker = RemoteWorker(self.control.url, self.registry, self.token, 'mixed-worker',
                                   heartbeat_interval=.05, heartbeat_timeout=2, poll_interval=.01)
        self.worker.start()
        self.addCleanup(self.worker.shutdown, 5)

    def api_call(self, service, path, body=None):
        request = Request(service.url + path, data=None if body is None else json.dumps(body).encode(),
                          headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            response = build_opener(ProxyHandler({})).open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    def test_management_submits_full_chain_and_remote_worker_executes_all_handlers(self):
        service = self.engine.create_management_service(self.token, poll_interval=.01).start()
        self.addCleanup(service.shutdown, 5)
        workflow = self.workflow()
        code, body = self.api_call(service, '/api/v1/workflows', workflow.to_dict())
        self.assertEqual(code, 202, body)
        run = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        code, body = self.api_call(service, '/api/v1/workflows/' + workflow.id)
        self.assertEqual(code, 200)
        self.assertEqual(body['definition'], workflow.to_dict())
        self.assertEqual(body['llm_usage']['call_count'], 2)
        self.assertEqual(body['llm_usage']['total_tokens'], 16)
        self.assertEqual(self.effects, [{'id': 'item-7'}])
        self.assertEqual(len(self.calls), 4)
        self.assertIsNone(self.worker.last_error)

    def test_remote_json_contract_failure_stops_downstream_effects(self):
        self.model_replies.append((200, '{"name":"http.request.v1"}', 'stop'))
        workflow = self.workflow()
        self.orchestrator.submit(workflow)
        run = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(run.nodes['parse'].status, NodeStatus.FAILED)
        self.assertEqual((run.nodes['parse'].attempt_count, run.nodes['parse'].retry_count), (1, 0))
        self.assertEqual(run.nodes['execute'].status, NodeStatus.SKIPPED)
        self.assertEqual(self.effects, [])
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertIsNone(self.worker.last_error)

    def test_remote_budget_blocks_only_later_generation(self):
        workflow = self.workflow(max_calls=1)
        self.orchestrator.submit(workflow)
        run = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(run.nodes['execute'].status, NodeStatus.SUCCESS)
        self.assertEqual(run.nodes['summarize'].attempt_count, 0)
        self.assertEqual(run.nodes['summarize'].status, NodeStatus.FAILED)
        self.assertEqual(run.llm_usage['call_count'], 1)
        self.assertEqual(self.effects, [{'id': 'item-7'}])
        self.assertIsNone(self.worker.last_error)


if __name__ == '__main__':
    unittest.main()
