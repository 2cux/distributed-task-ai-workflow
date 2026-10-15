"""Socket-level provider, remote worker, management API and CLI budget checks."""
from contextlib import redirect_stdout
import io
import json
from unittest.mock import patch
import unittest
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from test_llm import HTTPFixture
from workflow_engine import (
    DAGNode, DAGWorkflow, LLMBudget, LLM_HANDLER, LLM_OUTPUT_HANDLER, OutputRef,
    RemoteWorker, TaskEngine, WorkflowOrchestrator,
)
from workflow_engine.llm_cli import main


class LLMBudgetIntegrationTests(HTTPFixture):
    def request(self, service, path, body=None):
        request = Request(service.url + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer integration-token', 'Content-Type': 'application/json'})
        opener = build_opener(ProxyHandler({}))
        try:
            response = opener.open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            return response.code, json.loads(response.read())

    def test_management_accepts_budget_and_reports_deduplicated_usage(self):
        engine = self.engine()
        service = engine.create_management_service('integration-token', poll_interval=.01).start()
        self.addCleanup(service.shutdown, 5)
        flow = DAGWorkflow((DAGNode('model', LLM_HANDLER, kwargs={'prompt': 'hi'}),
                            DAGNode('check', LLM_OUTPUT_HANDLER, inputs={'result': OutputRef('model')})),
                           llm_budget=LLMBudget(max_calls=1))
        code, submitted = self.request(service, '/api/v1/workflows', flow.to_dict())
        self.assertEqual(code, 202)
        self.assertEqual(submitted['llm_usage']['call_count'], 0)
        run = WorkflowOrchestrator(engine).run(flow.id, timeout=5)
        code, saved = self.request(service, '/api/v1/workflows/' + flow.id)
        self.assertEqual(code, 200)
        self.assertEqual(saved['llm_usage'], run.llm_usage)
        self.assertEqual(saved['llm_budget']['remaining_calls'], 0)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(saved['llm_usage']['total_tokens'], 18)
        code, schema = self.request(service, '/openapi.json')
        self.assertEqual(code, 200)
        self.assertIn('llm_budget', schema['components']['schemas']['WorkflowDefinition']['properties'])
        self.assertIn('llm_usage', schema['components']['schemas']['Workflow']['properties'])

    def test_management_rejects_bad_budget_atomically(self):
        service = self.engine().create_management_service('integration-token', poll_interval=.01).start()
        self.addCleanup(service.shutdown, 5)
        for budget in (None, [], {'max_calls': True}, {'max_total_tokens': -1},
                       {'max_calls': 1.5}, {'unsupported_limit': 2}):
            with self.subTest(budget=budget):
                flow = dict(id='bad', nodes=[dict(id='a', handler=LLM_HANDLER)], llm_budget=budget)
                self.assertEqual(self.request(service, '/api/v1/workflows', flow)[0], 400)
        self.assertEqual(self.request(service, '/api/v1/workflows')[1]['items'], [])
        self.assertEqual(self.requests, [])

    def test_two_remote_workers_share_atomic_call_budget_with_central_placeholders(self):
        central = TaskEngine(database_path=self.path, task_registry={
            LLM_HANDLER: lambda **kwargs: self.fail('central handler executed')}, max_workers=2)
        service = central.create_recovery_service('integration-token').start()
        self.addCleanup(service.shutdown)
        workers = [RemoteWorker(service.url, {LLM_HANDLER: self.handler}, 'integration-token', 'remote-' + str(i),
                                poll_interval=.01, heartbeat_interval=.05) for i in range(2)]
        try:
            for worker in workers:
                worker.start()
            flow = DAGWorkflow(tuple(DAGNode(str(i), LLM_HANDLER, kwargs={'prompt': 'hi'}) for i in range(6)),
                               llm_budget=LLMBudget(max_calls=2))
            central.submit_workflow(flow)
            run = WorkflowOrchestrator(central).run(flow.id, execute=False, timeout=8)
            self.assertEqual(len(self.requests), 2)
            self.assertEqual(run.llm_usage['call_count'], 2)
            self.assertEqual(run.llm_usage['total_tokens'], 36)
            self.assertEqual(sum(node.attempt_count == 0 for node in run.nodes.values()), 4)
            for worker in workers:
                self.assertIsNone(worker.last_error)
        finally:
            for worker in workers:
                worker.shutdown(5)

    def test_remote_token_gate_stops_second_http_request_after_first_response(self):
        engine = self.engine(max_workers=2)
        service = engine.create_recovery_service('integration-token').start()
        self.addCleanup(service.shutdown)
        worker = RemoteWorker(service.url, self.registry, 'integration-token', 'remote-token',
                              poll_interval=.01, heartbeat_interval=.05)
        try:
            worker.start()
            flow = DAGWorkflow((DAGNode('a', LLM_HANDLER, kwargs={'prompt': 'hi'}),
                                DAGNode('b', LLM_HANDLER, kwargs={'prompt': 'hi'})),
                               llm_budget=LLMBudget(max_total_tokens=18))
            engine.submit_workflow(flow)
            run = WorkflowOrchestrator(engine).run(flow.id, execute=False, timeout=5)
            self.assertEqual(len(self.requests), 1)
            self.assertEqual(run.llm_usage['total_tokens'], 18)
            self.assertEqual(run.llm_budget['reason'], 'max_total_tokens')
        finally:
            worker.shutdown(5)

    def test_cli_zero_budget_makes_no_http_request_and_prints_accounting(self):
        output = io.StringIO()
        with patch.dict('os.environ', {'LLM_API_KEY': self.config.api_key, 'LLM_MODEL': self.config.model,
                                      'LLM_BASE_URL': self.base_url}), redirect_stdout(output):
            code = main(['--database', str(self.path), '--prompt', 'hi', '--max-calls', '0'])
        value = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(value['llm_usage']['call_count'], 0)
        self.assertEqual(value['llm_budget']['reason'], 'max_calls')
        self.assertEqual(self.requests, [])


if __name__ == '__main__':
    unittest.main()
