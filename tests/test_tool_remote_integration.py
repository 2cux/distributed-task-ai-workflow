"""Independent central/remote-worker integration for external tool calls."""
from pathlib import Path
from copy import deepcopy
import json
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

from workflow_engine import (DAGNode, DAGWorkflow, HTTPConfig, HTTPRequestHandler, HTTP_HANDLER, InputBinding,
                             NodeStatus, OutputRef, RemoteWorker, RetryPolicy, TOOL_HANDLER,
                             TaskEngine, TaskEventType, ToolRegistry, WorkflowOrchestrator, WorkflowStatus)
from tool_test_support import local_service, WRITE_PARAMETERS_SCHEMA


class RemoteToolTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.tool-remote-')
        self.addCleanup(temporary.cleanup)
        self.service = local_service(self)
        registry = ToolRegistry()
        http = HTTPRequestHandler(HTTPConfig((self.service.origin,)))
        registry.register(HTTP_HANDLER, http)
        registry.register('typed.write', http, parameters_schema=WRITE_PARAMETERS_SCHEMA)
        self.handlers = registry.to_task_registry()
        def placeholder():
            def forbidden(*args, **kwargs):
                raise AssertionError('central process must not execute external tools')
            return forbidden
        self.engine = TaskEngine(database_path=Path(temporary.name) / 'central.db',
                                 task_registry={name: placeholder() for name in self.handlers},
                                 retry_policy=RetryPolicy(2, initial_delay=.01))
        self.orchestrator = WorkflowOrchestrator(self.engine)
        self.token = 'local-tool-test-token'
        self.control = self.engine.create_recovery_service(self.token).start()
        self.addCleanup(self.control.shutdown)
        self.worker = RemoteWorker(self.control.url, self.handlers, self.token, 'tool-worker',
                                   heartbeat_interval=.05, heartbeat_timeout=2, poll_interval=.01)
        self.worker.start()
        self.addCleanup(self.worker.shutdown, 5)

    def start_management(self):
        service = self.engine.create_management_service(self.token, poll_interval=.01).start()
        self.addCleanup(service.shutdown, 5)
        return service

    def management_call(self, service, path, body=None):
        request = Request(service.url + path, data=None if body is None else json.dumps(body).encode(),
                          headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            response = build_opener(ProxyHandler({})).open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    def test_management_api_accepts_and_describes_nested_bindings_then_remote_worker_executes(self):
        service = self.start_management()
        workflow = DAGWorkflow((
            DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}),
            DAGNode('action', HTTP_HANDLER, kwargs=dict(url=self.service.origin + '/actions', method='POST', json_body={}),
                    bindings=(InputBinding(('json_body', 'id'), OutputRef('fetch', ('json', 'items', 0, 'id'))),)),
        ))
        code, body = self.management_call(service, '/api/v1/workflows', workflow.to_dict())
        self.assertEqual(code, 202, body)
        result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(self.service.effects, [{'id': 'item-7'}])
        code, body = self.management_call(service, '/api/v1/workflows/' + workflow.id)
        self.assertEqual(code, 200)
        self.assertEqual(body['definition'], workflow.to_dict())
        code, document = self.management_call(service, '/openapi.json')
        self.assertEqual(code, 200)
        schemas = document['components']['schemas']
        self.assertEqual(schemas['Node']['properties']['bindings']['items']['$ref'], '#/components/schemas/InputBinding')
        self.assertEqual(schemas['InputBinding']['required'], ['target', 'source'])
        code, _ = self.management_call(service, '/api/v1/workflows', workflow.to_dict())
        self.assertEqual(code, 202)
        self.assertEqual(len(self.service.calls), 2)

    def test_management_api_rejects_malformed_bindings_without_partial_submission(self):
        service = self.start_management()
        valid = {'target': ['json_body', 'id'], 'source': {'node_id': 'node', 'path': []}}
        invalid = [None, {}, [None], [{}], [dict(valid, extra=True)],
                   [dict(valid, target='body')], [dict(valid, target=[])],
                   [dict(valid, target=['body', -1])], [dict(valid, target=[True])],
                   [dict(valid, source=[])], [dict(valid, source={})],
                   [dict(valid, source={'node_id': 'node', 'path': 'invalid'})],
                   [dict(valid, source={'node_id': 'node', 'extra': True})]]
        for index, bindings in enumerate(invalid):
            workflow = dict(id=f'invalid-{index}', nodes=[dict(id='node', handler=HTTP_HANDLER,
                            kwargs={'url': self.service.origin}, bindings=deepcopy(bindings))])
            with self.subTest(bindings=bindings):
                code, body = self.management_call(service, '/api/v1/workflows', workflow)
                self.assertEqual(code, 400, body)
        self.assertEqual(self.management_call(service, '/api/v1/workflows')[1]['items'], [])
        self.assertEqual(self.management_call(service, '/api/v1/tasks')[1]['items'], [])
        self.assertEqual(self.service.calls, [])

    def test_remote_worker_receives_materialized_nested_arguments_and_returns_real_response(self):
        workflow = DAGWorkflow((
            DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}),
            DAGNode('action', TOOL_HANDLER, kwargs=dict(tool_name=HTTP_HANDLER,
                    arguments=dict(url=self.service.origin + '/actions', method='POST', json_body={})),
                    bindings=(InputBinding(('arguments', 'json_body', 'id'),
                                           OutputRef('fetch', ('json', 'items', 0, 'id'))),)),
        ))
        self.orchestrator.submit(workflow)
        result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['action'].result['json']['body'], {'id': 'item-7'})
        self.assertEqual(self.service.effects, [{'id': 'item-7'}])
        self.assertIsNone(self.worker.last_error)

    def test_remote_write_failure_category_survives_transport_and_vetoes_replay(self):
        self.service.replies.append((503, {'error': 'committed before error'}, {}))
        workflow = DAGWorkflow((
            DAGNode('action', TOOL_HANDLER, kwargs=dict(tool_name=HTTP_HANDLER,
                    arguments=dict(url=self.service.origin + '/actions', method='POST', json_body={'key': 1}))),
            DAGNode('tail', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}, depends_on=('action',)),
        ))
        self.orchestrator.submit(workflow)
        result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.FAILED)
        self.assertEqual((result.nodes['action'].attempt_count, result.nodes['action'].retry_count), (1, 0))
        self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
        self.assertEqual(self.service.effects, [{'key': 1}])
        error = self.engine.get_task(result.nodes['action'].task_id).error
        self.assertEqual(error.failure_category.value, 'uncertain')
        self.assertIn('HTTPStatusError', str(error))
        failures = [e for e in self.engine.get_events(result.nodes['action'].task_id)
                    if e.event_type is TaskEventType.FAILED]
        self.assertFalse(failures[0].details['retryable'])
        self.assertIsNone(self.worker.last_error)

    def test_remote_schema_validation_prevents_effects_and_transports_permanent_failure(self):
        for dynamic in (False, True):
            with self.subTest(dynamic=dynamic):
                arguments = dict(url=self.service.origin + '/actions', method='POST',
                                 json_body={'id': 'secret-invalid-id'})
                handler = TOOL_HANDLER if dynamic else 'typed.write'
                kwargs = dict(tool_name='typed.write', arguments=arguments) if dynamic else arguments
                workflow = DAGWorkflow((
                    DAGNode('action', handler, kwargs=kwargs, idempotent=True),
                    DAGNode('tail', HTTP_HANDLER, kwargs={'url': self.service.origin}, depends_on=('action',)),
                ))
                self.orchestrator.submit(workflow)
                result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
                self.assertEqual(result.status, WorkflowStatus.FAILED)
                self.assertEqual((result.nodes['action'].attempt_count, result.nodes['action'].retry_count), (1, 0))
                self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
                task_id = result.nodes['action'].task_id
                error = self.engine.get_task(task_id).error
                self.assertEqual(error.failure_category.value, 'permanent')
                self.assertIn('ToolInputError', str(error))
                self.assertNotIn('secret-invalid-id', str(error))
                failures = [e for e in self.engine.get_events(task_id) if e.event_type is TaskEventType.FAILED]
                self.assertEqual(failures[0].details['failure_category'], 'permanent')
                self.assertFalse(failures[0].details['retryable'])
        self.assertEqual(self.service.calls, [])
        self.assertEqual(self.service.effects, [])
        self.assertIsNone(self.worker.last_error)

    def test_management_api_and_remote_worker_accept_valid_schema_arguments(self):
        service = self.start_management()
        workflow = DAGWorkflow((
            DAGNode('static', 'typed.write', kwargs=dict(url=self.service.origin + '/actions',
                    method='POST', json_body={'id': 1})),
            DAGNode('dynamic', TOOL_HANDLER, kwargs=dict(tool_name='typed.write',
                    arguments=dict(url=self.service.origin + '/actions', method='POST', json_body={'id': 2})),
                    depends_on=('static',)),
        ))
        code, body = self.management_call(service, '/api/v1/workflows', workflow.to_dict())
        self.assertEqual(code, 202, body)
        result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(self.service.effects, [{'id': 1}, {'id': 2}])
        self.assertIsNone(self.worker.last_error)

    def test_remote_read_can_retry_under_the_central_budget(self):
        self.service.replies.append((503, {'error': 'busy'}, {}))
        workflow = DAGWorkflow((DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}),))
        self.orchestrator.submit(workflow)
        result = self.orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual((result.nodes['fetch'].attempt_count, result.nodes['fetch'].retry_count), (2, 1))
        self.assertEqual(len(self.service.calls), 2)
        self.assertIsNone(self.worker.last_error)


if __name__ == '__main__':
    unittest.main()
