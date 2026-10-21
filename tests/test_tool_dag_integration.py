"""Durable DAG integration: real HTTP effects, nested bindings and retry gates."""
from pathlib import Path
from contextlib import closing
import sqlite3
import tempfile
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, HTTPConfig, HTTPRequestHandler, HTTP_HANDLER,
                             InputBinding, NodeStatus, OutputRef, RetryPolicy, TOOL_HANDLER,
                             TaskEngine, TaskEventType, ToolRegistry, WorkflowOrchestrator, WorkflowStatus)
from tool_test_support import local_service


class ToolDAGTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.tool-dag-')
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / 'workflow.db'
        self.service = local_service(self)
        self.registry = ToolRegistry()
        self.registry.register(HTTP_HANDLER, HTTPRequestHandler(HTTPConfig((self.service.origin,),
                               default_headers={'Authorization': 'Bearer only-on-worker'})))
        self.registry.register('identity', lambda data: data)
        self.handlers = self.registry.to_task_registry()
        self.engine = TaskEngine(database_path=self.database, task_registry=self.handlers,
                                 retry_policy=RetryPolicy(2, initial_delay=.01), max_workers=2)
        self.orchestrator = WorkflowOrchestrator(self.engine)

    def run_flow(self, *nodes):
        workflow = DAGWorkflow(tuple(nodes))
        self.orchestrator.submit(workflow)
        return self.orchestrator.run(workflow.id, timeout=10)

    def test_fetch_nested_fields_post_action_and_join_survive_reopen(self):
        workflow = DAGWorkflow((
            DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}),
            DAGNode('action', HTTP_HANDLER,
                    kwargs=dict(url=self.service.origin + '/actions', method='POST',
                                json_body={'kind': 'archive', 'items': [{'fixed': 42}]}),
                    bindings=(InputBinding(('json_body', 'items', 0, 'id'), OutputRef('fetch', ('json', 'items', 0, 'id'))),
                              InputBinding(('json_body', 'label'), OutputRef('fetch', ('json', 'items', 0, 'label'))))),
            DAGNode('result', 'identity', inputs={'data': OutputRef('action', ('json', 'body'))}),
        ))
        self.assertEqual(workflow.topological_order(), ('fetch', 'action', 'result'))
        encoded = workflow.to_dict()
        self.assertEqual(DAGWorkflow.from_dict(encoded), workflow)
        self.orchestrator.submit(DAGWorkflow.from_dict(encoded))
        result = self.orchestrator.run(workflow.id, timeout=10)
        expected = {'kind': 'archive', 'items': [{'fixed': 42, 'id': 'item-7'}], 'label': '资料'}
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['result'].result, expected)
        self.assertEqual(self.service.effects, [expected])
        self.assertEqual(len(self.service.calls), 2)
        task = self.engine.get_task(result.nodes['action'].task_id)
        self.assertEqual(task.kwargs['json_body'], expected)
        reopened = WorkflowOrchestrator(TaskEngine(database_path=self.database, task_registry=self.handlers))
        self.assertEqual(reopened.run(workflow.id, timeout=1), result)
        self.assertEqual(reopened.submit(workflow), result)
        self.assertEqual(len(self.service.calls), 2)
        # Worker credentials did not enter task definitions, events or results.
        with closing(sqlite3.connect(self.database)) as db:
            for table, columns in (('tasks', 'definition,result,error'), ('task_events', 'details')):
                for row in db.execute(f'SELECT {columns} FROM {table}'):
                    self.assertNotIn('only-on-worker', str(row))

    def test_dynamic_planner_output_dispatches_only_a_registered_tool(self):
        # Deterministic planner output; the action itself is a real HTTP call.
        plan = {'name': HTTP_HANDLER, 'arguments': dict(url=self.service.origin + '/actions',
                                                     method='POST', json_body={'id': 9})}
        result = self.run_flow(
            DAGNode('plan', 'identity', kwargs={'data': plan}),
            DAGNode('execute', TOOL_HANDLER, inputs={'tool_name': OutputRef('plan', ('name',)),
                                                     'arguments': OutputRef('plan', ('arguments',))}),
            DAGNode('validate', 'identity', inputs={'data': OutputRef('execute', ('json', 'body', 'id'))}),
        )
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['validate'].result, 9)
        self.assertEqual(self.service.effects, [{'id': 9}])

    def test_unknown_dynamic_tool_fails_once_and_skips_only_its_descendants(self):
        result = self.run_flow(
            DAGNode('bad', TOOL_HANDLER, kwargs={'tool_name': 'unregistered', 'arguments': {}}),
            DAGNode('tail', 'identity', inputs={'data': OutputRef('bad')}),
            DAGNode('independent', 'identity', kwargs={'data': 7}),
        )
        self.assertEqual(result.status, WorkflowStatus.FAILED)
        self.assertEqual((result.nodes['bad'].attempt_count, result.nodes['bad'].retry_count), (1, 0))
        self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
        self.assertEqual(result.nodes['independent'].result, 7)
        self.assertEqual(self.service.calls, [])

    def test_read_transient_failure_is_retried_before_downstream_action(self):
        self.service.replies.append((503, {'error': 'busy'}, {}))
        result = self.run_flow(
            DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.service.origin + '/items'}),
            DAGNode('action', HTTP_HANDLER, kwargs=dict(url=self.service.origin + '/actions', method='POST'),
                    inputs={'json_body': OutputRef('fetch', ('json',))}),
        )
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual((result.nodes['fetch'].attempt_count, result.nodes['fetch'].retry_count), (2, 1))
        self.assertEqual([c['method'] for c in self.service.calls], ['GET', 'GET', 'POST'])
        self.assertEqual(len(self.service.effects), 1)
        failures = [e for e in self.engine.get_events(result.nodes['fetch'].task_id)
                    if e.event_type is TaskEventType.FAILED]
        self.assertEqual(failures[0].details['failure_category'], 'transient')

    def test_write_server_error_preserves_uncertainty_and_does_not_repeat_effect(self):
        self.service.replies.append((503, {'error': 'failed after commit'}, {}))
        result = self.run_flow(
            DAGNode('action', HTTP_HANDLER, kwargs=dict(url=self.service.origin + '/actions', method='POST',
                                                       json_body={'key': 'one-effect'}), idempotent=True),
            DAGNode('tail', 'identity', inputs={'data': OutputRef('action')}),
        )
        self.assertEqual(result.status, WorkflowStatus.FAILED)
        self.assertEqual((result.nodes['action'].attempt_count, result.nodes['action'].retry_count), (1, 0))
        self.assertEqual(self.service.effects, [{'key': 'one-effect'}])
        self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
        error = self.engine.get_task(result.nodes['action'].task_id).error
        self.assertEqual(error.failure_category.value, 'uncertain')

    def test_invalid_source_and_target_fail_binding_before_any_http_effect(self):
        for target, path in ((('json_body', 'id'), ('missing',)),
                             (('json_body', 'missing', 'id'), ('id',)),
                             (('json_body', 'items', 5), ('id',)),
                             (('json_body', 'items', 'bad'), ('id',))):
            with self.subTest(target=target, path=path):
                result = self.run_flow(
                    DAGNode('source', 'identity', kwargs={'data': {'id': 1}}),
                    DAGNode('action', HTTP_HANDLER, kwargs=dict(url=self.service.origin + '/actions',
                        method='POST', json_body={'items': []}),
                        bindings=(InputBinding(target, OutputRef('source', path)),)),
                    DAGNode('tail', 'identity', inputs={'data': OutputRef('action')}),
                )
                self.assertEqual(result.nodes['action'].status, NodeStatus.FAILED)
                self.assertEqual(result.nodes['action'].attempt_count, 0)
                self.assertIn('input binding failed', result.nodes['action'].error)
                self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
        self.assertEqual(self.service.calls, [])


class NestedBindingDefinitionTests(unittest.TestCase):
    def test_overlapping_targets_negative_indices_and_invalid_types_are_rejected(self):
        source = OutputRef('source')
        for target in ((), ('body', -1), (0, 'id'), ('body', True)):
            with self.subTest(target=target), self.assertRaises(ValueError):
                InputBinding(target, source)
        for targets in ((('body',), ('body', 'id')), (('body', 'id'), ('body', 'id'))):
            with self.subTest(targets=targets), self.assertRaises(ValueError):
                DAGNode('n', 'f', bindings=tuple(InputBinding(t, source) for t in targets))
        with self.assertRaises(ValueError):
            DAGNode('n', 'f', inputs={'body': source}, bindings=(InputBinding(('body', 'id'), source),))

    def test_nested_dependencies_validate_unknown_sources_and_cycles(self):
        with self.assertRaises(ValueError):
            DAGWorkflow((DAGNode('a', 'f', bindings=(InputBinding(('x',), OutputRef('unknown')),)),))
        with self.assertRaises(ValueError):
            DAGWorkflow((DAGNode('a', 'f', bindings=(InputBinding(('x',), OutputRef('b')),)),
                         DAGNode('b', 'f', inputs={'x': OutputRef('a')})))

    def test_materialization_does_not_share_sibling_values_or_static_inputs(self):
        node = DAGNode('n', 'f', kwargs={'body': {'fixed': 1}}, bindings=(
            InputBinding(('body', 'left'), OutputRef('s')),
            InputBinding(('body', 'right'), OutputRef('s'))))
        output = {'items': [1]}
        kwargs = node.materialize_kwargs({'s': output})
        kwargs['body']['left']['items'].append(2)
        self.assertEqual(kwargs['body']['right'], {'items': [1]})
        self.assertEqual(output, {'items': [1]})
        self.assertEqual(node.kwargs, {'body': {'fixed': 1}})
        self.assertNotIn('bindings', DAGNode('legacy', 'f').to_dict())


if __name__ == '__main__':
    unittest.main()
