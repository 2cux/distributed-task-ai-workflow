"""Faulty wire responses and malformed definitions must not release effects."""
from contextlib import closing
from copy import deepcopy
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from test_llm import HTTPFixture, completion
from tool_test_support import local_service
from workflow_engine import (
    DAGNode, DAGWorkflow, HTTPConfig, HTTP_HANDLER, LLM_HANDLER, NodeStatus,
    OutputRef, RemoteWorker, RetryPolicy, TaskEngine, WorkflowOrchestrator,
    WorkflowStatus, create_http_registry,
)


ROOT = Path(__file__).resolve().parents[1]


class TruncatedModelResponseTests(HTTPFixture):
    def test_truncated_completion_cannot_release_effects_locally_or_remotely(self):
        for remote in (False, True):
            with self.subTest(remote=remote):
                effects = []
                registry = {LLM_HANDLER: self.handler,
                            'effect': lambda result: effects.append(result),
                            'healthy': lambda: 7}
                central = ({name: (lambda **kwargs: None) for name in registry}
                           if remote else registry)
                database = self.path.with_name(f'truncated-{remote}.db')
                engine = TaskEngine(database_path=database, task_registry=central,
                                    max_workers=2, retry_policy=RetryPolicy(2))
                value = completion('private-answer')
                raw = json.dumps(value).encode()
                # Syntactically complete JSON can still be an incomplete HTTP body.
                self.replies.append((200, raw, {'Content-Length': str(len(raw) + 100)}))
                workflow = DAGWorkflow((
                    DAGNode('model', LLM_HANDLER, kwargs={'prompt': 'hi'}),
                    DAGNode('effect', 'effect', inputs={'result': OutputRef('model')}),
                    DAGNode('healthy', 'healthy'),
                ))
                engine.submit_workflow(workflow)
                if remote:
                    service = engine.create_recovery_service('fault-test-token').start()
                    self.addCleanup(service.shutdown)
                    worker = RemoteWorker(service.url, registry, 'fault-test-token', 'fault-worker',
                                          poll_interval=.01, heartbeat_interval=.05)
                    self.addCleanup(worker.shutdown, 5)
                    worker.start()
                run = WorkflowOrchestrator(engine).run(workflow.id, execute=not remote, timeout=5)
                self.assertEqual(run.status, WorkflowStatus.FAILED)
                self.assertEqual(run.nodes['model'].status, NodeStatus.FAILED)
                self.assertEqual((run.nodes['model'].attempt_count, run.nodes['model'].retry_count), (1, 0))
                self.assertIsNone(run.nodes['model'].result)
                self.assertEqual(run.nodes['effect'].status, NodeStatus.SKIPPED)
                self.assertEqual(run.nodes['healthy'].result, 7)
                self.assertEqual(effects, [])
                self.assertEqual(run.llm_usage['call_count'], 1)
                self.assertEqual(run.llm_usage['unknown_usage_calls'], 1)
                self.assertNotIn('private-answer', run.nodes['model'].error)
                if remote:
                    self.assertTrue(worker.shutdown(5))
                    self.assertIsNone(worker.last_error)
                    service.shutdown()
                reopened = TaskEngine(database_path=database, task_registry=central)
                self.assertEqual(reopened.submit_workflow(workflow), run)
                self.assertEqual(reopened.start(), [])
                with closing(sqlite3.connect(database)) as db:
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        self.assertEqual(len(self.requests), 2)


class EmptyHTTPResponseTests(unittest.TestCase):
    def test_empty_json_success_preserves_documented_null_without_replaying_write(self):
        service = local_service(self)
        temporary = tempfile.TemporaryDirectory(dir=ROOT, prefix='.wire-boundaries-')
        self.addCleanup(temporary.cleanup)
        registry = create_http_registry(HTTPConfig((service.origin,)))
        effects = []
        registry['effect'] = lambda result: effects.append(result)
        engine = TaskEngine(database_path=Path(temporary.name) / 'workflow.db',
                            task_registry=registry, retry_policy=RetryPolicy(2))
        for method in ('GET', 'POST'):
            with self.subTest(method=method):
                service.replies.append((200, b'', {}))
                workflow = DAGWorkflow((
                    DAGNode('request', HTTP_HANDLER, kwargs=dict(url=service.origin + '/actions', method=method)),
                    DAGNode('child', 'effect', inputs={'result': OutputRef('request')}),
                ))
                engine.submit_workflow(workflow)
                run = WorkflowOrchestrator(engine).run(workflow.id, timeout=5)
                self.assertEqual(run.status, WorkflowStatus.SUCCESS)
                self.assertEqual((run.nodes['request'].attempt_count, run.nodes['request'].retry_count), (1, 0))
                self.assertEqual(run.nodes['child'].status, NodeStatus.SUCCESS)
                self.assertIsNone(run.nodes['request'].result['json'])
                self.assertEqual(run.nodes['request'].result['text'], '')
        self.assertEqual(len(service.calls), 2)
        self.assertEqual(service.effects, [None])
        self.assertEqual(len(effects), 2)
        self.assertTrue(all(result['json'] is None for result in effects))


class RemoteShutdownBoundaryTests(unittest.TestCase):
    def test_repeated_shutdown_after_control_plane_exit_is_local_and_idempotent(self):
        temporary = tempfile.TemporaryDirectory(dir=ROOT, prefix='.remote-shutdown-')
        self.addCleanup(temporary.cleanup)
        registry = {'abs': abs}
        engine = TaskEngine(database_path=Path(temporary.name) / 'workflow.db', task_registry=registry)
        service = engine.create_recovery_service('shutdown-test-token').start()
        self.addCleanup(service.shutdown)
        worker = RemoteWorker(service.url, registry, 'shutdown-test-token', 'shutdown-worker',
                              heartbeat_interval=.05, poll_interval=.01)
        self.addCleanup(worker.shutdown, 5)
        worker.start()
        self.assertTrue(worker.shutdown(5))
        session = engine.worker_registry.list_sessions()[0]
        self.assertEqual(session.state, 'STOPPED')
        self.assertTrue(service.shutdown())
        self.assertTrue(worker.shutdown(5))
        worker.stop()
        self.assertIsNone(worker.last_error)
        self.assertEqual(engine.worker_registry.list_sessions()[0], session)


class SerializedDefinitionBoundaryTests(unittest.TestCase):
    def test_wrong_container_types_are_rejected_before_coercion(self):
        base = dict(id='invalid', nodes=[dict(id='a', handler=LLM_HANDLER)])
        cases = [dict(base, nodes={}), dict(base, nodes='nodes')]
        for field, value in (('args', 'hi'), ('depends_on', 'a'), ('inputs', []),
                             ('kwargs', []), ('bindings', {}),
                             ('inputs', {'prompt': {'node_id': 'a', 'path': 'text'}}),
                             ('bindings', [{'target': 'prompt', 'source': {'node_id': 'a'}}])):
            invalid = deepcopy(base)
            invalid['nodes'][0][field] = value
            cases.append(invalid)
        for definition in cases:
            with self.subTest(definition=definition), self.assertRaises((TypeError, ValueError)):
                DAGWorkflow.from_dict(definition)

    def test_cli_reports_bad_nested_shape_without_traceback_or_database(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix='.cli-boundaries-') as directory:
            root = Path(directory)
            definition = root / 'workflow.json'
            definition.write_text(json.dumps(dict(id='bad-cli', nodes=[dict(
                id='node', handler=LLM_HANDLER, inputs=[])])), encoding='utf-8')
            database = root / 'workflow.db'
            environment = {key: value for key, value in os.environ.items()
                           if not key.startswith(('LLM_', 'APP_LLM_', 'HTTP_'))}
            environment.update(PYTHONPATH=str(ROOT / 'src'), LLM_API_KEY='test-only', LLM_MODEL='test-model')
            process = subprocess.run([sys.executable, '-m', 'workflow_engine.llm_cli',
                '--database', str(database), '--workflow', str(definition)],
                env=environment, cwd=ROOT, capture_output=True, text=True, timeout=10)
            self.assertEqual(process.returncode, 2, process.stderr)
            self.assertNotIn('Traceback', process.stderr)
            self.assertFalse(database.exists())
