"""Independent failure/replay regressions; no credentials or external model calls."""
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
from pathlib import Path
import socket
import ssl
import tempfile
from threading import Thread
import time
import unittest
from unittest.mock import patch
from urllib.error import URLError

from workflow_engine import (
    DAGNode, DAGWorkflow, FailureCategory, LLMConfig, LLMHTTPError,
    LLMOutputError, LLMResponseError, LLMTimeoutError, LLM_HANDLER,
    LLM_OUTPUT_HANDLER, NodeStatus, OpenAIChatHandler, OutputRef, RemoteWorker,
    RetryPolicy, RetryPolicyError, Task, TaskEngine, TaskEventType, TaskStatus,
    WorkflowOrchestrator, WorkflowStatus, create_llm_registry, validate_llm_output,
)


SECRET = 'failure-tests-private-key'


def completion():
    return dict(id='saved-response', choices=[dict(message=dict(content='42'), finish_reason='stop')],
                usage=dict(prompt_tokens=3, completion_tokens=1, total_tokens=4))


class FailurePolicyTests(unittest.TestCase):
    def test_http_classification_distinguishes_rejected_and_unknown_execution(self):
        groups = {
            FailureCategory.PERMANENT: (302, 400, 401, 403, 404, 413, 422, 501, 505),
            FailureCategory.TRANSIENT: (429, 503),
            FailureCategory.UNCERTAIN: (408, 500, 502, 504),
        }
        for category, codes in groups.items():
            for code in codes:
                with self.subTest(code=code):
                    self.assertEqual(LLMHTTPError(code).failure_category, category)

    def test_uncertain_and_permanent_errors_cannot_consume_automatic_budget(self):
        for error in (LLMHTTPError(401), LLMTimeoutError('timeout'),
                      LLMResponseError('bad response'), LLMOutputError('contract')):
            with self.subTest(error=type(error).__name__):
                task = Task('model', lambda: None, status=TaskStatus.FAILED, error=error, max_retries=10)
                policy = RetryPolicy(10)
                self.assertTrue(policy.can_retry(task))  # Budget is independent of safety.
                self.assertFalse(policy.should_retry(task))
                with self.assertRaises(RetryPolicyError):
                    policy.begin_retry(task)
                self.assertEqual(task.retry_count, 0)

    def test_transport_faults_retry_only_when_request_was_not_sent(self):
        faults = (
            (URLError(socket.gaierror(socket.EAI_AGAIN, SECRET)), True),
            (URLError(socket.gaierror(socket.EAI_NONAME, SECRET)), False),
            (URLError(ConnectionRefusedError(SECRET)), True),
            (ConnectionRefusedError(SECRET), True),
            (URLError(ssl.SSLCertVerificationError(1, SECRET)), False),
            (URLError(ssl.SSLError(1, SECRET)), False),
            (URLError(PermissionError(SECRET)), False),
            (URLError(ConnectionResetError(SECRET)), False),
            (URLError(TimeoutError(SECRET)), False),
            (TimeoutError(SECRET), False),
        )
        for fault, retry in faults:
            with self.subTest(fault=type(fault).__name__, reason=type(getattr(fault, 'reason', fault)).__name__):
                handler = OpenAIChatHandler(LLMConfig(SECRET, 'model', 'http://unused.invalid/v1'))
                response = BytesIO(json.dumps(completion()).encode())
                response.status = 200
                with patch('workflow_engine.llm.build_opener') as opener:
                    opener.return_value.open.side_effect = [fault, response]
                    engine = TaskEngine(retry_policy=RetryPolicy(4))
                    task = engine.submit(Task('alias-model', handler, kwargs={'prompt': 'hi'}))
                    engine.start()
                    self.assertEqual(opener.return_value.open.call_count, 2 if retry else 1)
                self.assertEqual(task.status, TaskStatus.SUCCESS if retry else TaskStatus.FAILED)
                self.assertEqual(task.retry_count, int(retry))
                self.assertNotIn(SECRET, str(task.error))
                failed = next(e for e in task.events if e.event_type is TaskEventType.FAILED)
                self.assertIn('failure_category', failed.details)

    def test_error_while_reading_a_success_response_is_always_uncertain(self):
        # Even a normally pre-send error is unsafe once a 200 response exists.
        handler = OpenAIChatHandler(LLMConfig(SECRET, 'model'))
        with patch('workflow_engine.llm.build_opener') as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.status = 200
            response.read.side_effect = ssl.SSLError(1, SECRET)
            engine = TaskEngine(retry_policy=RetryPolicy(5))
            task = engine.submit(Task('model', handler, kwargs={'prompt': 'hi'}))
            engine.start()
            self.assertEqual(opener.return_value.open.call_count, 1)
        self.assertEqual(task.error.failure_category, FailureCategory.UNCERTAIN)
        self.assertNotIn(SECRET, str(task.error))

    def test_ordinary_business_errors_keep_existing_retry_behavior(self):
        calls = []
        def business():
            calls.append(1)
            if len(calls) == 1:
                raise ValueError('temporary business failure')
            return 42
        engine = TaskEngine(retry_policy=RetryPolicy(1))
        task = engine.submit(Task('business', business))
        engine.start()
        self.assertEqual((task.result, task.retry_count, len(calls)), (42, 1, 2))


class LLMFailureIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1],
                                               prefix='.llm-failure-tests-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'tasks.db'
        self.replies = deque()
        self.calls = []
        self.delay = 0
        fixture = self

        class Provider(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                fixture.calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                status, body = fixture.replies.popleft() if fixture.replies else (200, completion())
                time.sleep(fixture.delay)
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except OSError:
                    pass  # Timed-out client deliberately closed its connection.

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
        self.thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_provider)
        self.config = LLMConfig(SECRET, 'model', f'http://127.0.0.1:{self.server.server_port}/v1')
        self.registry = create_llm_registry(self.config)

    def close_provider(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def engine(self, path=None, registry=None, **kwargs):
        return TaskEngine(database_path=path or self.path, task_registry=registry or self.registry,
                          retry_policy=RetryPolicy(3, initial_delay=.01), **kwargs)

    def model_task(self, engine, **kwargs):
        return engine.submit(Task('model', self.registry[LLM_HANDLER], kwargs={'prompt': 'hi'}, **kwargs))

    def assert_failed(self, engine, task, category, attempts=1, retries=0):
        saved = engine.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count),
                         (TaskStatus.FAILED, attempts, retries))
        self.assertEqual(saved.error.failure_category, category)
        failed = [e for e in engine.get_events(task.id) if e.event_type is TaskEventType.FAILED]
        self.assertEqual(len(failed), attempts)
        self.assertEqual(failed[-1].details['failure_category'], category.value)
        self.assertEqual(failed[-1].details['retryable'], category is FailureCategory.TRANSIENT)
        self.assertNotIn(SECRET, str(failed[-1].details))
        return saved

    def test_permanent_rejection_stops_dag_and_remains_stopped_after_restart(self):
        self.replies.append((401, {'error': {'message': SECRET}}))
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('generate', LLM_HANDLER, kwargs={'prompt': 'hi'}, max_retries=8),
                            DAGNode('tail', LLM_HANDLER, inputs={'prompt': OutputRef('generate', ('text',))})))
        orchestrator = WorkflowOrchestrator(engine)
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual(run.nodes['tail'].status, NodeStatus.SKIPPED)
        task = engine.get_task(run.nodes['generate'].task_id)
        self.assert_failed(engine, task, FailureCategory.PERMANENT)
        reopened = self.engine(recover_interrupted=True)
        self.assertEqual(WorkflowOrchestrator(reopened).run(flow.id, timeout=2), run)
        self.assertFalse(RetryPolicy(99).should_retry(reopened.get_task(task.id)))
        self.assertEqual(len(self.calls), 1)

    def test_quota_exhaustion_is_permanent_even_though_http_status_is_429(self):
        for field in ('code', 'type'):
            with self.subTest(field=field):
                self.replies.append((429, {'error': {field: 'insufficient_quota', 'message': SECRET}}))
                engine = self.engine(self.path.with_name(field + '.db'))
                task = self.model_task(engine)
                before = len(self.calls)
                engine.start()
                self.assert_failed(engine, task, FailureCategory.PERMANENT)
                self.assertEqual(len(self.calls), before + 1)

    def test_rate_limit_uses_backoff_and_preserves_metadata_through_restart(self):
        self.replies.append((429, {'error': {'code': 'rate_limit_exceeded', 'message': SECRET}}))
        engine = self.engine()
        task = self.model_task(engine)
        engine.start()
        saved = self.engine().get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.SUCCESS, 2, 1))
        self.assertEqual(saved.last_error.failure_category, FailureCategory.TRANSIENT)
        scheduled = next(e for e in saved.events if e.event_type is TaskEventType.RETRY_SCHEDULED)
        self.assertEqual(scheduled.details['retry_delay'], .01)
        self.assertIsNotNone(scheduled.details['retry_at'])
        self.engine().start()
        self.assertEqual(len(self.calls), 2)

    def test_temporary_service_failure_respects_task_budget(self):
        self.replies.extend([(503, {'error': SECRET})] * 4)
        engine = self.engine()
        task = self.model_task(engine, max_retries=1)
        engine.start()
        self.assert_failed(engine, task, FailureCategory.TRANSIENT, attempts=2, retries=1)
        self.assertEqual(len(self.calls), 2)

    def test_ambiguous_http_errors_do_not_repeat_model_calls(self):
        for status in (408, 500, 502, 504):
            with self.subTest(status=status):
                self.replies.append((status, {'error': SECRET}))
                engine = self.engine(self.path.with_name(f'http-{status}.db'))
                task = self.model_task(engine, idempotent=True)
                before = len(self.calls)
                engine.start()
                self.assert_failed(engine, task, FailureCategory.UNCERTAIN)
                self.assertEqual(len(self.calls), before + 1)

    def test_timeout_does_not_replay_after_provider_received_request(self):
        self.delay = .1
        self.registry[LLM_HANDLER] = OpenAIChatHandler(LLMConfig(
            SECRET, 'model', self.config.base_url, timeout=.03))
        engine = self.engine()
        task = self.model_task(engine, idempotent=True)
        engine.start()
        self.assert_failed(engine, task, FailureCategory.UNCERTAIN)
        self.assertEqual(len(self.calls), 1)
        self.engine(recover_interrupted=True).start()
        self.assertEqual(len(self.calls), 1)

    def test_invalid_success_response_is_not_a_reason_to_regenerate(self):
        self.replies.append((200, b'invalid-json-' + SECRET.encode()))
        engine = self.engine()
        task = self.model_task(engine)
        engine.start()
        self.assert_failed(engine, task, FailureCategory.UNCERTAIN)
        self.assertEqual(len(self.calls), 1)

    def test_invalid_arguments_fail_before_http_and_do_not_retry(self):
        engine = self.engine()
        task = engine.submit(Task('invalid', self.registry[LLM_HANDLER], kwargs={'prompt': 'hi', 'max_tokens': -1}))
        engine.start()
        self.assert_failed(engine, task, FailureCategory.PERMANENT)
        self.assertEqual(self.calls, [])

    def test_output_validation_failure_preserves_single_paid_generation(self):
        engine = self.engine()
        flow = DAGWorkflow((DAGNode('generate', LLM_HANDLER, kwargs={'prompt': 'hi'}),
                            DAGNode('validate', LLM_OUTPUT_HANDLER, kwargs={'expected_text': 'different'},
                                    inputs={'result': OutputRef('generate')}, max_retries=5)))
        orchestrator = WorkflowOrchestrator(engine)
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.nodes['generate'].status, NodeStatus.SUCCESS)
        self.assertEqual(run.nodes['generate'].result['response_id'], 'saved-response')
        self.assert_failed(engine, engine.get_task(run.nodes['validate'].task_id), FailureCategory.PERMANENT)
        self.assertEqual(len(self.calls), 1)

    def test_task_soft_timeout_is_uncertain_even_if_http_later_succeeds(self):
        self.delay = .08
        engine = self.engine()
        task = self.model_task(engine, timeout=.02, idempotent=True)
        engine.start()
        self.assert_failed(engine, task, FailureCategory.UNCERTAIN)
        self.assertEqual(len(self.calls), 1)

    def test_interrupted_model_task_is_not_replayed_even_if_marked_idempotent(self):
        engine = self.engine()
        task = self.model_task(engine, idempotent=True)
        claimed = engine._store.claim(task, 'stopped-owner')
        self.assertIsNotNone(claimed)
        # Simulate the provider finishing before the worker could commit.
        self.registry[LLM_HANDLER](prompt='hi')
        reopened = self.engine(recover_interrupted=True)
        reopened.start()
        saved = reopened.get_task(task.id)
        self.assertEqual((saved.status, saved.retry_count, saved.attempt_count), (TaskStatus.FAILED, 0, 1))
        self.assertEqual(saved.error.failure_category, FailureCategory.UNCERTAIN)
        self.assertEqual(len(self.calls), 1)

    def test_explicit_operator_recovery_can_override_category_after_reconciliation(self):
        self.replies.append((401, {}))
        engine = self.engine()
        task = self.model_task(engine, idempotent=True)
        engine.start()
        recovery = engine.create_recovery_system()
        recovery.tick()
        incident = next(i for i in recovery.list_incidents() if i.task_id == task.id)
        recovery.resolve_task(incident.id, action='retry', operator='test-operator',
                              reason='corrected credentials and confirmed prior request was rejected')
        reopened = self.engine()
        reopened.start()
        saved = reopened.get_task(task.id)
        self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.SUCCESS, 2, 1))
        self.assertEqual(len(self.calls), 2)

    def test_legacy_remote_errors_without_category_fail_closed(self):
        engine = self.engine()
        service = engine.create_recovery_service('test-rpc-token')
        self.addCleanup(service.shutdown)
        session = engine.worker_registry.register('legacy-worker')
        for error_type, category in (('LLMHTTPError', FailureCategory.UNCERTAIN),
                                     ('TaskTimeoutError', FailureCategory.UNCERTAIN),
                                     ('ValueError', FailureCategory.PERMANENT)):
            with self.subTest(error_type=error_type):
                task = self.model_task(engine)
                engine._store.claim_next(session.session_id, 'legacy-owner')
                service._finish(task.id, 'legacy-owner', session.session_id, 1, 'FAILED', None,
                                dict(type=error_type, message='legacy failure', non_retryable=False))
                self.assert_failed(engine, task, category)
        self.assertEqual(self.calls, [])

    def test_remote_rejects_contradictory_failure_metadata_without_committing(self):
        engine = self.engine()
        service = engine.create_recovery_service('test-rpc-token')
        self.addCleanup(service.shutdown)
        session = engine.worker_registry.register('bad-metadata-worker')
        task = self.model_task(engine)
        engine._store.claim_next(session.session_id, 'owner')
        with self.assertRaises(ValueError):
            service._finish(task.id, 'owner', session.session_id, 1, 'FAILED', None,
                            dict(type='LLMHTTPError', message='failure', non_retryable=False,
                                 failure_category='uncertain', retryable=True))
        self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
        self.assertEqual(engine.get_task(task.id).retry_count, 0)

    def test_remote_worker_and_control_plane_preserve_selective_retry(self):
        for status, category, calls in ((401, FailureCategory.PERMANENT, 1),
                                        (429, FailureCategory.TRANSIENT, 2),
                                        (504, FailureCategory.UNCERTAIN, 1)):
            with self.subTest(status=status):
                self.replies.append((status, {'error': SECRET}))
                # Central placeholders must not execute or erase remote metadata.
                central_registry = {LLM_HANDLER: lambda **kwargs: self.fail('central model call'),
                                    LLM_OUTPUT_HANDLER: validate_llm_output}
                engine = self.engine(self.path.with_name(f'remote-{status}.db'), registry=central_registry)
                service = engine.create_recovery_service('test-rpc-token').start()
                worker = RemoteWorker(service.url, self.registry, 'test-rpc-token', f'remote-{status}',
                                      poll_interval=.01, heartbeat_interval=.05)
                before = len(self.calls)
                try:
                    worker.start()
                    orchestrator = WorkflowOrchestrator(engine)
                    flow = DAGWorkflow((DAGNode('model', LLM_HANDLER, kwargs={'prompt': 'hi'}),))
                    orchestrator.submit(flow)
                    run = orchestrator.run(flow.id, execute=False, timeout=5)
                    saved = engine.get_task(run.nodes['model'].task_id)
                    if category is FailureCategory.TRANSIENT:
                        self.assertEqual((saved.status, saved.retry_count), (TaskStatus.SUCCESS, 1))
                        self.assertEqual(saved.last_error.failure_category, category)
                    else:
                        self.assert_failed(engine, saved, category)
                    self.assertEqual(len(self.calls), before + calls)
                    self.assertIsNone(worker.last_error)
                finally:
                    worker.shutdown(5)
                    service.shutdown()


if __name__ == '__main__':
    unittest.main()
