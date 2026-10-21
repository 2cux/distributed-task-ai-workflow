"""Real HTTP transport against a local provider fixture; never uses paid credentials."""
from copy import deepcopy
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import socket
import ssl
import subprocess
import sys
import tempfile
from threading import Thread
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import URLError

from workflow_engine import (
    DAGNode, DAGWorkflow, LLMConfig, LLMError, LLMHTTPError, LLMResponseError,
    LLMTimeoutError, LLM_HANDLER, NodeStatus, OpenAIChatHandler, OutputRef,
    RemoteWorker, RetryPolicy, TaskEngine, TaskEventType, WorkflowOrchestrator,
    WorkflowStatus, create_llm_registry,
    LLM_OUTPUT_HANDLER, LLMOutputError, validate_llm_output,
)


SECRET = 'test-only-llm-key'
ROOT = Path(__file__).resolve().parents[1]


def completion(text='模型回答', model='provider-model'):
    return dict(id='chat-test', model=model,
                choices=[dict(index=0, message=dict(role='assistant', content=text), finish_reason='stop')],
                usage=dict(prompt_tokens=11, completion_tokens=7, total_tokens=18,
                           prompt_tokens_details=dict(cached_tokens=3)))


class LLMConfigTests(unittest.TestCase):
    def test_environment_configuration_and_safe_repr(self):
        config = LLMConfig.from_env(dict(LLM_API_KEY=SECRET, LLM_MODEL='my-model',
                                        LLM_BASE_URL='https://models.example/v1/', LLM_TIMEOUT='12.5'))
        self.assertEqual(config.model, 'my-model')
        self.assertEqual(config.base_url, 'https://models.example/v1')
        self.assertEqual(config.timeout, 12.5)
        self.assertNotIn(SECRET, repr(config))
        self.assertEqual(LLMConfig(SECRET, 'model').timeout, 60)
        with patch.dict(os.environ, {'LLM_API_KEY': SECRET, 'LLM_MODEL': 'model'}, clear=True):
            self.assertEqual(set(create_llm_registry()), {LLM_HANDLER, LLM_OUTPUT_HANDLER})

    def test_app_environment_full_endpoint_and_explicit_primary_precedence(self):
        app = dict(APP_LLM_API_KEY=SECRET, APP_LLM_MODEL='app-model',
                   APP_LLM_BASE_URL='https://models.example/v1/chat/completions/', APP_LLM_TIMEOUT='7')
        config = LLMConfig.from_env(app)
        self.assertEqual((config.model, config.base_url, config.timeout),
                         ('app-model', 'https://models.example/v1', 7))
        primary = dict(app, LLM_API_KEY='primary-key', LLM_MODEL='primary-model',
                       LLM_BASE_URL='https://primary.example/v2', LLM_TIMEOUT='9')
        config = LLMConfig.from_env(primary)
        self.assertEqual((config.api_key, config.model, config.base_url, config.timeout),
                         ('primary-key', 'primary-model', 'https://primary.example/v2', 9))
        for name in ('API_KEY', 'MODEL', 'BASE_URL', 'TIMEOUT'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                LLMConfig.from_env(dict(app, **{'LLM_' + name: ''}))

    def test_invalid_config_fails_before_execution_without_echoing_values(self):
        for values in ({}, {'LLM_API_KEY': SECRET}, {'LLM_MODEL': 'model'},
                       {'LLM_API_KEY': SECRET, 'LLM_MODEL': 'model', 'LLM_TIMEOUT': 'nan'}):
            with self.subTest(values=list(values)):
                with self.assertRaises(ValueError) as caught:
                    LLMConfig.from_env(values)
                self.assertNotIn(SECRET, str(caught.exception))
        for overrides in (dict(api_key=SECRET + '\n'), dict(model=' '), dict(timeout=True),
                          dict(timeout=0), dict(timeout=float('inf')), dict(max_response_bytes=False),
                          dict(base_url='file:///tmp/key'), dict(base_url='https://user:secret@host/v1'),
                          dict(base_url='https://host/v1?key=' + SECRET),
                          dict(base_url='https://host/v1#fragment'), dict(base_url='http://host:99999'),
                          dict(base_url='https://host/with space')):
            with self.subTest(overrides=list(overrides)):
                with self.assertRaises(ValueError):
                    LLMConfig(**dict(dict(api_key=SECRET, model='model'), **overrides))


class HTTPFixture(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=ROOT, prefix='.llm-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'llm.db'
        self.requests = []
        self.replies = []
        self.delay = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                fixture.requests.append(dict(path=self.path, authorization=self.headers.get('Authorization'), body=body))
                status, payload, headers = (fixture.replies.pop(0) if fixture.replies else
                                           (200, completion(), {}))
                time.sleep(fixture.delay)
                raw = payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False).encode('utf-8')
                try:
                    self.send_response(status)
                    self.send_header('Content-Type', 'application/json')
                    if not any(key.lower() == 'content-length' for key in headers):
                        self.send_header('Content-Length', str(len(raw)))
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.end_headers()
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)
        self.base_url = f'http://127.0.0.1:{self.server.server_port}/custom/v1'
        self.config = LLMConfig(SECRET, 'configured-model', self.base_url, timeout=2)
        self.handler = OpenAIChatHandler(self.config)
        self.registry = {LLM_HANDLER: self.handler, LLM_OUTPUT_HANDLER: validate_llm_output}

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def engine(self, **options):
        return TaskEngine(database_path=self.path, task_registry=self.registry, **options)


class LLMHTTPTests(HTTPFixture):
    def test_full_endpoint_is_normalized_before_real_http_request(self):
        handler = OpenAIChatHandler(LLMConfig(SECRET, 'model', self.base_url + '/chat/completions/'))
        handler('hi')
        self.assertEqual(self.requests[0]['path'], '/custom/v1/chat/completions')

    def test_network_errors_are_classified_without_private_exception_text(self):
        reasons = [(socket.gaierror(11001, SECRET), 'DNS resolution failed'),
                   (ssl.SSLCertVerificationError(1, SECRET), 'TLS certificate verification failed'),
                   (ssl.SSLError(1, SECRET), 'TLS negotiation failed'),
                   (ConnectionRefusedError(10061, SECRET), 'connection refused'),
                   (PermissionError(10013, SECRET), 'network access denied')]
        for reason, expected in reasons:
            with self.subTest(kind=type(reason).__name__):
                with patch('workflow_engine.llm.build_opener') as opener:
                    opener.return_value.open.side_effect = URLError(reason)
                    with self.assertRaisesRegex(LLMError, expected) as caught:
                        self.handler('hi')
                self.assertNotIn(SECRET, str(caught.exception))

    def test_real_request_auth_unicode_and_provider_usage(self):
        result = self.handler('介绍 DAG', system_prompt='用中文回答')
        sent = self.requests[0]
        self.assertEqual(sent['path'], '/custom/v1/chat/completions')
        self.assertEqual(sent['authorization'], 'Bearer ' + SECRET)
        self.assertEqual(sent['body'], dict(model='configured-model', stream=False,
                         messages=[dict(role='system', content='用中文回答'), dict(role='user', content='介绍 DAG')]))
        self.assertEqual(result['text'], '模型回答')
        self.assertEqual(result['model'], 'provider-model')
        self.assertEqual(result['requested_model'], 'configured-model')
        self.assertEqual(result['response_id'], 'chat-test')
        self.assertEqual(result['finish_reason'], 'stop')
        self.assertEqual(result['usage'], completion()['usage'])

    def test_messages_and_per_node_options(self):
        messages = [dict(role='developer', content='Be concise'), dict(role='user', content='hello')]
        self.handler(messages=messages, model='override', temperature=0, max_completion_tokens=99)
        sent = self.requests[-1]['body']
        self.assertEqual(sent['model'], 'override')
        self.assertEqual(sent['messages'], messages)
        self.assertEqual(sent['temperature'], 0)
        self.assertEqual(sent['max_completion_tokens'], 99)
        self.assertNotIn('max_tokens', sent)
        self.handler('hi', max_tokens=10)
        self.assertEqual(self.requests[-1]['body']['max_tokens'], 10)
        self.assertNotIn('max_completion_tokens', self.requests[-1]['body'])

    def test_invalid_node_inputs_do_not_make_requests(self):
        for kwargs in ({}, dict(prompt=' '), dict(prompt='hi', messages=[]), dict(messages=[]),
                       dict(messages=[dict(role='tool', content='hi')]),
                       dict(messages=[dict(role='user', content=123)]),
                       dict(prompt='hi', temperature=float('nan')), dict(prompt='hi', temperature=True),
                       dict(prompt='hi', max_tokens=0), dict(prompt='hi', max_tokens=True),
                       dict(prompt='hi', max_tokens=1, max_completion_tokens=1), dict(prompt='hi', model='')):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    self.handler(**kwargs)
        self.assertEqual(self.requests, [])

    def test_missing_partial_and_zero_usage_remain_distinct(self):
        for usage, expected in ((None, (None, None, None)), ({'prompt_tokens': 9}, (9, None, None)),
                               (dict(prompt_tokens=0, completion_tokens=0, total_tokens=0), (0, 0, 0))):
            value = completion()
            value['usage'] = usage
            self.replies.append((200, value, {}))
            actual = self.handler('hi')['usage']
            self.assertEqual(tuple(actual[key] for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')), expected)

    def test_malformed_responses_fail_without_echoing_body(self):
        malformed = [b'not json ' + SECRET.encode(), {'choices': []}, [],
                     dict(completion(), usage={'prompt_tokens': -1}),
                     dict(completion(), usage={'total_tokens': True}),
                     dict(completion(), usage={'completion_tokens': '3'}),
                     dict(completion(), usage={'details': float('inf')})]
        no_text = deepcopy(completion())
        no_text['choices'][0]['message']['content'] = None
        malformed.append(no_text)
        for value in malformed:
            with self.subTest(value_type=type(value)):
                self.replies.append((200, value, {}))
                with self.assertRaises(LLMResponseError) as caught:
                    self.handler('hi')
                self.assertNotIn(SECRET, str(caught.exception))

    def test_http_failure_is_sanitized_and_not_retried_by_adapter(self):
        for status in (401, 429, 503):
            before = len(self.requests)
            self.replies.append((status, {'error': SECRET}, {}))
            with self.assertRaises(LLMHTTPError) as caught:
                self.handler('hi')
            self.assertEqual(caught.exception.status_code, status)
            self.assertNotIn(SECRET, str(caught.exception))
            self.assertEqual(len(self.requests), before + 1)

    def test_redirect_is_not_followed(self):
        self.replies.append((302, {}, {'Location': self.base_url + '/stolen'}))
        with self.assertRaises(LLMHTTPError) as caught:
            self.handler('hi')
        self.assertEqual(caught.exception.status_code, 302)
        self.assertEqual(len(self.requests), 1)

    def test_client_timeout_and_response_limit(self):
        self.delay = .2
        handler = OpenAIChatHandler(LLMConfig(SECRET, 'model', self.base_url, timeout=.04))
        with self.assertRaises(LLMTimeoutError):
            handler('hi')
        self.delay = 0
        handler = OpenAIChatHandler(LLMConfig(SECRET, 'model', self.base_url, max_response_bytes=10))
        with self.assertRaisesRegex(LLMResponseError, 'exceeds'):
            handler('hi')

    def test_connection_failure_is_sanitized(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), BaseHTTPRequestHandler)
        port = server.server_port
        server.server_close()
        handler = OpenAIChatHandler(LLMConfig(SECRET, 'model', f'http://127.0.0.1:{port}/v1', timeout=.1))
        with self.assertRaises(LLMError) as caught:
            handler('hi')
        self.assertNotIn(SECRET, str(caught.exception))


class LLMDAGTests(HTTPFixture):
    def test_output_contract_failure_preserves_paid_response_and_skips_business(self):
        self.registry[LLM_OUTPUT_HANDLER] = validate_llm_output
        value = completion('42 ')
        value['choices'][0]['finish_reason'] = 'length'
        self.replies.append((200, value, {}))
        engine = self.engine()
        orchestrator = WorkflowOrchestrator(engine)
        flow = DAGWorkflow((
            DAGNode('model', LLM_HANDLER, kwargs={'prompt': 'Return TRACE:42'}),
            DAGNode('contract', LLM_OUTPUT_HANDLER, kwargs={'expected_text': 'TRACE:42'},
                    inputs={'result': OutputRef('model')}),
            DAGNode('business', LLM_HANDLER, kwargs={'prompt': 'must not execute'}, depends_on=('contract',)),
        ))
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual(run.nodes['model'].status, NodeStatus.SUCCESS)
        saved = engine.get_task(run.nodes['model'].task_id)
        self.assertEqual(saved.result['text'], '42 ')
        self.assertEqual(saved.result['usage']['total_tokens'], 18)
        self.assertEqual(run.nodes['contract'].status, NodeStatus.FAILED)
        self.assertIn('LLMOutputError', run.nodes['contract'].error)
        self.assertNotIn('TRACE:42', run.nodes['contract'].error)
        self.assertEqual(run.nodes['business'].status, NodeStatus.SKIPPED)
        self.assertEqual(len(self.requests), 1)
        reopened = WorkflowOrchestrator(self.engine())
        self.assertEqual(reopened.run(flow.id, timeout=1), run)
        self.assertEqual(engine.get_events(saved.id)[-1].details['result'], saved.result)
        self.assertEqual(len(self.requests), 1)

    def test_remote_worker_runs_output_contract_before_downstream_model(self):
        self.registry[LLM_OUTPUT_HANDLER] = validate_llm_output
        engine = self.engine()
        service = engine.create_recovery_service('contract-rpc-token').start()
        self.addCleanup(service.shutdown)
        worker = RemoteWorker(service.url, create_llm_registry(self.config), 'contract-rpc-token',
                              'contract-remote', poll_interval=.01, heartbeat_interval=.1)
        self.addCleanup(worker.shutdown, 5)
        worker.start()
        flow = DAGWorkflow((DAGNode('draft', LLM_HANDLER, kwargs={'prompt': 'hello'}),
            DAGNode('contract', LLM_OUTPUT_HANDLER, kwargs={'expected_text': '模型回答'},
                    inputs={'result': OutputRef('draft')}),
            DAGNode('review', LLM_HANDLER, inputs={'prompt': OutputRef('contract', ('text',))})))
        orchestrator = WorkflowOrchestrator(engine)
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, execute=False, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual(run.nodes['contract'].result, run.nodes['draft'].result)
        self.assertEqual(self.requests[1]['body']['messages'][-1]['content'], '模型回答')
        self.assertEqual(len(self.requests), 2)

    def test_cli_uses_app_settings_without_validation_script_translation(self):
        env = {key: value for key, value in os.environ.items() if not key.startswith('LLM_')}
        env.update(PYTHONPATH=str(ROOT / 'src'), PYTHONIOENCODING='utf-8', APP_LLM_API_KEY=SECRET,
                   APP_LLM_MODEL='app-cli-model', APP_LLM_BASE_URL=self.base_url + '/chat/completions')
        result = subprocess.run([sys.executable, '-m', 'workflow_engine.llm_cli',
                                 '--database', str(self.path), '--prompt', 'hello'],
                                env=env, capture_output=True, text=True, encoding='utf-8', timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value['status'], 'SUCCESS')
        self.assertEqual(value['nodes']['generate']['result']['requested_model'], 'app-cli-model')
        self.assertEqual(self.requests[0]['path'], '/custom/v1/chat/completions')

    def workflow(self):
        return DAGWorkflow((
            DAGNode('draft', LLM_HANDLER, kwargs={'prompt': '起草一个计划'}),
            DAGNode('review', LLM_HANDLER, kwargs={'system_prompt': '审查以下计划', 'model': 'review-model'},
                    inputs={'prompt': OutputRef('draft', ('text',))}),
        ), id='real-llm-dag')

    def test_dag_persists_answers_usage_and_events_survives_restart(self):
        engine = self.engine()
        orchestrator = WorkflowOrchestrator(engine)
        flow = self.workflow()
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual(self.requests[1]['body']['messages'][-1]['content'], '模型回答')
        self.assertEqual(self.requests[1]['body']['model'], 'review-model')
        for node in run.nodes.values():
            stored = engine.get_task(node.task_id)
            self.assertEqual(stored.result, node.result)
            self.assertEqual(stored.result['usage']['total_tokens'], 18)
            event = engine.get_events(node.task_id)[-1]
            self.assertEqual(event.event_type, TaskEventType.SUCCEEDED)
            self.assertEqual(event.details['result'], node.result)
        restarted = WorkflowOrchestrator(self.engine())
        self.assertEqual(restarted.run(flow.id, timeout=1), run)
        self.assertEqual(len(self.requests), 2)
        with closing(sqlite3.connect(self.path)) as db:
            dump = '\n'.join(db.iterdump())
        self.assertNotIn(SECRET, dump)
        self.assertNotIn(self.base_url, dump)

    def test_engine_retry_budget_controls_network_calls(self):
        self.replies.append((429, {'error': SECRET}, {}))
        orchestrator = WorkflowOrchestrator(self.engine(retry_policy=RetryPolicy(1, initial_delay=.01)))
        flow = DAGWorkflow((DAGNode('llm', LLM_HANDLER, kwargs={'prompt': 'hi'}),))
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.SUCCESS)
        self.assertEqual(run.nodes['llm'].attempt_count, 2)
        self.assertEqual(run.nodes['llm'].retry_count, 1)
        self.assertEqual(len(self.requests), 2)
        history = orchestrator.engine.get_events(run.nodes['llm'].task_id)
        failed = next(event for event in history if event.event_type is TaskEventType.FAILED)
        self.assertNotIn(SECRET, str(failed.details))
        self.assertIn('429', str(failed.details))

    def test_failed_api_call_skips_downstream_and_persists_safe_error(self):
        self.replies.append((401, {'error': SECRET}, {}))
        orchestrator = WorkflowOrchestrator(self.engine())
        flow = self.workflow()
        orchestrator.submit(flow)
        run = orchestrator.run(flow.id, timeout=5)
        self.assertEqual(run.status, WorkflowStatus.FAILED)
        self.assertEqual(run.nodes['draft'].status, NodeStatus.FAILED)
        self.assertEqual(run.nodes['review'].status, NodeStatus.SKIPPED)
        self.assertIn('401', run.nodes['draft'].error)
        self.assertNotIn(SECRET, run.nodes['draft'].error)
        self.assertEqual(len(self.requests), 1)

    def test_remote_worker_returns_llm_result_to_central_sqlite(self):
        engine = self.engine()
        service = engine.create_recovery_service('test-rpc-token').start()
        self.addCleanup(service.shutdown)
        worker = RemoteWorker(service.url, create_llm_registry(self.config), 'test-rpc-token',
                              'llm-remote', poll_interval=.01, heartbeat_interval=.1)
        self.addCleanup(worker.shutdown, 5)
        worker.start()
        orchestrator = WorkflowOrchestrator(engine)
        flow = self.workflow()
        orchestrator.submit(flow)
        result = orchestrator.run(flow.id, execute=False, timeout=5)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['review'].result['usage']['total_tokens'], 18)
        self.assertEqual(len(self.requests), 2)
        self.assertIsNone(worker.last_error)

    def test_management_api_exposes_saved_answer_and_usage(self):
        engine = self.engine()
        service = engine.create_management_service('test-admin').start()
        self.addCleanup(service.shutdown)
        opener = build_opener(ProxyHandler({}))
        flow = self.workflow()
        request = Request(service.url + '/api/v1/workflows', data=json.dumps(flow.to_dict()).encode(),
                          headers={'Authorization': 'Bearer test-admin', 'Content-Type': 'application/json'})
        with opener.open(request, timeout=3) as response:
            self.assertEqual(response.status, 202)
        WorkflowOrchestrator(engine).run(flow.id, timeout=5)
        request = Request(service.url + '/api/v1/workflows/' + flow.id,
                          headers={'Authorization': 'Bearer test-admin'})
        with opener.open(request, timeout=3) as response:
            value = json.load(response)
        self.assertEqual(value['status'], 'SUCCESS')
        for node in value['nodes']:
            self.assertEqual(node['result']['text'], '模型回答')
            self.assertEqual(node['result']['usage']['total_tokens'], 18)
        self.assertNotIn(SECRET, json.dumps(value))

    def test_cli_runs_json_dag_and_reuses_saved_result(self):
        definition = self.path.with_suffix('.json')
        definition.write_text(json.dumps(self.workflow().to_dict()), encoding='utf-8')
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'), PYTHONIOENCODING='utf-8',
                   LLM_API_KEY=SECRET, LLM_BASE_URL=self.base_url, LLM_MODEL='cli-model')
        command = [sys.executable, '-m', 'workflow_engine.llm_cli', '--database', str(self.path),
                   '--workflow', str(definition)]
        for _ in range(2):
            result = subprocess.run(command, env=env, capture_output=True, text=True, encoding='utf-8', timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            value = json.loads(result.stdout)
            self.assertEqual(value['status'], 'SUCCESS')
            self.assertEqual(value['nodes']['draft']['result']['requested_model'], 'cli-model')
            self.assertEqual(value['nodes']['review']['result']['usage']['total_tokens'], 18)
        self.assertEqual(len(self.requests), 2)


class LLMOutputContractTests(unittest.TestCase):
    def test_exact_and_pattern_contracts_preserve_result_metadata(self):
        result = dict(text='TRACE:42', finish_reason='stop', usage={'total_tokens': 18}, response_id='provider-id')
        self.assertEqual(validate_llm_output(result, expected_text='TRACE:42', pattern=r'TRACE:\d+'), result)

    def test_empty_truncated_and_mismatched_answers_fail_with_safe_messages(self):
        for result, kwargs in ((dict(text='private-answer', finish_reason='stop'), {'expected_text': SECRET}),
                               (dict(text='private-answer', finish_reason='stop'), {'pattern': r'\d+'}),
                               (dict(text='42', finish_reason='length'), {}),
                               (dict(text='42'), {}), (dict(text='  ', finish_reason='stop'), {})):
            with self.subTest(result=result), self.assertRaises(LLMOutputError) as caught:
                validate_llm_output(result, **kwargs)
            self.assertNotIn('private-answer', str(caught.exception))
            self.assertNotIn(SECRET, str(caught.exception))
        # No silent stripping or repair; contracts must opt into whitespace.
        with self.assertRaises(LLMOutputError):
            validate_llm_output(dict(text='42\n', finish_reason='stop'), pattern=r'\d+')
        self.assertEqual(validate_llm_output(dict(text='42', finish_reason='length'), require_stop=False)['text'], '42')

    def test_invalid_contracts_fail_without_echoing_pattern(self):
        for kwargs in ({'pattern': '[' + SECRET}, {'pattern': 4},
                       {'expected_text': 42}, {'require_stop': 'yes'}):
            with self.subTest(keys=list(kwargs)), self.assertRaises(ValueError) as caught:
                validate_llm_output(dict(text='42', finish_reason='stop'), **kwargs)
            self.assertNotIn(SECRET, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
