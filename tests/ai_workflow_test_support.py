"""Socket-level model and HTTP service fixture for independent integration suites."""
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
from threading import Lock, Thread
import unittest

from workflow_engine import (
    DAGNode, DAGWorkflow, HTTPConfig, HTTP_HANDLER, InputBinding, LLMBudget,
    LLMConfig, LLM_HANDLER, LLM_JSON_HANDLER, LLM_OUTPUT_HANDLER, OutputRef,
    TOOL_HANDLER, create_workflow_registry,
)


ROOT = Path(__file__).resolve().parents[1]
MODEL_SECRET = 'integration-model-secret'
HTTP_SECRET = 'integration-service-secret'


class MixedWorkflowFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=ROOT, prefix='.mixed-workflow-')
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.database = self.directory / 'workflow.db'
        self.calls = []
        self.effects = []
        self.model_replies = deque()
        self.read_statuses = deque()
        self.action_status = 200
        self.lock = Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                body = json.loads(raw) if raw else None
                with fixture.lock:
                    fixture.calls.append(dict(path=self.path, method=self.command, body=body,
                                              authorization=self.headers.get('Authorization')))
                    if self.path == '/v1/chat/completions':
                        if fixture.model_replies:
                            status, text, finish = fixture.model_replies.popleft()
                        elif body['messages'][-1]['content'] == 'Explain item-7':
                            status, text, finish = 200, json.dumps(dict(name=HTTP_HANDLER, arguments=dict(
                                url=fixture.origin + '/actions', method='POST', json_body={'id': 'item-7'}))), 'stop'
                        else:
                            status, text, finish = 200, 'Action completed for item-7', 'stop'
                        payload = (dict(error={'code': 'busy'}) if status != 200 else dict(
                            id='fixture-answer', model='fixture-model',
                            choices=[dict(message=dict(role='assistant', content=text), finish_reason=finish)],
                            usage=dict(prompt_tokens=5, completion_tokens=3, total_tokens=8)))
                    elif self.path == '/context':
                        status = fixture.read_statuses.popleft() if fixture.read_statuses else 200
                        payload = dict(prompt='Explain item-7')
                    elif self.path == '/actions':
                        fixture.effects.append(body)
                        status = fixture.action_status
                        payload = dict(summary='Committed item-7', body=body)
                    else:
                        status, payload = 404, {}
                data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = handle_request

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        self.thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)
        self.llm_config = LLMConfig(MODEL_SECRET, 'fixture-model', self.origin + '/v1', timeout=2)
        self.http_config = HTTPConfig((self.origin,), timeout=2,
                                      default_headers={'Authorization': 'Bearer ' + HTTP_SECRET})
        self.registry = create_workflow_registry(self.llm_config, http_config=self.http_config)

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def workflow(self, *, max_calls=2):
        return DAGWorkflow((
            DAGNode('fetch', HTTP_HANDLER, kwargs={'url': self.origin + '/context'}),
            DAGNode('plan', LLM_HANDLER,
                    kwargs={'messages': [{'role': 'system', 'content': 'Return only JSON with name and arguments.'},
                                         {'role': 'user', 'content': ''}]},
                    bindings=(InputBinding(('messages', 1, 'content'), OutputRef('fetch', ('json', 'prompt'))),)),
            DAGNode('parse', LLM_JSON_HANDLER, kwargs={'required_keys': ['name', 'arguments']},
                    inputs={'result': OutputRef('plan')}),
            DAGNode('execute', TOOL_HANDLER, inputs={'tool_name': OutputRef('parse', ('name',)),
                                                    'arguments': OutputRef('parse', ('arguments',))}),
            DAGNode('summarize', LLM_HANDLER, inputs={'prompt': OutputRef('execute', ('json', 'summary'))}),
            DAGNode('check', LLM_OUTPUT_HANDLER, kwargs={'expected_text': 'Action completed for item-7'},
                    inputs={'result': OutputRef('summarize')}),
        ), name='http-model-tool-model', llm_budget=LLMBudget(max_calls=max_calls))
