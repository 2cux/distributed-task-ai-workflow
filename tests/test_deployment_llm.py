"""Built-in model handler runs in a CLI worker; center needs no provider secret."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from deployment_test_support import DeploymentTestCase


class LLMProcessDeploymentTests(DeploymentTestCase):
    handlers = ['llm.chat.v1', 'llm.validate.v1']

    def test_model_http_call_validation_and_usage_persist_across_remote_cli_process(self):
        requests = []

        class Provider(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                raw = json.dumps(dict(id='test-completion', model='test-model', choices=[
                    dict(message=dict(role='assistant', content='42'), finish_reason='stop')],
                    usage=dict(prompt_tokens=5, completion_tokens=1, total_tokens=6))).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        server = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
        thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        worker = self.spawn('worker', '--server', self.rpc_url,
            '--registry', 'workflow_engine.llm:create_llm_registry', '--worker-id', 'model-host',
            env=dict(self.env, LLM_API_KEY='local-fixture-only', LLM_MODEL='test-model',
                     LLM_BASE_URL=f'http://127.0.0.1:{server.server_port}/v1'))
        self.ready(worker, 'worker_ready')
        self.request('/api/v1/workflows', dict(id='model-flow', llm_budget={'max_calls': 1}, nodes=[
            dict(id='generate', handler='llm.chat.v1', kwargs={'prompt': '19+23'}),
            dict(id='check', handler='llm.validate.v1', kwargs={'expected_text': '42'},
                 inputs={'result': {'node_id': 'generate'}})]))
        self.wait(lambda: self.request('/api/v1/workflows/model-flow')['status'] == 'SUCCESS')
        run = self.request('/api/v1/workflows/model-flow')
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['messages'][-1]['content'], '19+23')
        self.assertEqual(run['nodes'][-1]['result']['text'], '42')
        self.assertEqual(run['llm_usage']['call_count'], 1)
        self.assertEqual(run['llm_usage']['total_tokens'], 6)
        self.assertEqual(run['llm_budget']['remaining_calls'], 0)
        self.assertNotIn('local-fixture-only', json.dumps(run))
        self.assertFalse(list(worker.output_path.parent.glob('*.db')))
