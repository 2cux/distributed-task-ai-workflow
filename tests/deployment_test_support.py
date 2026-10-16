"""Real central/worker CLI processes and a switchable TCP link; no mocked RPC."""
import json
import os
from pathlib import Path
import select
import socket
import socketserver
import subprocess
import sys
import tempfile
from threading import Event, Thread
import time
import unittest
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler

ROOT = Path(__file__).resolve().parents[1]
HANDLERS = ['probe.echo.v1', 'probe.summarize.v1']


class FaultLink:
    """A partition closes existing TCP flows and refuses subsequent flows.

    Unlike shutting down the endpoint, this leaves the central service healthy.
    """
    def __init__(self, target):
        self.blocked = Event()
        self.closed = Event()
        link = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                if link.blocked.is_set():
                    return
                try:
                    with socket.create_connection(target, timeout=1) as upstream:
                        peers = (self.request, upstream)
                        while not link.blocked.is_set() and not link.closed.is_set():
                            readable, _, _ = select.select(peers, (), (), .02)
                            for source in readable:
                                data = source.recv(65536)
                                if not data:
                                    return
                                destination = upstream if source is self.request else self.request
                                destination.sendall(data)
                except OSError:
                    return

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True

        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = Thread(target=self.server.serve_forever,
                             kwargs={'poll_interval': .02}, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_address[1])

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class DeploymentTestCase(unittest.TestCase):
    handlers = HANDLERS

    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=ROOT, prefix='.deployment-test-')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.database = self.directory / 'central' / 'workflow.db'
        self.database.parent.mkdir()
        self.processes = []
        self.env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'),
                        WORKFLOW_API_TOKEN='deployment-tests-admin',
                        WORKFLOW_RECOVERY_TOKEN='deployment-tests-worker')
        self.opener = build_opener(ProxyHandler({}))
        self.start_central()

    def spawn(self, role, *args, env=None):
        work = self.directory / (role + '-' + str(len(self.processes)))
        work.mkdir()
        output = work / 'process.log'
        log = output.open('w', encoding='utf-8')
        self.addCleanup(log.close)
        process = subprocess.Popen([sys.executable, '-m', 'workflow_engine.deployment', role, *args],
            cwd=work, env=env or self.env, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(process)
        self.addCleanup(self.kill, process)
        process.output_path = output
        return process

    @staticmethod
    def kill(process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=8)

    def wait(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.02)
        logs = '\n'.join(p.output_path.read_text(encoding='utf-8', errors='replace') for p in self.processes)
        self.fail('deployment condition timed out\n' + logs)

    def ready(self, process, event):
        def read():
            for line in process.output_path.read_text(encoding='utf-8', errors='replace').splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if value.get('event') == event:
                    return value
            if process.poll() is not None:
                self.fail(process.output_path.read_text(encoding='utf-8', errors='replace'))
        return self.wait(read)

    def start_central(self):
        self.central = self.spawn('central', '--database', str(self.database), '--handlers', *self.handlers,
            '--api-port', '0', '--rpc-port', '0', '--poll-interval', '.03')
        urls = self.ready(self.central, 'central_ready')
        self.api_url, self.rpc_url = urls['api_url'], urls['rpc_url']
        def api_ready():
            try:
                return self.request('/health/ready')
            except HTTPError as error:
                if error.code != 503:
                    raise
                error.close()
                return False  # The listener can start before the first recovery tick.
            except URLError:
                return False
        self.wait(api_ready)

    def link(self):
        from urllib.parse import urlparse
        parsed = urlparse(self.rpc_url)
        link = FaultLink((parsed.hostname, parsed.port))
        self.addCleanup(link.close)
        return link

    def worker(self, name='victim', *, server=None, env=None,
               registry='workflow_engine.deployment_demo:create_registry'):
        process = self.spawn('worker', '--server', server or self.rpc_url,
            '--registry', registry, '--worker-id', name,
            '--host-id', name + '-host', '--heartbeat-interval', '.05', '--heartbeat-timeout', '.6',
            '--request-timeout', '.2', '--poll-interval', '.02', env=env)
        record = self.ready(process, 'worker_ready')
        process.session_id = record['session_id']
        process.worker_id = name
        return process

    def request(self, path, body=None, token=None):
        raw = json.dumps(body).encode() if body is not None else None
        request = Request(self.api_url + path, raw, headers={'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + (token or self.env['WORKFLOW_API_TOKEN'])})
        with self.opener.open(request, timeout=3) as response:
            return json.load(response)

    def submit(self, *, idempotent=True, delay=3):
        return self.request('/api/v1/workflows', dict(id='flow', name='deployment-test', nodes=[
            dict(id='prepare', handler=HANDLERS[0], kwargs={'value': 'context'}),
            dict(id='generate', handler=HANDLERS[0], depends_on=['prepare'],
                 kwargs={'value': 'answer', 'delay': delay}, idempotent=idempotent),
            dict(id='validate', handler=HANDLERS[1],
                 inputs={'item': {'node_id': 'generate', 'path': []}})]))

    def node(self, name='generate'):
        return next(n for n in self.request('/api/v1/workflows/flow')['nodes'] if n['id'] == name)

    def running(self):
        return self.wait(lambda: (n if (n := self.node())['task_status'] == 'RUNNING' else None))

    def session(self, worker):
        return self.request('/api/v1/sessions/' + worker.session_id)

    def fenced(self, worker):
        return self.wait(lambda: self.session(worker)['state'] == 'FENCED')

    def unchanged_attempt(self):
        node = self.node()
        self.assertEqual((node['task_status'], node['attempt_count'], node['retry_count']), ('RUNNING', 1, 0))

    def attest(self, worker, host=None):
        self.assertIsNotNone(worker.poll(), 'must join actual worker process before attesting stop')
        worker.wait(timeout=1)
        result = subprocess.run([sys.executable, '-m', 'workflow_engine.deployment', 'confirm-stopped',
            '--database', str(self.database), '--session-id', worker.session_id,
            '--host-id', host or worker.worker_id + '-host', '--operator', 'test-process-supervisor',
            '--evidence', f'joined exact worker pid={worker.pid}, exitcode={worker.returncode}; no child processes'],
            env=self.env, capture_output=True, text=True, timeout=10)
        return result

    def recovered(self):
        self.wait(lambda: self.request('/api/v1/workflows/flow')['status'] == 'SUCCESS')
        self.assertEqual(self.node('prepare')['attempt_count'], 1)
        self.assertEqual((self.node()['attempt_count'], self.node()['retry_count']), (2, 1))
        self.assertEqual(self.node('validate')['result']['answer'], 'verified:answer')
        self.assertEqual(self.node()['result']['host'], self.node('validate')['result']['source_host'])
        self.assertEqual(self.request('/api/v1/incidents?status=OPEN')['items'], [])
