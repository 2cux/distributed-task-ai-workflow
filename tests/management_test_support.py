"""Real HTTP/SQLite fixtures shared by the independently runnable API suites."""
from pathlib import Path
import json
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from workflow_engine import RetryPolicy, TaskEngine


ADMIN = 'management-tests-admin'
READER = 'management-tests-reader'


class ManagementTestCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.management-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'engine.db'
        self.services = []

    def start_service(self, registry=None, **options):
        self.engine = TaskEngine(database_path=self.path, task_registry=registry or {'abs': abs},
                                 retry_policy=RetryPolicy(0))
        self.service = self.engine.create_management_service(ADMIN, readonly_token=READER,
                         poll_interval=.02, **options).start()
        self.services.append(self.service)
        self.addCleanup(self.service.shutdown, 5)
        self.wait_for(lambda: self.call('/health/ready', token=None)[0] == 200)
        return self.service

    def call(self, path, body=None, *, token=ADMIN, method=None, headers=None, raw=None, timeout=5):
        data = raw if raw is not None else json.dumps(body).encode('utf-8') if body is not None else None
        request_headers = {'Content-Type': 'application/json'}
        if token is not None:
            request_headers['Authorization'] = 'Bearer ' + token
        request_headers.update(headers or {})
        request = Request(self.service.url + path, data=data, headers=request_headers,
                          method=method or ('POST' if data is not None else 'GET'))
        opener = build_opener(ProxyHandler({}))
        try:
            response = opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        with response:
            payload = response.read().decode('utf-8')
            return response.code, (json.loads(payload) if response.headers.get_content_type() == 'application/json'
                                   else payload), dict(response.headers)

    @staticmethod
    def workflow(key='run', value=-7):
        return dict(id=key, name='external-run', nodes=[dict(id='root', handler='abs', args=[value], idempotent=True)])

    def submit(self, key='run', value=-7):
        status, payload, _ = self.call('/api/v1/workflows', self.workflow(key, value))
        self.assertEqual(status, 202, payload)
        return payload

    def wait_for(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('condition did not become true before deadline')
            time.sleep(.01)

    def worker(self, key='api-worker'):
        worker = self.engine.create_worker(key, poll_interval=.005, heartbeat_interval=.05)
        self.addCleanup(worker.shutdown, 5)
        worker.start()
        return worker

