"""Local external-service fixture, shared without importing another test suite."""
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Lock, Thread
import time


class ToolService:
    def __init__(self):
        self.calls = []
        self.effects = []
        self.replies = deque()
        self.delay = 0
        self.lock = Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                try:
                    body = json.loads(raw) if raw else None
                except ValueError:
                    body = raw.decode('utf-8')
                with fixture.lock:
                    fixture.calls.append(dict(method=self.command, path=self.path, body=body,
                                              headers=dict(self.headers)))
                    if self.path == '/actions' and self.command == 'POST':
                        fixture.effects.append(body)
                    if fixture.replies:
                        status, payload, headers = fixture.replies.popleft()
                    elif self.path == '/items':
                        status, payload, headers = 200, {'items': [{'id': 'item-7', 'label': '资料'}]}, {}
                    else:
                        status, payload, headers = 200, dict(method=self.command, path=self.path, body=body), {}
                time.sleep(fixture.delay)
                data = payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False).encode('utf-8')
                try:
                    self.send_response(status)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    if not any(key.lower() == 'content-length' for key in headers):
                        self.send_header('Content-Length', str(len(data)))
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.end_headers()
                    if self.command != 'HEAD':
                        self.wfile.write(data)
                except OSError:
                    pass

            do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = handle_request

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


def local_service(test):
    service = ToolService()
    test.addCleanup(service.close)
    return service
