"""Versioned HTTP management API over the existing durable control plane.

The standard-library server is intended for trusted deployments behind a TLS
reverse proxy. A bounded number of handlers prevents unbounded thread growth.
Business execution remains in explicitly provisioned local or remote workers.
"""
from __future__ import annotations

import argparse
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import ipaddress
import json
import logging
import os
import re
import socket
import sqlite3
import ssl
from threading import BoundedSemaphore, Condition, Event, RLock, Thread
import time
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import uuid4

from .dag import DAGWorkflow
from .management_store import ManagementQueries
from .observability import ServiceMetrics
from .persistence import IdempotencyConflict, PersistenceError, _json
from .recovery import RecoveryConflict, RecoverySystem
from .reliable_worker import _join_timeout
from .remote import _token
from .taskengine import TaskEngine
from .worker_registry import WorkerRegistry, WorkerRegistryError


logger = logging.getLogger('workflow_engine.management')
PREFIX = '/api/v1'
STATES = {'workflows': {'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'PAUSED'},
          'tasks': {'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING'},
          'workers': {'ACTIVE', 'STOPPED'},
          'sessions': {'ACTIVE', 'STOPPING', 'SUSPECT', 'FENCED', 'STOPPED'},
          'incidents': {'OPEN', 'RESOLVED'}}


class APIError(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


def _object(value, allowed, required=()):
    if type(value) is not dict or value.keys() - set(allowed) or set(required) - value.keys():
        raise APIError(400, 'invalid_request', 'object has missing or unsupported fields')


def _workflow(value, registry):
    _object(value, ('id', 'name', 'nodes', 'llm_budget'), ('id', 'nodes'))
    if 'llm_budget' in value:
        _object(value['llm_budget'], ('max_calls', 'max_total_tokens'))
    for field, bound in (('id', 256), ('name', 512)):
        if field in value and (type(value[field]) is not str or not 1 <= len(value[field]) <= bound):
            raise APIError(400, 'invalid_workflow', field + ' is empty or exceeds the character limit')
    if type(value['nodes']) is not list or not 1 <= len(value['nodes']) <= 1000:
        raise APIError(400, 'invalid_workflow', 'nodes must contain 1..1000 entries')
    for node in value['nodes']:
        _object(node, ('id', 'handler', 'depends_on', 'args', 'kwargs', 'inputs',
                       'priority', 'max_retries', 'timeout', 'idempotent', 'scheduled_at'), ('id', 'handler'))
        if type(node['handler']) is not str or node['handler'] not in registry:
            raise APIError(400, 'unknown_handler', 'handler is not registered')
        for field in ('args', 'depends_on'):
            if field in node and type(node[field]) is not list:
                raise APIError(400, 'invalid_workflow', field + ' must be an array')
        for field in ('kwargs', 'inputs'):
            if field in node and type(node[field]) is not dict:
                raise APIError(400, 'invalid_workflow', field + ' must be an object')
        for reference in node.get('inputs', {}).values():
            _object(reference, ('node_id', 'path'), ('node_id',))
            if 'path' in reference and type(reference['path']) is not list:
                raise APIError(400, 'invalid_workflow', 'output path must be an array')
    return DAGWorkflow.from_dict(value)


def _json_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON field')
        value[key] = item
    return value


def _reject_constant(value):
    raise ValueError('nonfinite JSON')


def _drain_rejected_connection(request):
    # An unread upload can cause TCP reset on Windows and hide the error response.
    # Let the client read EOF first, then discard bytes for at most 100 ms.
    try:
        request.shutdown(socket.SHUT_WR)
        deadline = time.monotonic() + .1
        while time.monotonic() < deadline:
            request.settimeout(max(.001, deadline - time.monotonic()))
            if not request.recv(65536):
                break
    except OSError:
        pass


class _BoundedServer(ThreadingHTTPServer):
    daemon_threads = True
    # Windows SO_REUSEADDR can permit another process to steal the port.
    allow_reuse_address = False

    def __init__(self, address, handler, service):
        self.service = service
        self._slots = BoundedSemaphore(service.max_concurrency)
        self._drain = Condition()
        self._active = 0
        # TCPServer's default backlog of five causes SYN retransmission tails
        # even when the handler limit has room for ordinary concurrent clients.
        self.request_queue_size = max(16, service.max_concurrency * 2)
        super().__init__(address, handler)

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            self.service.metrics.overloaded()
            try:
                request.settimeout(.1)
                request_id = uuid4().hex
                raw = _json(dict(error=dict(code='overloaded', message='request capacity exhausted'),
                                 request_id=request_id)).encode('utf-8')
                request.sendall(b'HTTP/1.0 503 Service Unavailable\r\nContent-Type: application/json\r\n'
                                b'Connection: close\r\nRetry-After: 1\r\nX-Request-ID: ' + request_id.encode() +
                                b'\r\nContent-Length: ' +
                                str(len(raw)).encode() + b'\r\n\r\n' + raw)
                _drain_rejected_connection(request)
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return
        with self._drain:
            self._active += 1
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._release()
            raise

    def _release(self):
        self._slots.release()
        with self._drain:
            self._active -= 1
            self._drain.notify_all()

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._release()

    def handle_error(self, request, client_address):
        logger.error('{"event":"handler_failed"}')

    def drain(self, deadline):
        with self._drain:
            while self._active:
                remaining = max(0, deadline - time.monotonic())
                if not remaining:
                    return False
                self._drain.wait(remaining)
        return True


class ManagementService:
    """REST control plane with admin/read-only credentials and durable read models.

    start() owns one recovery/reconciliation loop, with the same trusted-stop
    rules as RecoverySystem. shutdown() drains HTTP handlers and that loop, and
    never stops workers owned by the application. No request executes business code.
    """

    def __init__(self, engine: TaskEngine, token: str, *, readonly_token=None,
                 host='127.0.0.1', port=0, ssl_context=None, allow_insecure=False,
                 request_timeout=2, max_request_bytes=1024 * 1024,
                 max_response_bytes=4 * 1024 * 1024, max_concurrency=32,
                 poll_interval=.1, coordinator_stale_after=10, recovery=None):
        if not isinstance(engine, TaskEngine) or not engine.persistent:
            raise ValueError('management service requires a persistent TaskEngine')
        self.engine, self.store = engine, engine._store
        self._token = _token(token)
        self._readonly_token = _token(readonly_token) if readonly_token is not None else None
        if self._readonly_token == self._token:
            raise ValueError('admin and read-only tokens must differ')
        WorkerRegistry._seconds(request_timeout, 'request_timeout')
        WorkerRegistry._seconds(poll_interval, 'poll_interval')
        WorkerRegistry._seconds(coordinator_stale_after, 'coordinator_stale_after')
        if coordinator_stale_after <= poll_interval:
            raise ValueError('coordinator_stale_after must exceed poll_interval')
        for name, value in (('max_request_bytes', max_request_bytes), ('max_response_bytes', max_response_bytes)):
            if type(value) is not int or value < 1024:
                raise ValueError(name + ' must be an integer >= 1024')
        if type(max_concurrency) is not int or not 1 <= max_concurrency <= 256:
            raise ValueError('max_concurrency must be in 1..256')
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError('port must be in 0..65535')
        if type(allow_insecure) is not bool:
            raise TypeError('allow_insecure must be bool')
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host == 'localhost'
        if not loopback and ssl_context is None and not allow_insecure:
            raise ValueError('non-loopback service requires TLS or an explicitly secured tunnel')
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError('ssl_context must be SSLContext')
        if recovery is not None and (not isinstance(recovery, RecoverySystem) or recovery.store is not self.store):
            raise ValueError('recovery must use this engine store')
        self.recovery = recovery or RecoverySystem(self.store)
        self.queries = ManagementQueries(self.store, busy_timeout=request_timeout)
        self.metrics = ServiceMetrics()
        self.request_timeout = request_timeout
        self.max_request_bytes, self.max_response_bytes = max_request_bytes, max_response_bytes
        self.max_concurrency, self.poll_interval = max_concurrency, poll_interval
        self.coordinator_stale_after = coordinator_stale_after
        self._stop, self._lock = Event(), RLock()
        self._thread = self._coordinator = None
        self._coordinator_ok = False
        self._last_tick_clock = 0.0
        self._closed = False
        service = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                self.request.settimeout(service.request_timeout)
                super().setup()

            def log_message(self, format, *args):
                pass

            def do_GET(self):
                self._handle('GET')

            def do_POST(self):
                self._handle('POST')

            def do_HEAD(self):
                self._handle('HEAD')

            def do_PUT(self):
                self._handle('PUT')

            def do_DELETE(self):
                self._handle('DELETE')

            def do_PATCH(self):
                self._handle('PATCH')

            def do_OPTIONS(self):
                self._handle('OPTIONS')

            def _handle(self, method):
                started = time.perf_counter()
                supplied_id = self.headers.get('X-Request-ID', '')
                request_id = supplied_id if re.fullmatch(r'[A-Za-z0-9._-]{1,64}', supplied_id) else uuid4().hex
                route, status = 'unmatched', 500
                failure_kind = None
                try:
                    parsed = urlsplit(self.path)
                    if parsed.fragment or parsed.scheme or parsed.netloc or len(parsed.path) > 4096:
                        raise APIError(400, 'invalid_request', 'invalid request target')
                    route = service.route_template(parsed.path)
                    role = service.authenticate(self.headers.get('Authorization', ''))
                    public = parsed.path in ('/health/live', '/health/ready') and method == 'GET'
                    if not public and role is None:
                        raise APIError(401, 'unauthorized', 'authentication required')
                    if method not in ('GET', 'POST'):
                        raise APIError(405, 'method_not_allowed', 'supported methods are GET and POST')
                    if method == 'POST' and role != 'admin':
                        raise APIError(403, 'forbidden', 'admin credential required')
                    query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=12,
                                     encoding='utf-8', errors='strict')
                    if any(len(values) != 1 for values in query.values()):
                        raise APIError(400, 'invalid_query', 'query fields cannot repeat')
                    body = self._body() if method == 'POST' else None
                    with service.store.connection_timeout(service.request_timeout):
                        status, payload, content_type = service.dispatch(method, parsed.path,
                            {key: values[0] for key, values in query.items()}, body)
                    raw = payload.encode('utf-8') if content_type.startswith('text/') else _json(payload).encode('utf-8')
                    if len(raw) > service.max_response_bytes:
                        raise APIError(413, 'response_too_large', 'response exceeds size limit; reduce page size')
                except APIError as error:
                    status, raw, content_type = self._error(error.status, error.code, error.message, request_id)
                except KeyError:
                    status, raw, content_type = self._error(404, 'not_found', 'resource not found', request_id)
                except (IdempotencyConflict, RecoveryConflict, WorkerRegistryError):
                    status, raw, content_type = self._error(409, 'conflict', 'definition or recovery state conflicts', request_id)
                except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
                    status, raw, content_type = self._error(400, 'invalid_request', 'invalid JSON or workflow definition', request_id)
                except sqlite3.Error as error:
                    code = getattr(error, 'sqlite_errorcode', 0)
                    busy = (code & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                    failure_kind = 'database_busy' if busy else 'database_error'
                    if busy:
                        service.metrics.database_busy()
                    status, raw, content_type = self._error(503, 'unavailable', 'control plane temporarily unavailable', request_id)
                except (PersistenceError, TimeoutError):
                    status, raw, content_type = self._error(503, 'unavailable', 'control plane temporarily unavailable', request_id)
                except Exception:
                    status, raw, content_type = self._error(500, 'internal_error', 'request failed', request_id)
                try:
                    self.send_response(status)
                    self.send_header('Content-Type', content_type)
                    self.send_header('Content-Length', str(len(raw)))
                    self.send_header('X-Request-ID', request_id)
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Connection', 'close')
                    if status == 401:
                        self.send_header('WWW-Authenticate', 'Bearer')
                    if status == 503:
                        self.send_header('Retry-After', '1')
                    if status == 405:
                        self.send_header('Allow', 'GET, POST')
                    self.end_headers()
                    if method != 'HEAD':
                        self.wfile.write(raw)
                except OSError:
                    pass  # The durable write may have committed; never replay it here.
                finally:
                    self.close_connection = True
                    if status >= 400:
                        _drain_rejected_connection(self.request)
                    elapsed = time.perf_counter() - started
                    service.metrics.observe(method, route, status, elapsed)
                    logger.info(json.dumps(dict(event='http_request', request_id=request_id,
                        method=method, route=route, status=status, failure_kind=failure_kind,
                        duration_seconds=round(elapsed, 6))))

            @staticmethod
            def _error(status, code, message, request_id):
                return status, _json(dict(error=dict(code=code, message=message),
                                         request_id=request_id)).encode('utf-8'), 'application/json; charset=utf-8'

            def _body(self):
                if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                    raise APIError(400, 'invalid_request', 'one Content-Length is required; chunked bodies are unsupported')
                length = self.headers.get('Content-Length')
                if not re.fullmatch(r'[0-9]+', length):
                    raise APIError(400, 'invalid_request', 'invalid Content-Length')
                if not 0 < int(length) <= service.max_request_bytes:
                    raise APIError(413, 'request_too_large', 'body exceeds size limit or is empty')
                if self.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json':
                    raise APIError(415, 'unsupported_media_type', 'Content-Type must be application/json')
                raw = self.rfile.read(int(length))
                if len(raw) != int(length):
                    raise APIError(400, 'invalid_request', 'incomplete request body')
                value = json.loads(raw.decode('utf-8'), object_pairs_hook=_json_object,
                                   parse_constant=_reject_constant)
                _object(value, value.keys() if type(value) is dict else ())
                try:
                    _json(value)
                except PersistenceError:
                    raise APIError(400, 'invalid_request', 'body must contain finite JSON values') from None
                return value

        self._server = _BoundedServer((host, port), Handler, self)
        if ssl_context:
            # TLS negotiation happens in the bounded handler with its socket timeout.
            self._server.socket = ssl_context.wrap_socket(self._server.socket, server_side=True,
                                                         do_handshake_on_connect=False)
        self._scheme = 'https' if ssl_context else 'http'

    @property
    def address(self):
        return self._server.server_address

    @property
    def url(self):
        host, port = self.address[:2]
        return f'{self._scheme}://{host}:{port}'

    def authenticate(self, authorization):
        supplied = authorization.encode('utf-8')
        if hmac.compare_digest(supplied, ('Bearer ' + self._token).encode('utf-8')):
            return 'admin'
        if self._readonly_token and hmac.compare_digest(supplied, ('Bearer ' + self._readonly_token).encode('utf-8')):
            return 'reader'
        return None

    @staticmethod
    def route_template(path):
        parts = path.split('/')
        if path in ('/health/live', '/health/ready', '/metrics', '/openapi.json', PREFIX + '/overview', PREFIX + '/handlers', PREFIX + '/audit'):
            return path
        if len(parts) in (4, 5, 6) and parts[:3] == ['', 'api', 'v1'] and parts[3] in STATES:
            template = PREFIX + '/' + parts[3]
            if len(parts) >= 5:
                template += '/{id}'
            if len(parts) == 6:
                if parts[5] not in ('events', 'pause', 'resume', 'stop', 'resolve'):
                    return 'unmatched'
                template += '/' + parts[5]
            return template
        return 'unmatched'

    @staticmethod
    def pagination(query, *, allowed=()):
        if query.keys() - {'cursor', 'limit', *allowed}:
            raise APIError(400, 'invalid_query', 'unsupported query field')
        for name, default in (('cursor', '0'), ('limit', '50')):
            if not re.fullmatch(r'[0-9]{1,19}', query.get(name, default)):
                raise APIError(400, 'invalid_query', name + ' must be an integer')
        cursor, limit = int(query.get('cursor', '0')), int(query.get('limit', '50'))
        if cursor > 9223372036854775807 or not 1 <= limit <= 100:
            raise APIError(400, 'invalid_query', 'cursor out of range or limit not in 1..100')
        return dict(cursor=cursor, limit=limit)

    def dispatch(self, method, path, query, body):
        content_type = 'application/json; charset=utf-8'
        if method == 'GET' and path in ('/health/live', '/health/ready'):
            if query:
                raise APIError(400, 'invalid_query', 'health takes no query fields')
            ready = not self._stop.is_set()
            if path.endswith('/ready'):
                try:
                    self.queries.ready()
                    ready = ready and self._coordinator_ok and self._coordinator is not None and self._coordinator.is_alive()
                    ready = ready and time.monotonic() - self._last_tick_clock <= self.coordinator_stale_after
                except (sqlite3.Error, OSError):
                    ready = False
            return (200 if ready else 503), dict(status='ok' if ready else 'unavailable'), content_type
        if method == 'GET' and path in ('/metrics', '/openapi.json', PREFIX + '/overview', PREFIX + '/handlers'):
            if query:
                raise APIError(400, 'invalid_query', 'endpoint takes no query fields')
            if path == '/metrics':
                return 200, self.metrics.render(self.queries.overview()), 'text/plain; version=0.0.4; charset=utf-8'
            if path == '/openapi.json':
                from .management_schema import openapi_document
                return 200, openapi_document(), content_type
            return 200, (self.queries.overview() if path.endswith('/overview') else
                         dict(items=sorted(self.store.registry))), content_type
        if method == 'GET' and path == PREFIX + '/audit':
            return 200, self.queries.events('audit', **self.pagination(query)), content_type
        parts = path.split('/')
        if len(parts) not in (4, 5, 6) or parts[:3] != ['', 'api', 'v1'] or parts[3] not in STATES:
            raise APIError(404, 'not_found', 'endpoint not found')
        resource = parts[3]
        key = unquote(parts[4], encoding='utf-8', errors='strict') if len(parts) >= 5 else None
        if key == '':
            raise APIError(404, 'not_found', 'resource not found')
        action = parts[5] if len(parts) == 6 else None
        if method == 'GET':
            if action == 'events' and resource in ('workflows', 'tasks', 'sessions'):
                return 200, self.queries.events(resource, key, **self.pagination(query)), content_type
            if action is None and key is None:
                page = self.pagination(query, allowed=('status', 'workflow_id') if resource == 'tasks' else ('status',))
                status = query.get('status')
                if status is not None and status not in STATES[resource]:
                    raise APIError(400, 'invalid_query', 'unsupported status')
                if 'workflow_id' in query and not query['workflow_id']:
                    raise APIError(400, 'invalid_query', 'workflow_id cannot be empty')
                return 200, self.queries.list(resource, **page, status=status, workflow_id=query.get('workflow_id')), content_type
            if action is None:
                if query:
                    raise APIError(400, 'invalid_query', 'detail takes no query fields')
                return 200, self.queries.get(resource, key), content_type
        elif query:
            raise APIError(400, 'invalid_query', 'mutations take no query fields')
        elif resource == 'workflows' and key is None:
            workflow = _workflow(body, self.store.registry)
            self.engine.submit_workflow(workflow)
            # 202 also for an identical replay; accepted does not promise completion.
            return 202, self.queries.get('workflows', workflow.id), content_type
        elif resource == 'workflows' and action in ('pause', 'resume'):
            _object(body, ('operator', 'reason'), ('operator', 'reason'))
            getattr(self.recovery, action + '_workflow')(key, **body)
            return 200, self.queries.get(resource, key), content_type
        elif resource == 'incidents' and action == 'resolve':
            _object(body, ('operator', 'reason', 'action', 'result', 'extra_retries'), ('operator', 'reason', 'action'))
            self.queries.get(resource, key)
            self.recovery.resolve_task(key, **body)
            return 200, self.queries.get(resource, key), content_type
        elif resource == 'sessions' and action == 'stop':
            _object(body, ('operator', 'reason'), ('operator', 'reason'))
            self.recovery._operator(body['operator'], body['reason'])
            with self.store._transaction() as db:
                row = db.execute('SELECT * FROM worker_sessions WHERE session_id=?', (key,)).fetchone()
                if row is None:
                    raise KeyError(key)
                if row['state'] in ('ACTIVE', 'SUSPECT'):
                    db.execute("UPDATE worker_sessions SET state='STOPPING',stop_requested=1 WHERE session_id=?", (key,))
                    self.store.workers._event(db, key, 'STOPPING', 'management API graceful stop request')
                self.recovery._event(db, 'WORKER_STOP_REQUESTED', key, body['operator'], reason=body['reason'])
            return 202, self.queries.get(resource, key), content_type
        raise APIError(405, 'method_not_allowed', 'operation is unsupported for this endpoint')

    def _coordinate(self):
        with self.store.connection_scope():
            self._coordinate_loop()

    def _coordinate_loop(self):
        while not self._stop.is_set():
            try:
                report = self.recovery.tick()
                self._coordinator_ok = not report.errors
            except Exception:
                self._coordinator_ok = False
            self._last_tick_clock = time.monotonic()
            self.metrics.tick(self._coordinator_ok)
            if not self._coordinator_ok:
                logger.error('{"event":"coordinator_failed"}')
            self._stop.wait(self.poll_interval)

    def start(self):
        with self._lock:
            if self._thread is not None or self._closed:
                raise RuntimeError('ManagementService can start only once')
            self._coordinator = Thread(target=self._coordinate, name='workflow-management-coordinator', daemon=True)
            self._thread = Thread(target=self._server.serve_forever, kwargs={'poll_interval': .05},
                                  name='workflow-management-http', daemon=True)
            self._coordinator.start()
            self._thread.start()
        return self

    def shutdown(self, timeout=5):
        _join_timeout(timeout)
        if timeout is None:
            raise ValueError('management shutdown requires a finite timeout')
        deadline = time.monotonic() + timeout
        with self._lock:
            if not self._closed:
                self._closed = True
                self._stop.set()
                if self._thread is not None:
                    self._server.shutdown()
                self._server.server_close()
        drained = self._server.drain(deadline)
        for thread in (self._thread, self._coordinator):
            if thread is not None:
                thread.join(max(0, deadline - time.monotonic()))
        return drained and all(thread is None or not thread.is_alive() for thread in (self._thread, self._coordinator))


def main():
    parser = argparse.ArgumentParser(description='Workflow management control plane (workers provisioned separately)')
    parser.add_argument('--database', required=True)
    parser.add_argument('--registry', required=True, help='trusted Python module:factory returning handler mapping')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--allow-insecure', action='store_true', help='explicitly acknowledge a protected TLS proxy/tunnel')
    options = parser.parse_args()
    token = os.environ.get('WORKFLOW_API_TOKEN')
    if not token:
        parser.error('WORKFLOW_API_TOKEN environment variable is required')
    module, separator, factory = options.registry.partition(':')
    if not separator or not module or not factory:
        parser.error('--registry must be module:factory')
    from .retry import RetryPolicy
    engine = TaskEngine(database_path=options.database,
                        task_registry=getattr(importlib.import_module(module), factory)(), retry_policy=RetryPolicy(0))
    service = ManagementService(engine, token, readonly_token=os.environ.get('WORKFLOW_API_READ_TOKEN'),
                                host=options.host, port=options.port, allow_insecure=options.allow_insecure)
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    service.start()
    logger.info(json.dumps(dict(event='service_started', url=service.url)))
    try:
        while True:
            time.sleep(.5)
    except KeyboardInterrupt:
        return 0 if service.shutdown() else 1


if __name__ == '__main__':
    raise SystemExit(main())
