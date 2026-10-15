"""Authenticated JSON transport for workers on other hosts.

Only the service opens SQLite, on its local disk. Clients execute explicitly
registered handlers and never receive pickles or import instructions. Run a
RecoverySystem beside the service; network loss is not proof of process exit.
Outside loopback use TLS, or explicitly opt into a separately secured tunnel.
The service and its workers are a trusted deployment, not a tenant boundary.
"""

from __future__ import annotations

from dataclasses import asdict
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import ssl
from threading import Thread
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, build_opener, ProxyHandler, HTTPSHandler, HTTPRedirectHandler

from .events import TaskEvent, TaskEventType
from .failures import annotate_task_failure, failure_metadata, restore_failure_metadata
from .persistence import (SQLiteTaskStore, PersistenceError, StaleExecutionError,
                          StoredTaskError, _json)
from .reliable_worker import ReliableWorker, _join_timeout
from .retry import RetryPolicy
from .task import Task, TaskStatus
from .timeout import TaskTimeoutError
from .worker_registry import (WorkerRegistry, WorkerRecord, WorkerSessionRecord,
                              WorkerRegistryError, WorkerAlreadyActive,
                              WorkerSessionMismatch, WorkerNotReady, WorkerStopRequested)


class RemoteTransportError(PersistenceError):
    """A request has an unknown outcome; callers must not automatically replay it."""


class RemoteAuthenticationError(RemoteTransportError):
    """The control plane rejected the worker's credentials."""


_ERRORS = {cls.__name__: cls for cls in (PersistenceError, StaleExecutionError,
    WorkerRegistryError, WorkerAlreadyActive, WorkerSessionMismatch, WorkerNotReady, WorkerStopRequested,
    ValueError, TypeError)}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        # Never forward an Authorization header to a redirected endpoint.
        return None


def _token(value):
    if not isinstance(value, str) or not value or len(value) > 1024 or any(
            ord(char) < 33 or ord(char) > 126 for char in value):
        raise ValueError('token must be a nonempty printable ASCII secret without spaces')
    return value


def _event_payload(event):
    return dict(task_id=event.task_id, sequence=event.sequence,
        event_type=event.event_type.value, timestamp=event.timestamp,
        previous_status=event.previous_status.value if event.previous_status else None,
        status=event.status.value, attempt_count=event.attempt_count,
        retry_count=event.retry_count, details=event.details)


def _task_payload(store, task):
    if task is None:
        return None
    # Read a coherent persisted snapshot, including the execution's actual token.
    with store._connection() as db:
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM tasks WHERE id=?', (task.id,)).fetchone()
        return dict(id=row['id'], definition=json.loads(row['definition']),
            status=row['status'], attempt_count=row['attempt_count'],
            retry_count=row['retry_count'], retry_limit=row['retry_limit'],
            retry_config=json.loads(row['retry_config']), retry_at=row['retry_at'],
            worker_session=row['worker_session'],
            result=json.loads(row['result']) if row['result'] is not None else None,
            error=json.loads(row['error']) if row['error'] is not None else None,
            last_error=json.loads(row['last_error']) if row['last_error'] is not None else None,
            events=[_event_payload(e) for e in store._read_events(db, task.id)])


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # Never print request bodies, credentials, or application exception data.
        pass


class RecoveryService:
    """Central worker endpoint; storage and recovery coordination remain local.

    Non-loopback HTTP requires allow_insecure=True, which is appropriate only
    when a TLS proxy, VPN or SSH tunnel already protects the complete connection.
    ssl_context enables direct HTTPS. Authentication is required in both modes.
    """

    def __init__(self, store: SQLiteTaskStore, token: str, *, host='127.0.0.1',
                 port=0, ssl_context: ssl.SSLContext | None = None,
                 allow_insecure=False, request_timeout=2, max_request_bytes=1024 * 1024):
        if not isinstance(store, SQLiteTaskStore):
            raise TypeError('store must be SQLiteTaskStore')
        self.store, self._token = store, _token(token)
        WorkerRegistry._seconds(request_timeout, 'request_timeout')
        if type(max_request_bytes) is not int or max_request_bytes < 1024:
            raise ValueError('max_request_bytes must be an integer >= 1024')
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
        self.request_timeout = request_timeout
        self.max_request_bytes = max_request_bytes
        service = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                self.request.settimeout(service.request_timeout)
                super().setup()

            def log_message(self, format, *args):
                pass

            def do_POST(self):
                if self.path != '/rpc':
                    self._reply(404, dict(error='ValueError', message='unknown endpoint'))
                    return
                authorization = self.headers.get('Authorization', '')
                if not hmac.compare_digest(authorization.encode('utf-8'),
                        ('Bearer ' + service._token).encode('utf-8')):
                    self._reply(401, dict(error='RemoteAuthenticationError', message='authentication rejected'))
                    return
                try:
                    if self.headers.get('Transfer-Encoding'):
                        raise ValueError('chunked requests are unsupported')
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= service.max_request_bytes:
                        self._reply(413, dict(error='ValueError', message='invalid request size'))
                        return
                    raw = self.rfile.read(length)
                    if len(raw) != length:
                        raise ValueError('incomplete request')
                    payload = json.loads(raw)
                    _json(payload)
                    if type(payload) is not dict or set(payload) != {'operation', 'arguments'}:
                        raise ValueError('invalid request envelope')
                    if type(payload['arguments']) is not dict:
                        raise ValueError('arguments must be an object')
                    value = service._dispatch(payload['operation'], payload['arguments'])
                    self._reply(200, dict(value=value))
                except (ValueError, TypeError, PersistenceError, WorkerRegistryError) as error:
                    self._reply(409, dict(error=type(error).__name__, message='request rejected'))
                except Exception:
                    self._reply(503, dict(error='RemoteTransportError', message='control plane unavailable'))

            def _reply(self, status, payload):
                raw = _json(payload).encode('utf-8')
                if len(raw) > service.max_request_bytes:
                    status = 413
                    raw = b'{"error":"RemoteTransportError","message":"response exceeds size limit"}'
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(raw)
                self.close_connection = True

        self._server = _Server((host, port), Handler)
        if ssl_context is not None:
            self._server.socket = ssl_context.wrap_socket(self._server.socket, server_side=True)
        self._scheme = 'https' if ssl_context else 'http'
        self._thread = None
        self._closed = False

    @property
    def address(self):
        return self._server.server_address

    @property
    def url(self):
        host, port = self.address[:2]
        return f'{self._scheme}://{host}:{port}'

    def start(self):
        if self._thread is not None or self._closed:
            raise RuntimeError('RecoveryService can start only once')
        self._thread = Thread(target=self._server.serve_forever,
                              kwargs={'poll_interval': .05}, daemon=True,
                              name='workflow-recovery-service')
        self._thread.start()
        return self

    def shutdown(self, timeout=5):
        _join_timeout(timeout)
        if not self._closed:
            self._closed = True
            if self._thread is not None:
                self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout)
        return self._thread is None or not self._thread.is_alive()

    def _handlers(self, handlers):
        if type(handlers) is not list or any(type(name) is not str for name in handlers) or (
                len(handlers) != len(set(handlers)) or set(handlers) != set(self.store.registry)):
            raise PersistenceError('worker handler names must match the authoritative registry')

    def _dispatch(self, operation, arguments):
        registry = self.store.workers
        if operation == 'register':
            arguments = dict(arguments)
            self._handlers(arguments.pop('handlers'))
            return asdict(registry.register(**arguments))
        if operation in ('heartbeat', 'request_stop', 'confirm_stopped'):
            value = getattr(registry, operation)(**arguments)
            return asdict(value) if value is not None else None
        if operation == 'detect_overdue':
            if arguments:
                raise ValueError('detect_overdue takes no arguments')
            return [asdict(record) for record in registry.detect_overdue()]
        if operation == 'claim_next':
            arguments = dict(arguments)
            self._handlers(arguments.pop('handlers'))
            WorkerRegistry._id(arguments.get('owner'), 'owner')
            return _task_payload(self.store, self.store.claim_next(**arguments))
        if operation == 'get':
            return _task_payload(self.store, self.store.get(**arguments))
        if operation == 'finish':
            return self._finish(**arguments)
        raise ValueError('unknown operation')

    def _finish(self, task_id, owner, worker_session, attempt_count, outcome, result, error):
        """The database defines the task, attempt and retry policy, never the client."""
        if type(attempt_count) is not int or attempt_count < 1:
            raise ValueError('attempt_count must be a positive integer')
        if outcome not in ('SUCCESS', 'FAILED') or (outcome == 'SUCCESS' and error is not None):
            raise ValueError('invalid outcome')
        with self.store._connection() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if (row is None or row['status'] != 'RUNNING' or row['owner'] != owner or
                    row['worker_session'] != worker_session or worker_session is None or
                    row['attempt_count'] != attempt_count):
                raise StaleExecutionError('execution was superseded')
            task = self.store._decode(row, db)
        task.result, task.error = result, None
        task.status = TaskStatus(outcome)
        if outcome == 'FAILED':
            if (type(error) is not dict or not {'type', 'message', 'non_retryable'} <= error.keys() or
                    error.keys() - {'type', 'message', 'non_retryable', 'timeout', 'elapsed',
                                    'failure_category', 'retryable'} or any(
                    type(error[key]) is not str for key in ('type', 'message')) or
                    type(error['non_retryable']) is not bool):
                raise ValueError('failure must report a portable exception')
            task.result = None
            error_type, message = error['type'], error['message']
            # Workers may veto replay of any PersistenceError subclass without
            # importing that subclass on the server. A veto cannot grant budget.
            failure_class = PersistenceError if error['non_retryable'] or error_type in (
                'PersistenceError', 'StaleExecutionError', 'RemoteTransportError') else StoredTaskError
            task.error = failure_class(f'{error_type}: {message}')
            if error_type == 'TaskTimeoutError' and {'timeout', 'elapsed'} <= error.keys():
                WorkerRegistry._seconds(error['timeout'], 'timeout')
                _join_timeout(error['elapsed'])
                if error['elapsed'] is None:
                    raise ValueError('elapsed must be finite')
                task.error = TaskTimeoutError(error['timeout'], error['elapsed'], task.name)
            restore_failure_metadata(task.error, error)
            annotate_task_failure(task)
            policy = RetryPolicy(row['retry_limit'], **task._persisted_retry_config)
            if not isinstance(task.error, PersistenceError) and policy.should_retry(task):
                policy.begin_retry(task)
        # finish rechecks owner, attempt and live session inside its write transaction.
        self.store.finish(task, owner)
        return _task_payload(self.store, task)


class _Transport:
    def __init__(self, server_url, token, *, request_timeout=2, ssl_context=None,
                 max_request_bytes=1024 * 1024):
        parsed = urlparse(server_url)
        if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or (
                parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            raise ValueError('server_url must be an HTTP(S) service origin')
        WorkerRegistry._seconds(request_timeout, 'request_timeout')
        if type(max_request_bytes) is not int or max_request_bytes < 1024:
            raise ValueError('max_request_bytes must be an integer >= 1024')
        self.url = server_url.rstrip('/') + '/rpc'
        self.token = _token(token)
        self.timeout, self.context, self.limit = request_timeout, ssl_context, max_request_bytes
        self._opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl_context), _NoRedirect())

    def call(self, operation, **arguments):
        raw = _json(dict(operation=operation, arguments=arguments)).encode('utf-8')
        if len(raw) > self.limit:
            raise RemoteTransportError('request exceeds size limit')
        request = Request(self.url, raw, method='POST', headers={
            'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token})
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                data = response.read(self.limit + 1)
                if len(data) > self.limit:
                    raise RemoteTransportError('response exceeds size limit')
            payload = json.loads(data)
            _json(payload)
            return payload['value']
        except HTTPError as error:
            if error.code == 401:
                raise RemoteAuthenticationError('authentication rejected') from None
            try:
                payload = json.loads(error.read(self.limit + 1))
                cls = _ERRORS.get(payload.get('error'), RemoteTransportError)
            except Exception:
                cls = RemoteTransportError
            finally:
                error.close()
            raise cls('control plane rejected request') from None
        except (URLError, OSError, ValueError, KeyError) as error:
            # No write retries: loss of the response cannot prove the write failed.
            raise RemoteTransportError('control plane request failed; outcome may be unknown') from None


class RemoteWorkerRegistry:
    _id = staticmethod(WorkerRegistry._id)
    _seconds = staticmethod(WorkerRegistry._seconds)

    def __init__(self, transport, handlers):
        self._transport, self._handlers = transport, handlers

    def register(self, worker_id, *, heartbeat_timeout=30, host_id=None):
        import socket
        value = self._transport.call('register', worker_id=worker_id,
            heartbeat_timeout=heartbeat_timeout, host_id=host_id or socket.gethostname(),
            handlers=self._handlers)
        return WorkerRecord(**value)

    def heartbeat(self, worker_id, session_id):
        return WorkerRecord(**self._transport.call('heartbeat', worker_id=worker_id, session_id=session_id))

    def detect_overdue(self):
        return [WorkerSessionRecord(**v) for v in self._transport.call('detect_overdue')]

    def request_stop(self, worker_id, session_id):
        self._transport.call('request_stop', worker_id=worker_id, session_id=session_id)

    def confirm_stopped(self, worker_id, session_id, *, evidence):
        return WorkerSessionRecord(**self._transport.call('confirm_stopped',
            worker_id=worker_id, session_id=session_id, evidence=evidence))


class RemoteTaskStore(SQLiteTaskStore):
    """A worker-only adapter; it never opens a local or network SQLite file."""

    def __init__(self, server_url, registry, token, **transport_options):
        if type(registry) is not dict or not all(
                isinstance(k, str) and k and callable(v) for k, v in registry.items()):
            raise TypeError('registry must map nonempty names to callables')
        self.registry = dict(registry)
        self._transport = _Transport(server_url, token, **transport_options)
        self.workers = RemoteWorkerRegistry(self._transport, sorted(self.registry))

    def _connection(self):
        raise PersistenceError('remote workers cannot access the central SQLite database')

    def _decode_payload(self, payload):
        if payload is None:
            return None
        definition = dict(payload['definition'])
        handler = definition.pop('handler')
        if handler not in self.registry:
            raise PersistenceError('claimed task handler is unavailable on this worker')
        definition['args'] = tuple(definition['args'])
        if definition.get('max_retries') is not None:
            definition['max_retries'] = max(definition['max_retries'], payload['retry_limit'])
        task = Task(id=payload['id'], callable=self.registry[handler],
            status=TaskStatus(payload['status']), retry_count=payload['retry_count'], **definition)
        task._attempt_count = payload['attempt_count']
        task._persisted_retry_limit = payload['retry_limit']
        task._persisted_retry_config = payload['retry_config']
        task._retry_at, task._persisted = payload['retry_at'], True
        task._remote_session_id = payload['worker_session']
        task._handler_name = handler
        task.result = payload['result']
        for field in ('error', 'last_error'):
            value = payload[field]
            error = StoredTaskError(f"{value['type']}: {value['message']}") if value else None
            if error is not None:
                restore_failure_metadata(error, value)
            setattr(task, field, error)
        task._events = [TaskEvent(v['task_id'], v['sequence'], TaskEventType(v['event_type']),
            v['timestamp'], TaskStatus(v['previous_status']) if v['previous_status'] else None,
            TaskStatus(v['status']), v['attempt_count'], v['retry_count'], _json(v['details']))
            for v in payload['events']]
        return task

    def claim_next(self, worker_session, owner):
        return self._decode_payload(self._transport.call('claim_next',
            worker_session=worker_session, owner=owner, handlers=sorted(self.registry)))

    def get(self, task_id):
        return self._decode_payload(self._transport.call('get', task_id=task_id))

    def finish(self, task, owner):
        error = None if task.error is None else dict(type=type(task.error).__name__,
            message=str(task.error), non_retryable=isinstance(task.error, PersistenceError),
            **failure_metadata(task.error))
        if isinstance(task.error, TaskTimeoutError):
            error.update(timeout=task.error.timeout, elapsed=task.error.elapsed)
        payload = self._transport.call('finish', task_id=task.id, owner=owner,
            worker_session=getattr(task, '_remote_session_id', None),
            attempt_count=task.attempt_count,
            outcome='SUCCESS' if task.status is TaskStatus.SUCCESS else 'FAILED',
            result=task.result,
            error=error)
        committed = self._decode_payload(payload)
        # Server retry calculations and timestamps are authoritative.
        # This replaces the client's speculative snapshot, rather than taking a
        # new lifecycle transition (the server may reject its speculative retry).
        object.__setattr__(task, 'status', committed.status)
        object.__setattr__(task, 'retry_count', committed.retry_count)
        task.result, task.error, task.last_error = committed.result, committed.error, committed.last_error
        task._retry_at, task._events = committed.retry_at, committed._events


class RemoteWorker(ReliableWorker):
    """Execute on this host using the central service's fenced worker protocol."""

    def __init__(self, server_url, task_registry, token, worker_id, *,
                 request_timeout=2, ssl_context=None, max_request_bytes=1024 * 1024, **worker_options):
        store = RemoteTaskStore(server_url, task_registry, token,
            request_timeout=request_timeout, ssl_context=ssl_context,
            max_request_bytes=max_request_bytes)
        super().__init__(store, worker_id, **worker_options)
