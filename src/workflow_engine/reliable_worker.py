"""Registered consumers of the shared durable queue, with independent heartbeats."""

from __future__ import annotations

from threading import Event, RLock, Thread, current_thread
from uuid import uuid4

from .lifecycle import WorkerStatus
from .persistence import PersistentExecutor, SQLiteTaskStore, StaleExecutionError
from .timeout import TimeoutPolicy
from .worker_registry import (WorkerNotReady, WorkerRegistry, WorkerSessionMismatch,
                              WorkerStopRequested)


class WorkerMonitor:
    """Periodically observe worker heartbeats. Never replay tasks from a timeout."""

    def __init__(self, registry: WorkerRegistry, *, interval: float = 1) -> None:
        registry._seconds(interval, 'interval')
        self.registry = registry
        self.interval = interval
        self.last_error: BaseException | None = None
        self._stop = Event()
        self._lock = RLock()
        self._thread: Thread | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise RuntimeError('WorkerMonitor 只能启动一次')
            self._thread = Thread(target=self._run, name='workflow-worker-monitor', daemon=True)
            try:
                self._thread.start()
            except BaseException as error:
                self.last_error = error
                self._thread = None
                self._stop.set()
                raise

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.registry.detect_overdue()
            except Exception as error:
                self.last_error = error
            self._stop.wait(self.interval)

    def shutdown(self, timeout: float | None = None) -> bool:
        _join_timeout(timeout)
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            if thread is current_thread():
                raise RuntimeError('Monitor 不能等待自身')
            thread.join(timeout)
        return thread is None or not thread.is_alive()


def _join_timeout(timeout: float | None) -> None:
    if timeout is not None:
        # Zero is a useful nonblocking join.
        if isinstance(timeout, bool) or not isinstance(timeout, (float, int)):
            raise TypeError('timeout 必须是有限非负秒数或 None')
        import math
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('timeout 必须是有限非负秒数或 None')


class ReliableWorker:
    """One execution slot per durable worker session, shared across engine processes.

    Heartbeat and consumption use different threads. A slow callable does not make
    an otherwise healthy worker appear dead. A control-plane failure stops new
    claims, but draining waits for the actual callable, including soft timeouts.
    """

    def __init__(self, store: SQLiteTaskStore, worker_id: str, *,
                 heartbeat_interval: float = 1, heartbeat_timeout: float = 10,
                 poll_interval: float = 0.05, timeout_policy: TimeoutPolicy | None = None,
                 host_id: str | None = None) -> None:
        if not isinstance(store, SQLiteTaskStore):
            raise TypeError('store 必须是 SQLiteTaskStore')
        self.registry = store.workers
        self.registry._id(worker_id, 'worker_id')
        for name, value in (('heartbeat_interval', heartbeat_interval),
                            ('heartbeat_timeout', heartbeat_timeout), ('poll_interval', poll_interval)):
            self.registry._seconds(value, name)
        if heartbeat_interval >= heartbeat_timeout:
            raise ValueError('heartbeat_interval 必须小于 heartbeat_timeout')
        self.store = store
        self.worker_id = worker_id
        self.host_id = host_id
        if host_id is not None:
            self.registry._id(host_id, 'host_id')
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.poll_interval = poll_interval
        self._executor = PersistentExecutor(store, None, timeout_policy)
        self._lock = RLock()
        self._stop = Event()
        self._heartbeat_stop = Event()
        self._status = WorkerStatus.CREATED
        self._session_id: str | None = None
        self._thread: Thread | None = None
        self._heartbeat_thread: Thread | None = None
        self.last_error: BaseException | None = None
        self.processed_count = 0

    @property
    def session_id(self) -> str | None:
        with self._lock:
            return self._session_id

    @property
    def status(self) -> WorkerStatus:
        with self._lock:
            return self._status

    def start(self) -> None:
        with self._lock:
            if self._status is not WorkerStatus.CREATED:
                raise RuntimeError('ReliableWorker 只能启动一次')
            record = self.registry.register(self.worker_id, heartbeat_timeout=self.heartbeat_timeout,
                                            host_id=self.host_id)
            self._session_id = record.session_id
            self._status = WorkerStatus.RUNNING
            self._heartbeat_thread = Thread(target=self._heartbeats, name=f'heartbeat-{self.worker_id}', daemon=True)
            self._thread = Thread(target=self._run, name=f'worker-{self.worker_id}', daemon=True)
            try:
                self._heartbeat_thread.start()
                self._thread.start()
            except BaseException as error:
                self._stop.set()
                self._heartbeat_stop.set()
                self._status = WorkerStatus.STOPPED
                self._thread = None
                self.last_error = error
                try:
                    self.registry.request_stop(self.worker_id, record.session_id)
                    self.registry.confirm_stopped(self.worker_id, record.session_id,
                                                  evidence='consumer thread failed to start')
                except Exception as cleanup_error:
                    # No business thread was started. Failed registry writes must
                    # not corrupt local lifecycle or mask the original failure.
                    self.last_error = cleanup_error
                raise

    def _heartbeats(self) -> None:
        while not self._heartbeat_stop.is_set():
            try:
                self.registry.heartbeat(self.worker_id, self._session_id)
                self.registry.detect_overdue()
            except Exception as error:
                with self._lock:
                    self.last_error = error
                # Fail closed. Already-started business code must still drain.
                self._stop.set()
                return
            self._heartbeat_stop.wait(self.heartbeat_interval)

    def _run(self) -> None:
        try:
            with self.store.connection_scope():
                self._consume()
        except WorkerSessionMismatch as error:
            if not self._stop.is_set():
                self.last_error = error
        except Exception as error:
            self.last_error = error
        finally:
            self._stop.set()
            self._heartbeat_stop.set()
            self._heartbeat_thread.join()
            # The executor waits for soft-timeout business threads to exit; at
            # this point no old invocation can overlap a recovered attempt.
            try:
                self.registry.request_stop(self.worker_id, self._session_id)
                self.registry.confirm_stopped(self.worker_id, self._session_id,
                                              evidence='runtime drained all business invocations')
            except WorkerSessionMismatch:
                pass  # A supervisor already stopped/replaced this incarnation.
            except Exception as error:
                self.last_error = error  # Leave unresolved ownership durable.
            with self._lock:
                self._status = WorkerStatus.STOPPED

    def _consume(self) -> None:
        while not self._stop.is_set():
            owner = uuid4().hex
            try:
                task = self.store.claim_next(self._session_id, owner)
            except WorkerStopRequested:
                break
            except WorkerNotReady:
                self._stop.wait(self.poll_interval)
                continue
            if task is None:
                self._stop.wait(self.poll_interval)
                continue
            try:
                self._executor.execute_claimed(task, owner)
            except StaleExecutionError as error:
                self.last_error = error
                break
            with self._lock:
                self.processed_count += 1

    def stop(self) -> None:
        with self._lock:
            if self._status is WorkerStatus.STOPPED:
                return
            if self._status is WorkerStatus.CREATED:
                self._status = WorkerStatus.STOPPED
            elif self._status is WorkerStatus.RUNNING:
                self._status = WorkerStatus.STOPPING
            self._stop.set()
            session_id = self._session_id
        if session_id is not None:
            try:
                self.registry.request_stop(self.worker_id, session_id)
            except WorkerSessionMismatch:
                pass

    def join(self, timeout: float | None = None) -> bool:
        _join_timeout(timeout)
        with self._lock:
            thread = self._thread
        if thread is None:
            return self.status is WorkerStatus.STOPPED
        if thread is current_thread():
            raise RuntimeError('Worker 不能等待自身')
        thread.join(timeout)
        return not thread.is_alive()

    def shutdown(self, timeout: float | None = None) -> bool:
        _join_timeout(timeout)
        self.stop()
        return self.join(timeout)
