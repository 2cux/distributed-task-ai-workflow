"""同步 Worker 原语。

Worker 只负责把内存任务队列中的任务交给执行器处理。它不描述或保存任务，
也不决定任务顺序或并发模型；当前实现始终在调用线程中按队列顺序同步执行。

Worker 是"失败 -> 判断剩余次数 -> 重新入队"这条链路的编排者：执行完一个
任务后，如果任务失败且可选的 :class:`RetryPolicy` 判定还有剩余重试次数，
Worker 就把任务重新放回队列尾部，并在下一次循环里再执行它。Worker 自己
不做重试决策，也不修改任务的重试计数；那些都由重试策略完成。

由于重新入队发生在队尾，重试不会插队，也不会阻塞队列中其他任务。每次
重试都会消耗一次策略允许的次数，因此 ``run()`` 仍然在有限步内结束。
"""

from __future__ import annotations

from collections.abc import Callable
from threading import Event, RLock, Thread, current_thread
from typing import Final

from .executor import Executor
from .lifecycle import WorkerStatus
from .queue import TaskQueue
from .retry import RetryPolicy
from .task import Task, TaskStatus


class Worker:
    """消费 :class:`TaskQueue` 并使用 :class:`Executor` 执行任务的同步 Worker。"""

    def __init__(
        self,
        queue: TaskQueue,
        executor: Executor,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not isinstance(queue, TaskQueue):
            raise TypeError("queue 必须是 TaskQueue 实例")
        if not isinstance(executor, Executor):
            raise TypeError("executor 必须是 Executor 实例")
        if retry_policy is not None and not isinstance(retry_policy, RetryPolicy):
            raise TypeError("retry_policy 必须是 RetryPolicy 实例或 None")

        self._queue = queue
        self._executor = executor
        self._retry_policy = retry_policy
        self._status = WorkerStatus.CREATED
        self._lifecycle_lock = RLock()

    @property
    def status(self) -> WorkerStatus:
        """返回当前 Worker 生命周期状态。"""
        with self._lifecycle_lock:
            return self._status

    def stop(self) -> None:
        """请求 Worker 停止。

        对正在运行的 Worker，此方法不会中断已经开始执行的任务；该任务完成后
        ``run()`` 将不再从队列取新任务，并把状态推进到 ``STOPPED``。重复调用
        是安全的。尚未启动的 Worker 会直接进入 ``STOPPED``。
        """
        with self._lifecycle_lock:
            if self._status is WorkerStatus.CREATED:
                self._status = WorkerStatus.STOPPED
            elif self._status is WorkerStatus.RUNNING:
                self._status = WorkerStatus.STOPPING
        # 这对同步 Worker 没有副作用；对等待队列的 WorkerLoop 则能让它立刻
        # 重新检查底层 Worker 的停止状态。
        self._queue.wake_waiters()

    def _begin_run(self) -> None:
        """原子地进入运行态；供 Worker 子类复用。"""
        with self._lifecycle_lock:
            if self._status is not WorkerStatus.CREATED:
                raise RuntimeError(f"Worker 不能从 {self._status.value} 状态启动")
            self._status = WorkerStatus.RUNNING

    def _should_stop(self) -> bool:
        with self._lifecycle_lock:
            return self._status is WorkerStatus.STOPPING

    def _finish_run(self) -> None:
        """完成运行并保证状态进入终态。"""
        with self._lifecycle_lock:
            if self._status is WorkerStatus.RUNNING:
                self._status = WorkerStatus.STOPPING
            self._status = WorkerStatus.STOPPED

    def process_next(
        self,
        *,
        wait_for_task: bool = False,
        stop_requested: Callable[[], bool] | None = None,
    ) -> Task | None:
        """同步处理队首任务，失败时按重试策略决定是否重新入队。

        队列为空时返回 ``None``。返回值始终是被处理的那个任务对象，无论它
        成功、失败还是刚刚被安排重试；重试只是把任务放回队列尾部，再次执行
        发生在后续的 :meth:`process_next` 或 :meth:`run` 循环中。
        """
        def stopping() -> bool:
            return self._should_stop() or (
                stop_requested is not None and stop_requested()
            )

        # ``process_next`` 仍可作为独立的单次消费 API 使用；只有处于 STOPPING
        # 状态的 Worker 才拒绝开始新的任务。这个预检也覆盖普通（非阻塞）出队，
        # 使 WorkerLoop 的 stop 请求不会在下一次循环中取走一个待执行任务。
        if stopping():
            return None

        task = (
            self._queue.dequeue_wait(stop_requested=stopping)
            if wait_for_task
            else self._queue.dequeue()
        )
        if task is None:
            return None

        # 对阻塞出队而言，任务可能刚好在等待被唤醒时遇到 stop；再检查一次，
        # 避免此时启动新的执行。该分支在 WorkerLoop 中不会丢失任务：
        # dequeue_wait 已在持有队列条件锁时完成停止检查。
        if stopping():
            self._queue.enqueue(task)
            return None

        executed = self._executor.execute(task)
        if executed is task and task.status is TaskStatus.RETRYING and not self._queue.contains(task):
            self._queue.enqueue(task)
        task = executed
        if task.status is TaskStatus.FAILED:
            self._handle_failure(task)

        return task

    def run(self) -> list[Task]:
        """同步处理任务，直至队列为空，并按处理顺序返回它们。

        被重新入队的失败任务会在同一轮 ``run()`` 中再次执行，因此返回的列表
        可能包含同一个任务对象的多次出现，按每次尝试的顺序排列。
        """
        self._begin_run()
        processed: list[Task] = []
        try:
            while not self._should_stop():
                task = self.process_next(wait_for_task=not self._queue.is_empty())
                if task is None:
                    break
                processed.append(task)
            return processed
        finally:
            self._finish_run()

    def _handle_failure(self, task: Task) -> None:
        """在任务失败后决定重新入队还是让其停留在终态。"""
        if self._retry_policy is None:
            return
        if not self._retry_policy.should_retry(task):
            return

        # 先由策略消耗一次重试机会并把任务改写为 RETRYING，再入队。顺序不能
        # 反过来：队列只接受等待执行的任务，FAILED 任务入队会被拒绝。重复
        # 重新入队由队列自身的重复检查拦截，不会静默插入两份。
        self._retry_policy.begin_retry(task)
        self._queue.enqueue(task)


class WorkerLoop:
    """在专用后台线程中持续驱动一个 :class:`Worker`。

    这个类是 Worker 的运行外壳，而非 TaskEngine 的另一种入口：它唯一依赖的
    行为是 ``Worker.process_next()``。因此任务提交、引擎装配和业务调度仍在
    WorkerLoop 之外。队列暂时为空时，loop 会在队列上阻塞等待；任务入队或调用
    :meth:`stop` 都会立即唤醒该等待。``idle_wait`` 保留为兼容旧调用方的参数。

    每个实例只能启动一次。停止是协作式的：已经由底层 Worker 开始执行的任务
    不会被中断，loop 随后不会再开始下一轮处理。
    """

    _DEFAULT_IDLE_WAIT: Final[float] = 0.01

    def __init__(self, worker: Worker, *, idle_wait: float = _DEFAULT_IDLE_WAIT) -> None:
        if not isinstance(worker, Worker):
            raise TypeError("worker 必须是 Worker 实例")
        if isinstance(idle_wait, bool) or not isinstance(idle_wait, (int, float)):
            raise TypeError("idle_wait 必须是非负秒数")
        if idle_wait < 0:
            raise ValueError("idle_wait 必须是非负秒数")

        self._worker = worker
        self._idle_wait = float(idle_wait)
        # 生命周期状态与 thread 的创建/读取必须在同一把锁内完成，避免两个
        # 调用方同时 start，或在目标线程结束时观察到过期状态。
        self._lifecycle_lock = RLock()
        self._status = WorkerStatus.CREATED
        self._thread: Thread | None = None
        self._stop_requested = Event()
        self._processed_count = 0

    @property
    def status(self) -> WorkerStatus:
        """返回 loop 的线程安全生命周期快照。"""
        with self._lifecycle_lock:
            return self._status

    @property
    def processed_count(self) -> int:
        """返回 loop 已完成处理的 attempt 数量。"""
        with self._lifecycle_lock:
            return self._processed_count

    @property
    def is_alive(self) -> bool:
        """返回后台线程是否仍存活。"""
        with self._lifecycle_lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """创建并启动后台 loop 线程。"""
        with self._lifecycle_lock:
            if self._status is not WorkerStatus.CREATED:
                raise RuntimeError(f"WorkerLoop 不能从 {self._status.value} 状态启动")
            # 将底层 Worker 纳入 loop 的生命周期，保证 loop 请求停止时其状态也
            # 会推进；直接调用 Worker.stop() 同样会令该 loop 结束。
            self._worker._begin_run()
            self._status = WorkerStatus.RUNNING
            self._thread = Thread(target=self._run, name="workflow-worker-loop", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """请求 loop 停止；重复调用安全，且不会中断正在执行的任务。"""
        with self._lifecycle_lock:
            if self._status is WorkerStatus.CREATED:
                self._status = WorkerStatus.STOPPED
            elif self._status is WorkerStatus.RUNNING:
                self._status = WorkerStatus.STOPPING
            self._stop_requested.set()
            self._worker.stop()
            self._worker._queue.wake_waiters()

    def shutdown(self, timeout: float | None = None) -> bool:
        """请求优雅停止并等待后台线程收尾。

        已开始的任务会正常结束；尚未开始的任务留在队列中供新的 Worker
        继续处理。返回 ``True`` 表示 loop 已在 ``timeout`` 内结束，``False``
        表示仍在等待当前任务完成。传入 ``None`` 时无限等待。
        """
        self.stop()
        return self.join(timeout)

    def join(self, timeout: float | None = None) -> bool:
        """等待 loop 线程结束，并返回它是否已经结束。"""
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("timeout 必须是非负秒数或 None")
            if timeout < 0:
                raise ValueError("timeout 必须是非负秒数或 None")

        with self._lifecycle_lock:
            thread = self._thread
        if thread is None:
            return self.status is WorkerStatus.STOPPED
        if thread is current_thread():
            raise RuntimeError("WorkerLoop 线程不能等待自身")
        thread.join(timeout)
        return not thread.is_alive()

    def _run(self) -> None:
        try:
            while self._is_running() and not self._worker._should_stop():
                task = self._worker.process_next(
                    wait_for_task=True,
                    stop_requested=self._stop_requested.is_set,
                )
                if task is not None:
                    with self._lifecycle_lock:
                        self._processed_count += 1
                    continue

        finally:
            with self._lifecycle_lock:
                self._status = WorkerStatus.STOPPED
            self._worker._finish_run()
            self._worker._queue.wake_waiters()

    def _is_running(self) -> bool:
        with self._lifecycle_lock:
            return self._status is WorkerStatus.RUNNING
