"""进程内线程池并发执行原语。

本模块刻意只提供固定大小线程池：不含协程、跨进程 Worker、动态扩缩容或
后台常驻调度。``ConcurrentWorker.run`` 只按空闲槽位取任务，最多保留
``max_workers`` 个在途尝试；任一尝试完成后立即补位。
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from .executor import Executor
from .queue import TaskQueue
from .retry import RetryPolicy
from .task import Task, TaskStatus
from .worker import Worker


class ConcurrentWorker(Worker):
    """用固定大小线程池消费 :class:`TaskQueue` 的 Worker。

    返回值按提交给线程池的顺序排列，而非完成顺序；因此任务函数本身的完成
    顺序没有契约。失败后的重试在该尝试结束后重新入队，到期后与其他就绪任务
    一起按优先级及同级 FIFO 竞争空闲槽位；退避等待不占用执行槽位。
    """

    def __init__(
        self,
        queue: TaskQueue,
        executor: Executor,
        *,
        max_workers: int = 4,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not isinstance(max_workers, int) or isinstance(max_workers, bool):
            raise TypeError("max_workers 必须是正整数")
        if max_workers <= 0:
            raise ValueError("max_workers 必须是正整数")
        # 继承 Worker 的依赖校验和 Scheduler 兼容性；只替换 run 的执行模型。
        super().__init__(queue, executor, retry_policy)
        self._max_workers = max_workers

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def run(self) -> list[Task]:
        """按空闲槽位消费，直到队列与在途尝试均为空。

        stop() 后停止取新任务，等待已提交的尝试收尾；这些尝试产生的重试
        仍按策略入队，留给新的 Worker。返回列表保留每次尝试的提交顺序。
        """
        self._begin_run()
        processed: list[Task] = []
        try:
            with ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="workflow-worker") as pool:
                in_flight: dict[Future[Task], Task] = {}
                while True:
                    while len(in_flight) < self._max_workers:
                        # 取任务与提交共用 stop() 的生命周期锁：停止请求生效后
                        # 不会再移走排队任务。锁不覆盖任务执行或等待完成。
                        with self._lifecycle_lock:
                            if self._should_stop():
                                break
                            task = self._queue.dequeue()
                            if task is None:
                                break
                            try:
                                future = pool.submit(self._execute_one, task)
                            except BaseException:
                                self._queue.enqueue(task)
                                raise
                            in_flight[future] = task
                            processed.append(task)

                    if not in_flight:
                        if self._should_stop() or self._queue.is_empty():
                            break
                        # 仅有退避任务时等待到期；不占用线程池执行槽位。
                        task = self._queue.dequeue_wait(stop_requested=self._should_stop)
                        if task is not None:
                            with self._lifecycle_lock:
                                if self._should_stop():
                                    self._queue.enqueue(task)
                                else:
                                    try:
                                        future = pool.submit(self._execute_one, task)
                                    except BaseException:
                                        self._queue.enqueue(task)
                                        raise
                                    in_flight[future] = task
                                    processed.append(task)
                        continue

                    delay = self._queue.next_ready_delay() if len(in_flight) < self._max_workers else None
                    # 等待在途任务的同时，也要在退避到期时补上空闲槽位。
                    timeout = min(delay, 0.05) if delay is not None else None
                    completed, _ = wait(in_flight, timeout=timeout, return_when=FIRST_COMPLETED)
                    # 先回收完成的尝试并安排重试，再从优先级队列补位。
                    # 按提交顺序处理同时观察到的完成项，避免 set 的随机顺序。
                    for future in tuple(in_flight):
                        if future not in completed:
                            continue
                        task = future.result()
                        if (task is in_flight[future] and task.status is TaskStatus.RETRYING
                                and not self._queue.contains(task)):
                            self._queue.enqueue(task)
                        del in_flight[future]
                        if task.status is TaskStatus.FAILED:
                            self._handle_failure(task)
            return processed
        finally:
            self._finish_run()

    def _execute_one(self, task: Task) -> Task:
        return self._executor.execute(task)
