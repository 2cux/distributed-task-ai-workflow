"""进程内线程池并发执行原语。

本模块刻意只提供固定大小线程池：不含协程、跨进程 Worker、动态扩缩容或
后台常驻调度。``ConcurrentWorker.run`` 在调用期间取尽当前队列中的任务，
每一批最多并发 ``max_workers`` 项，并等待所有任务（及其重试）结束后返回。
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

from .executor import Executor
from .queue import TaskQueue
from .retry import RetryPolicy
from .task import Task, TaskStatus
from .worker import Worker


class ConcurrentWorker(Worker):
    """用固定大小线程池消费 :class:`TaskQueue` 的 Worker。

    返回值按提交给线程池的顺序排列，而非完成顺序；因此任务函数本身的完成
    顺序没有契约。失败后的重试在当前批次全部结束后重新入队，并在下一批执行，
    避免同一 Task 的两次尝试重叠。
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
        """并发处理队列，直到一次取批操作观察到队列为空。"""
        processed: list[Task] = []
        with ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="workflow-worker") as pool:
            while batch := self._drain_batch():
                futures: list[Future[Task]] = [pool.submit(self._execute_one, task) for task in batch]
                # 按提交顺序读取，同时确保本批全部结束才启动重试批次。
                for future in futures:
                    task = future.result()
                    processed.append(task)
                    if task.status is TaskStatus.FAILED:
                        self._handle_failure(task)
        return processed

    def _drain_batch(self) -> list[Task]:
        batch: list[Task] = []
        while (task := self._queue.dequeue()) is not None:
            batch.append(task)
        return batch

    def _execute_one(self, task: Task) -> Task:
        return self._executor.execute(task)

    def _handle_failure(self, task: Task) -> None:
        if self._retry_policy is None or not self._retry_policy.should_retry(task):
            return
        self._retry_policy.begin_retry(task)
        self._queue.enqueue(task)
