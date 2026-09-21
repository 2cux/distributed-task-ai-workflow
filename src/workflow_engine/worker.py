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

from .executor import Executor
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

    def process_next(self) -> Task | None:
        """同步处理队首任务，失败时按重试策略决定是否重新入队。

        队列为空时返回 ``None``。返回值始终是被处理的那个任务对象，无论它
        成功、失败还是刚刚被安排重试；重试只是把任务放回队列尾部，再次执行
        发生在后续的 :meth:`process_next` 或 :meth:`run` 循环中。
        """
        task = self._queue.dequeue()
        if task is None:
            return None

        self._executor.execute(task)
        if task.status is TaskStatus.FAILED:
            self._handle_failure(task)

        return task

    def run(self) -> list[Task]:
        """同步处理任务，直至队列为空，并按处理顺序返回它们。

        被重新入队的失败任务会在同一轮 ``run()`` 中再次执行，因此返回的列表
        可能包含同一个任务对象的多次出现，按每次尝试的顺序排列。
        """
        processed: list[Task] = []
        while (task := self.process_next()) is not None:
            processed.append(task)

        return processed

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
