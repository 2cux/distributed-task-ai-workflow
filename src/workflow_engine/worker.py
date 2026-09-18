"""同步 Worker 原语。

Worker 只负责把内存任务队列中的任务交给执行器处理。它不描述或保存任务，
也不决定任务顺序、重试策略或并发模型；当前实现始终在调用线程中按队列顺序
同步执行。
"""

from __future__ import annotations

from .executor import Executor
from .queue import TaskQueue
from .task import Task


class Worker:
    """消费 :class:`TaskQueue` 并使用 :class:`Executor` 执行任务的同步 Worker。"""

    def __init__(self, queue: TaskQueue, executor: Executor) -> None:
        if not isinstance(queue, TaskQueue):
            raise TypeError("queue 必须是 TaskQueue 实例")
        if not isinstance(executor, Executor):
            raise TypeError("executor 必须是 Executor 实例")

        self._queue = queue
        self._executor = executor

    def process_next(self) -> Task | None:
        """同步处理队首任务；当队列为空时返回 ``None``。"""
        task = self._queue.dequeue()
        if task is None:
            return None

        return self._executor.execute(task)

    def run(self) -> list[Task]:
        """同步处理任务，直至队列为空，并按处理顺序返回它们。"""
        processed: list[Task] = []
        while (task := self.process_next()) is not None:
            processed.append(task)

        return processed
