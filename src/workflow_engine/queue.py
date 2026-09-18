"""任务队列原语。

当前阶段提供进程内、先进先出（FIFO）的任务暂存能力。队列只负责接收、
保存和取出 :class:`Task`，不负责执行、持久化或并发协调。
"""

from __future__ import annotations

from collections import deque

from .task import Task


class TaskQueue:
    """用于暂存待执行任务的最小 FIFO 队列。"""

    def __init__(self) -> None:
        self._tasks: deque[Task] = deque()

    def enqueue(self, task: Task) -> None:
        """接收并保存一个任务。"""
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")

        self._tasks.append(task)

    def dequeue(self) -> Task | None:
        """取出最早进入队列的任务；空队列返回 ``None``。"""
        if self.is_empty():
            return None

        return self._tasks.popleft()

    def is_empty(self) -> bool:
        """返回队列是否没有待处理任务。"""
        return not self._tasks

    def size(self) -> int:
        """返回队列中待处理任务的数量。"""
        return len(self._tasks)
