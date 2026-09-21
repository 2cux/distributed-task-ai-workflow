"""任务队列原语。

当前阶段提供进程内、先进先出（FIFO）的任务暂存能力。队列只负责接收、
保存和取出 :class:`Task`，不负责执行、持久化或并发协调。

队列对"什么任务可以进入"有两个约束，用来保证重试重新入队时 FIFO 语义与
队列视图不被破坏：

- 只接受等待执行的任务，即 ``PENDING`` 与 ``RETRYING``。失败任务必须先被
  重试策略改写为 ``RETRYING`` 才能回到队列，这样"队列里只有待执行任务"
  这一不变量始终成立。
- 同一个任务对象在队列中至多出现一次。重复入队会被拒绝，避免重试路径上的
  重复回队导致同一任务被并行执行多次、或让 ``size()`` 与实际任务数不一致。
"""

from __future__ import annotations

from collections import deque

from .task import Task, TaskStatus

#: 允许进入队列的状态：都是"等待被执行"的状态。
QUEUEABLE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.PENDING, TaskStatus.RETRYING})


class TaskQueue:
    """用于暂存待执行任务的最小 FIFO 队列。"""

    def __init__(self) -> None:
        self._tasks: deque[Task] = deque()
        self._queued_ids: set[str] = set()

    def enqueue(self, task: Task) -> None:
        """接收并保存一个待执行任务。

        非 ``Task``、非等待执行状态或者已经在队列中的任务都会被拒绝，并且
        不改动队列内容。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        if task.status not in QUEUEABLE_STATUSES:
            raise ValueError(f"只有 PENDING 或 RETRYING 状态的任务可以入队，当前状态为 {task.status.value}")
        if task.id in self._queued_ids:
            raise ValueError(f"任务 {task.id!r} 已经在队列中，不能重复入队")

        self._tasks.append(task)
        self._queued_ids.add(task.id)

    def dequeue(self) -> Task | None:
        """取出最早进入队列的任务；空队列返回 ``None``。"""
        if self.is_empty():
            return None

        task = self._tasks.popleft()
        self._queued_ids.discard(task.id)

        return task

    def is_empty(self) -> bool:
        """返回队列是否没有待处理任务。"""
        return not self._tasks

    def size(self) -> int:
        """返回队列中待处理任务的数量。"""
        return len(self._tasks)

    def contains(self, task: Task) -> bool:
        """返回给定任务当前是否在队列中。"""
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")

        return task.id in self._queued_ids

    def __contains__(self, task: object) -> bool:
        return isinstance(task, Task) and task.id in self._queued_ids
