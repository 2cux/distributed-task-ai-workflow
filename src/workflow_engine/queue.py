"""任务队列原语。

当前阶段提供进程内、稳定优先级的任务暂存能力。队列只负责接收、保存和
取出 :class:`Task`，不负责执行、持久化或并发协调。数值更大的
``Task.priority`` 会先被取出；优先级相同的任务保持先进先出（FIFO）顺序。

队列对"什么任务可以进入"有两个约束，用来保证重试重新入队时 FIFO 语义与
队列视图不被破坏：

- 只接受等待执行的任务，即 ``PENDING`` 与 ``RETRYING``。失败任务必须先被
  重试策略改写为 ``RETRYING`` 才能回到队列，这样"队列里只有待执行任务"
  这一不变量始终成立。
- 同一个任务对象在队列中至多出现一次。重复入队会被拒绝，避免重试路径上的
  重复回队导致同一任务被并行执行多次、或让 ``size()`` 与实际任务数不一致。
"""

from __future__ import annotations

from collections.abc import Callable
import heapq
from itertools import count
import threading
import time

from .task import Task, TaskStatus

#: 允许进入队列的状态：都是"等待被执行"的状态。
QUEUEABLE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.PENDING, TaskStatus.RETRYING})


class TaskQueue:
    """用于暂存待执行任务的稳定优先级队列。"""

    def __init__(self) -> None:
        # ``heapq`` 是最小堆，故将优先级取负；序号保证同优先级的 FIFO。
        self._tasks: list[tuple[int, int, Task]] = []
        self._sequence = count()
        self._queued_ids: set[str] = set()
        self._lock = threading.RLock()
        self._tasks_ready = threading.Condition(self._lock)

    def enqueue(self, task: Task) -> None:
        """接收并保存一个待执行任务。

        非 ``Task``、非等待执行状态或者已经在队列中的任务都会被拒绝，并且
        不改动队列内容。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        # 统一采用“队列锁 -> 任务锁”的顺序，令状态检查和去重检查成为一个
        # 原子操作，多个提交线程不能把同一任务插入两次。
        with self._tasks_ready:
            with task._lock:
                if task.status not in QUEUEABLE_STATUSES:
                    raise ValueError(f"只有 PENDING 或 RETRYING 状态的任务可以入队，当前状态为 {task.status.value}")
                if task.status is TaskStatus.PENDING and task.attempt_count != 0:
                    raise ValueError("已经执行过首次尝试的任务不能以 PENDING 状态再次入队")
                if task.id in self._queued_ids:
                    raise ValueError(f"任务 {task.id!r} 已经在队列中，不能重复入队")

                heapq.heappush(self._tasks, (-task.priority, next(self._sequence), task))
                self._queued_ids.add(task.id)
                self._tasks_ready.notify()

    def dequeue(self) -> Task | None:
        """取出最高优先级任务；同优先级时取出最早进入队列的任务。"""
        with self._lock:
            return self._dequeue_locked()

    def dequeue_wait(
        self,
        timeout: float | None = None,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> Task | None:
        """等待并取出一个任务；超时前没有任务时返回 ``None``。

        ``None`` 表示无限等待。入队会立即唤醒等待者，因此常驻 worker 无需
        通过定时轮询发现新任务。
        """
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("timeout 必须是非负秒数或 None")
            if timeout < 0:
                raise ValueError("timeout 必须是非负秒数或 None")
        if stop_requested is not None and not callable(stop_requested):
            raise TypeError("stop_requested 必须是可调用对象或 None")

        with self._tasks_ready:
            if timeout is None:
                while not self._tasks:
                    if stop_requested is not None and stop_requested():
                        return None
                    self._tasks_ready.wait()
            else:
                deadline = time.monotonic() + float(timeout)
                while not self._tasks:
                    if stop_requested is not None and stop_requested():
                        return None
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self._tasks_ready.wait(remaining)
            if stop_requested is not None and stop_requested():
                return None
            return self._dequeue_locked()

    def wake_waiters(self) -> None:
        """唤醒等待消费者，以便其重新检查外部停止状态。"""
        with self._tasks_ready:
            self._tasks_ready.notify_all()

    def _dequeue_locked(self) -> Task | None:
        if not self._tasks:
            return None

        _, _, task = heapq.heappop(self._tasks)
        self._queued_ids.discard(task.id)
        return task

    def is_empty(self) -> bool:
        """返回队列是否没有待处理任务。"""
        with self._lock:
            return not self._tasks

    def size(self) -> int:
        """返回队列中待处理任务的数量。"""
        with self._lock:
            return len(self._tasks)

    def contains(self, task: Task) -> bool:
        """返回给定任务当前是否在队列中。"""
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")

        with self._lock:
            return task.id in self._queued_ids

    def __contains__(self, task: object) -> bool:
        return isinstance(task, Task) and self.contains(task)
