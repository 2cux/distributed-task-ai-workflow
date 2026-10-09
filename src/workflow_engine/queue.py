"""任务队列原语。

当前阶段提供进程内、稳定优先级的任务暂存能力。队列只负责接收、保存和
取出 :class:`Task`，不负责执行或持久化。到期任务中数值更大的
``Task.priority`` 会先被取出；优先级相同的任务保持入队 FIFO 顺序。
未到期的重试由独立的延迟堆保存，不阻塞已就绪任务。

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
from .events import TaskEventType

#: 允许进入队列的状态：都是"等待被执行"的状态。
QUEUEABLE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.PENDING, TaskStatus.RETRYING})


class TaskQueue:
    """用于暂存待执行任务的稳定优先级队列。"""

    def __init__(self) -> None:
        # ``heapq`` 是最小堆，故将优先级取负；序号保证同优先级的 FIFO。
        self._tasks: list[tuple[int, int, Task]] = []
        self._delayed: list[tuple[float, int, tuple[int, int, Task]]] = []
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

                sequence = next(self._sequence)
                entry = (-task.priority, sequence, task)
                delay = max(0.0, task.retry_at - time.time()) if task.retry_at is not None else 0.0
                if delay:
                    heapq.heappush(self._delayed, (time.monotonic() + delay, sequence, entry))
                else:
                    heapq.heappush(self._tasks, entry)
                self._queued_ids.add(task.id)
                if task.status is TaskStatus.PENDING and not getattr(task, "_persisted", False):
                    task._record_event(TaskEventType.SUBMITTED, details={"priority": task.priority})
                self._tasks_ready.notify_all()

    def dequeue(self) -> Task | None:
        """取出最高优先级的就绪任务；同优先级时按入队 FIFO，未到期返回 None。"""
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
            deadline = None if timeout is None else time.monotonic() + float(timeout)
            while True:
                self._promote_due_locked()
                if self._tasks:
                    break
                if stop_requested is not None and stop_requested():
                    return None
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                if self._delayed:
                    due_in = max(0.0, self._delayed[0][0] - time.monotonic())
                    remaining = due_in if remaining is None else min(remaining, due_in)
                # Condition.wait 有平台超时上限；分段等待不改变到期语义。
                self._tasks_ready.wait(None if remaining is None else min(remaining, threading.TIMEOUT_MAX))
            if stop_requested is not None and stop_requested():
                return None
            return self._dequeue_locked()

    def wake_waiters(self) -> None:
        """唤醒等待消费者，以便其重新检查外部停止状态。"""
        with self._tasks_ready:
            self._tasks_ready.notify_all()

    def _dequeue_locked(self) -> Task | None:
        self._promote_due_locked()
        if not self._tasks:
            return None

        _, _, task = heapq.heappop(self._tasks)
        self._queued_ids.discard(task.id)
        return task

    def _promote_due_locked(self) -> None:
        now = time.monotonic()
        while self._delayed and self._delayed[0][0] <= now:
            _, _, entry = heapq.heappop(self._delayed)
            heapq.heappush(self._tasks, entry)

    def next_ready_delay(self) -> float | None:
        """就绪任务返回 0，仅有延迟任务返回剩余秒数，空队列返回 None。"""
        with self._lock:
            self._promote_due_locked()
            if self._tasks:
                return 0.0
            return max(0.0, self._delayed[0][0] - time.monotonic()) if self._delayed else None

    def is_empty(self) -> bool:
        """返回队列是否没有待处理任务。"""
        with self._lock:
            return not self._queued_ids

    def size(self) -> int:
        """返回队列中待处理任务的数量。"""
        with self._lock:
            return len(self._queued_ids)

    def contains(self, task: Task) -> bool:
        """返回给定任务当前是否在队列中。"""
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")

        with self._lock:
            return task.id in self._queued_ids

    def __contains__(self, task: object) -> bool:
        return isinstance(task, Task) and self.contains(task)
