"""任务重试策略原语。

本模块决定失败任务是否还有重试预算，以及下一次尝试的退避到期时间。它
不执行任务、不接触队列、不消费执行器；把决定"重新入队"的动作留给 Worker。
这样重试语义可以独立测试，也能被后续的调度策略复用。

重试次数上限的取值规则是任务级优先：

1. 任务设置了 ``max_retries`` 时使用任务自己的上限；
2. 任务未设置时回退到 :class:`RetryPolicy` 的默认上限。

策略在工作时会把任务改写为 ``RETRYING`` 并消耗一次重试次数，同时把本次
失败异常记入 ``last_error``，让"是否重试"与"记录重试"落在同一处，避免
Worker 重复判定。
"""

from __future__ import annotations

import math
import time

from .task import Task, TaskStatus


class RetryPolicyError(RuntimeError):
    """在任务的当前状态无法开始重试时抛出。"""


class RetryPolicy:
    """重试预算和指数退避策略；initial_delay=0 保持立即重试。"""

    def __init__(self, max_retries: int = 0, *, initial_delay: float = 0,
                 backoff_factor: float = 2, max_delay: float = 60) -> None:
        if not isinstance(max_retries, int) or isinstance(max_retries, bool):
            raise TypeError("max_retries 必须是整数")
        if max_retries < 0:
            raise ValueError("max_retries 不能为负数")

        self._max_retries = max_retries
        for name, value in (("initial_delay", initial_delay),
                            ("backoff_factor", backoff_factor), ("max_delay", max_delay)):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} 必须是有限数值")
            if not math.isfinite(value) or value < (1 if name == "backoff_factor" else 0):
                raise ValueError(f"{name} 必须是有限数值且不小于 {1 if name == 'backoff_factor' else 0}")
        self._initial_delay = float(initial_delay)
        self._backoff_factor = float(backoff_factor)
        self._max_delay = float(max_delay)

    @property
    def initial_delay(self) -> float:
        return self._initial_delay

    @property
    def backoff_factor(self) -> float:
        return self._backoff_factor

    @property
    def max_delay(self) -> float:
        return self._max_delay

    def delay_for(self, retry_number: int) -> float:
        """第 n 次重试的秒数：min(initial_delay * factor ** (n-1), max_delay)。"""
        if isinstance(retry_number, bool) or not isinstance(retry_number, int):
            raise TypeError("retry_number 必须是正整数")
        if retry_number < 1:
            raise ValueError("retry_number 必须是正整数")
        if self.initial_delay == 0 or self.max_delay == 0:
            return 0.0
        if self.initial_delay >= self.max_delay:
            return self.max_delay
        if self.backoff_factor == 1:
            return self.initial_delay
        # 先判断封顶，避免大量重试时指数计算溢出。
        cap_exponent = (math.log(self.max_delay) - math.log(self.initial_delay)) / math.log(self.backoff_factor)
        if retry_number - 1 >= cap_exponent:
            return self.max_delay
        try:
            return min(self.initial_delay * self.backoff_factor ** (retry_number - 1), self.max_delay)
        except OverflowError:
            return min(math.exp(math.log(self.initial_delay) +
                                (retry_number - 1) * math.log(self.backoff_factor)), self.max_delay)

    @property
    def max_retries(self) -> int:
        """返回未单独设置上限的任务所使用的默认重试次数。"""
        return self._max_retries

    def limit_for(self, task: Task) -> int:
        """返回该任务适用的重试次数上限。"""
        if task.max_retries is not None:
            return task.max_retries

        return self._max_retries

    def can_retry(self, task: Task) -> bool:
        """返回这个失败的任务是否还能再被执行一次。"""
        return task.retry_count < self.limit_for(task)

    def should_retry(self, task: Task) -> bool:
        """返回失败的任务是否应当重新入队。

        只有 ``FAILED`` 状态的任务才有重试资格；``RUNNING``、``SUCCESS``、
        ``PENDING``、``RETRYING`` 状态一律返回 ``False``，避免把未失败或
        已经在队列中的任务重复入队。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        with task._lock:
            if task.status is not TaskStatus.FAILED:
                return False

            return self.can_retry(task)

    def begin_retry(self, task: Task) -> Task:
        """为任务保留一次重试机会，并把它置为待重新执行的 ``RETRYING``。

        记入 ``last_error`` 的异常优先取 ``task.error``（本次执行的失败），
        没有时回退到已有的 ``last_error``。该方法不把任务放回队列，重新入队
        由 Worker 在确认队列可接收后完成。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        with task._lock:
            if not self.should_retry(task):
                raise RetryPolicyError(
                    f"任务 {task.name!r} 当前无法重试："
                    f"status={task.status.value}, retry_count={task.retry_count}, "
                    f"max_retries={self.limit_for(task)}"
                )

            task.last_error = task.error if task.error is not None else task.last_error
            task._retry_delay = self.delay_for(task.retry_count + 1)
            task._retry_at = time.time() + task._retry_delay if task._retry_delay else None
            task.mark_retrying()

        return task


def should_retry(task: Task, max_retries: int = 0) -> bool:
    """使用默认重试策略判断一个任务是否应当重新执行。"""
    return RetryPolicy(max_retries=max_retries).should_retry(task)
