"""任务重试策略原语。

本模块只回答一个决策问题：一个刚刚失败的任务还有没有剩余重试次数。它
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

from .task import Task, TaskStatus


class RetryPolicyError(RuntimeError):
    """在任务的当前状态无法开始重试时抛出。"""


class RetryPolicy:
    """决定失败任务是否重新执行，并记录重试次数的最小策略。"""

    def __init__(self, max_retries: int = 0) -> None:
        if not isinstance(max_retries, int) or isinstance(max_retries, bool):
            raise TypeError("max_retries 必须是整数")
        if max_retries < 0:
            raise ValueError("max_retries 不能为负数")

        self._max_retries = max_retries

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
        if not self.should_retry(task):
            raise RetryPolicyError(
                f"任务 {task.name!r} 当前无法重试："
                f"status={task.status.value}, retry_count={task.retry_count}, "
                f"max_retries={self.limit_for(task)}"
            )

        task.last_error = task.error if task.error is not None else task.last_error
        task.mark_retrying()

        return task


def should_retry(task: Task, max_retries: int = 0) -> bool:
    """使用默认重试策略判断一个任务是否应当重新执行。"""
    return RetryPolicy(max_retries=max_retries).should_retry(task)
