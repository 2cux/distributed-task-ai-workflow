"""任务的同步执行器。

本模块只负责单个任务的一次执行及其生命周期更新；不包含队列、并发、
调度、重试或持久化。任务是否需要在失败后再次执行由重试策略决定，执行器
只把这一次执行的结果或异常写回任务。

"一次执行"分成两件事：状态流转（本类）与如何调用任务的可调用对象
（:meth:`Executor._invoke`）。默认实现直接在调用线程中同步调用；需要改变
单次尝试执行方式的原语（例如超时）覆写这个扩展点即可，不必复制状态流转。
"""

from __future__ import annotations

from typing import Any

from .task import Task, TaskStatus

#: 允许进入执行的状态：首次执行的 PENDING 与被重试策略重新入队的 RETRYING。
EXECUTABLE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.PENDING, TaskStatus.RETRYING})


class Executor:
    """同步执行 :class:`Task` 的最小执行器。"""

    def execute(self, task: Task) -> Task:
        """执行一个待执行任务，并将结果或异常写回任务。

        仅 ``PENDING``（首次执行）与 ``RETRYING``（重试执行）状态的任务可以
        执行。执行开始前任务会变为 ``RUNNING``；可调用对象正常返回时变为
        ``SUCCESS``，抛出异常时变为 ``FAILED``。异常会被记录在 ``task.error``，
        而不会向调用方再次抛出。

        每次执行都会重置 ``result`` 与 ``error``：``error`` 只描述本次执行失败
        了什么，上一次失败的异常在此之前已由重试策略保存到 ``last_error``。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        if task.status not in EXECUTABLE_STATUSES:
            raise ValueError(f"只能执行 PENDING 或 RETRYING 状态的任务，当前状态为 {task.status.value}")

        task.status = TaskStatus.RUNNING
        task.result = None
        task.error = None

        try:
            task.result = self._invoke(task)
        except BaseException as error:
            task.error = error
            task.status = TaskStatus.FAILED
        else:
            task.status = TaskStatus.SUCCESS

        return task

    def _invoke(self, task: Task) -> Any:
        """调用任务的可调用对象并返回其结果。

        这是"一次尝试"中唯一执行任务代码的地方。默认实现在当前线程中同步
        调用；子类可以覆写它来改变单次尝试的执行方式，而不影响状态流转。
        这里抛出的异常由 :meth:`execute` 按普通失败记录。
        """
        return task.callable(*task.args, **task.kwargs)


def execute(task: Task) -> Task:
    """使用默认执行器同步执行一个任务。"""
    return Executor().execute(task)
