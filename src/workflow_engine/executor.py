"""任务的同步执行器。

本模块只负责单个任务的一次执行及其生命周期更新；不包含队列、并发、
调度、重试或持久化。
"""

from __future__ import annotations

from .task import Task, TaskStatus


class Executor:
    """同步执行 :class:`Task` 的最小执行器。"""

    def execute(self, task: Task) -> Task:
        """执行一个待执行任务，并将结果或异常写回任务。

        仅 ``PENDING`` 状态的任务可以执行。执行开始前任务会变为
        ``RUNNING``；可调用对象正常返回时变为 ``SUCCESS``，抛出异常时
        变为 ``FAILED``。异常会被记录在 ``task.error``，而不会向调用方
        再次抛出。
        """
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        if task.status is not TaskStatus.PENDING:
            raise ValueError("只能执行 PENDING 状态的任务")

        task.status = TaskStatus.RUNNING
        task.result = None
        task.error = None

        try:
            task.result = task.callable(*task.args, **task.kwargs)
        except BaseException as error:
            task.error = error
            task.status = TaskStatus.FAILED
        else:
            task.status = TaskStatus.SUCCESS

        return task


def execute(task: Task) -> Task:
    """使用默认执行器同步执行一个任务。"""
    return Executor().execute(task)
