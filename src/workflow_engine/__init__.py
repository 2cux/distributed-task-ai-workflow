"""系统执行原语。

本包只包含原语本身，不包含调度、执行、存储等模块。
"""

from .task import Task, TaskStatus

__all__ = ["Task", "TaskStatus"]
