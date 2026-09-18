"""系统执行原语。

本包包含任务原语及最小同步执行器，不包含调度、队列或存储模块。
"""

from .executor import Executor, execute
from .task import Task, TaskStatus

__all__ = ["Executor", "Task", "TaskStatus", "execute"]
