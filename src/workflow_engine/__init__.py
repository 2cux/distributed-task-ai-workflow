"""系统执行原语。

本包包含任务原语、最小同步执行器、内存任务队列和同步 Worker。
"""

from .executor import Executor, execute
from .queue import TaskQueue
from .task import Task, TaskStatus
from .worker import Worker

__all__ = ["Executor", "Task", "TaskQueue", "TaskStatus", "Worker", "execute"]
