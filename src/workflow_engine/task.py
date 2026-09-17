"""Task：对系统内一份可被调度、执行、追踪的工作的描述。

Task 只是一份描述。它不负责调度，不负责执行，也不负责状态流转。
调度、执行、状态流转由其它模块读取本对象后完成。

字段：

- ``id``       唯一标识
- ``name``     工作名称
- ``callable`` 工作要执行的可调用对象
- ``args``     位置参数，默认空
- ``kwargs``   关键字参数，默认空
- ``priority`` 调度优先级，数字越大优先级越高，默认 0
- ``status``   生命周期状态，创建时为 PENDING
- ``result``   执行成功的返回值
- ``error``    执行失败的异常

生命周期：

.. code-block:: text

    PENDING ──> RUNNING ──> SUCCESS
       │           │
       │           └─────> FAILED
       └──────────────────> FAILED

PENDING 到 FAILED 用于提交阶段就已判定失败的场景，例如可调用对象无法导入。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class TaskStatus(str, Enum):
    """任务生命周期状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


def generate_id() -> str:
    """生成任务 id。"""
    return uuid.uuid4().hex


@dataclass
class Task:
    """一份可被调度、执行、追踪的工作的描述。"""

    name: str
    callable: Callable[..., Any]
    id: str = field(default_factory=generate_id)
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: BaseException | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id:
            raise ValueError("id 必须为非空字符串")
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name 必须为非空字符串")
        if not callable(self.callable):
            raise TypeError("callable 必须是可调用对象")
        if isinstance(self.args, (str, bytes)) or not isinstance(self.args, tuple):
            raise TypeError("args 必须是元组")
        if not isinstance(self.kwargs, dict):
            raise TypeError("kwargs 必须是字典")
        if not all(isinstance(key, str) for key in self.kwargs):
            raise TypeError("kwargs 的键必须是字符串")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError("priority 必须是整数")
        if not isinstance(self.status, TaskStatus):
            raise TypeError("status 必须是 TaskStatus")
        if self.error is not None and not isinstance(self.error, BaseException):
            raise TypeError("error 必须是异常对象")
        if self.status is TaskStatus.SUCCESS and self.error is not None:
            raise ValueError("SUCCESS 状态不允许携带 error")

    def __repr__(self) -> str:
        return (
            f"Task(id={self.id!r}, name={self.name!r}, "
            f"priority={self.priority}, status={self.status.value})"
        )
