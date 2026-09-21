"""Task：对系统内一份可被调度、执行、追踪的工作的描述。

Task 只是一份描述。它不负责调度或执行，也不执行重试；执行器与重试策略
会读取本对象并完成状态流转。

字段：

- ``id``          唯一标识
- ``name``        工作名称
- ``callable``    工作要执行的可调用对象
- ``args``        位置参数，默认空
- ``kwargs``      关键字参数，默认空
- ``priority``    调度优先级，数字越大优先级越高，默认 0
- ``status``      生命周期状态，创建时为 PENDING
- ``result``      执行成功的返回值
- ``error``       最近一次执行的异常
- ``max_retries`` 允许的重试次数上限；``None`` 表示由重试策略决定
- ``retry_count`` 已经安排过的重试次数，默认 0
- ``last_error``  最近一次触发重试的异常，默认 None

失败信息分成两个字段：``error`` 描述"这一次执行失败了什么"，会被重新执行
覆盖；``last_error`` 保存"上一次失败留下的是什么"，仅在任务再次进入队列时
被重试策略写入，重试成功不会清除它。

生命周期：

.. code-block:: text

    PENDING ──> RUNNING ──> SUCCESS
       ↑           │
       │           └─────> FAILED ──> RETRYING
       │                      ↑           │
       │                      └───────────┘
       └──────────────────────────────────┘

PENDING 到 FAILED 用于提交阶段就已判定失败的场景，例如可调用对象无法导入。

``RETRYING`` 表示任务已经取到一次重试机会、正处于待重新执行的等待状态；
重试策略把任务改写为该状态后立刻重新入队，因此队列中的 ``RETRYING`` 任务
与 ``PENDING`` 任务一样等待被执行。``FAILED`` 与 ``SUCCESS`` 都是终态。
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
    RETRYING = "RETRYING"


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
    max_retries: int | None = None
    retry_count: int = 0
    # last_error 只是"上一次失败留下了什么"的记账，不参与任务之间的比较，
    # 避免它让两个描述同一份工作的任务被判为不同。
    last_error: BaseException | None = field(default=None, compare=False)

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
        if self.max_retries is not None and (
            not isinstance(self.max_retries, int) or isinstance(self.max_retries, bool)
        ):
            raise TypeError("max_retries 必须是整数或 None")
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError("max_retries 不能为负数")
        if not isinstance(self.retry_count, int) or isinstance(self.retry_count, bool):
            raise TypeError("retry_count 必须是整数")
        if self.retry_count < 0:
            raise ValueError("retry_count 不能为负数")
        if self.last_error is not None and not isinstance(self.last_error, BaseException):
            raise TypeError("last_error 必须是异常对象")
        if self.max_retries is not None and self.retry_count > self.max_retries:
            raise ValueError("retry_count 不能超过 max_retries")
        if self.status is TaskStatus.RETRYING and self.retry_count < 1:
            raise ValueError("RETRYING 状态要求 retry_count 至少为 1")

    @property
    def remaining_retries(self) -> int | None:
        """按任务自身的上限返回剩余重试次数；未设置上限时返回 ``None``。"""
        if self.max_retries is None:
            return None

        return max(self.max_retries - self.retry_count, 0)

    def can_retry(self) -> bool:
        """按任务自身的上限返回是否还有剩余重试次数。

        该方法只读任务字段，不做全局重试决策：``FAILED`` 状态以外的任务一律
        返回 ``False``；未设置 ``max_retries`` 的任务返回 ``True``，因为没有
        任务级上限时剩余次数取决于重试策略的默认值，由重试策略自行判断。
        """
        if self.status is not TaskStatus.FAILED:
            return False
        if self.max_retries is None:
            return True

        return self.retry_count < self.max_retries

    def mark_retrying(self) -> None:
        """把失败的任务改写为等待重试状态，并消耗一次重试次数。"""
        if self.status is not TaskStatus.FAILED:
            raise ValueError(f"只有 FAILED 状态的任务可以进入 RETRYING，当前状态为 {self.status.value}")

        self.retry_count += 1
        self.status = TaskStatus.RETRYING

    def __repr__(self) -> str:
        retry = ""
        if self.retry_count > 0 or self.max_retries is not None:
            retry = f", retry_count={self.retry_count}, max_retries={self.max_retries}"

        return (
            f"Task(id={self.id!r}, name={self.name!r}, "
            f"priority={self.priority}, status={self.status.value}{retry})"
        )
