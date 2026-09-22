"""任务超时原语：限制"一次执行"的持续时间。

超时是 **一次尝试（attempt）** 的属性，不是任务生命周期的总时间限制：

- 上限在每次执行开始时重新计时，一次尝试用掉的时间不会被后续尝试继承；
- 任务被重试多次时，每次尝试都拿到完整的一份上限；
- 本模块不提供"任务必须在某个时刻之前全部完成"的总体截止时间。

职责划分与重试链路保持一致，都是"描述 -> 取值 -> 编排"三段：

| 组件 | 职责 |
| --- | --- |
| ``Task.timeout`` | 只保存这份工作每次执行的超时上限；``None`` 表示由策略决定 |
| :class:`TimeoutPolicy` | 只做取值：任务级上限优先、策略默认值兜底，两处都没有则不设上限 |
| :func:`run_with_timeout` | 只做执行：在限定时间内运行一个可调用对象，超时抛出 :class:`TaskTimeoutError` |
| :class:`TimeoutExecutor` | 编排：把策略取到的上限交给 ``run_with_timeout``，其余生命周期沿用 :class:`Executor` |

与重试的关系：超时的这次执行以 ``FAILED`` 结束，``task.error`` 是
:class:`TaskTimeoutError`。是否重试仍由 :class:`RetryPolicy` 判断，本模块不
替任务决定重试，也不读写重试次数；被重试的任务在下一次尝试里重新获得完整
上限。超时因此只是"一次失败"，而不是一种特殊终态。

执行方式与边界：本模块用"独立守护线程 + 限时等待"实现 **软超时**。超过上限
时调用方立即返回并把这次尝试判为失败，但已经开始的可调用对象不会被强行
终止，它会继续在后台线程中运行。若重试策略安排下一次尝试，旧 attempt 与新
attempt 的任务函数可能重叠运行；这是软超时的既定语义。旧 attempt 后续的返回值
与异常只保存在它自己的线程局部结果槽中，永远不会再写回 ``Task``，因此不能覆盖
新 attempt 写入的 ``status``、``result`` 或 ``error``。本模块不做 Worker 强杀、
进程隔离或分布式超时：要在超时点真正终止执行，必须把调用放进可被终止的进程或
容器里，那属于后续阶段的能力。
"""

from __future__ import annotations

import math
import threading
import time
from typing import Any, Callable

from .executor import Executor
from .task import Task

#: 受限执行所在线程的名字，便于调试时识别仍在后台运行的那次尝试。
ATTEMPT_THREAD_NAME = "workflow-engine-attempt"


class TaskTimeoutError(TimeoutError):
    """一次执行在完成之前超过了它的超时上限。

    继承内置 ``TimeoutError``，因此既能被精确捕获，也能被
    ``except TimeoutError`` 这样的通用写法捕获。``timeout`` 是这次执行适用的
    上限，``elapsed`` 是已经等待的秒数，``task_name`` 是任务名称（未提供时为
    ``None``）。
    """

    def __init__(
        self,
        timeout: float,
        elapsed: float,
        task_name: str | None = None,
    ) -> None:
        self.timeout = timeout
        self.elapsed = elapsed
        self.task_name = task_name

        who = "本次执行" if task_name is None else f"任务 {task_name!r} 的一次执行"
        super().__init__(f"{who}超过 {timeout} 秒上限（已运行 {elapsed:.6f} 秒）")


def validate_timeout(timeout: float | None, field: str = "timeout") -> float | None:
    """校验一个超时上限取值，并按原样返回。

    ``None`` 表示不限制；其余取值必须是有限正数。布尔值被明确拒绝，避免
    ``True`` 被当成 1 秒。
    """
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError(f"{field} 必须是有限正数或 None")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"{field} 必须是有限正数或 None")

    return timeout


class TimeoutPolicy:
    """决定一次执行可以使用多长时间的最小策略。

    取值规则与 :class:`RetryPolicy` 一致，都是任务级优先：

    1. 任务设置了 ``Task.timeout`` 时使用任务自己的上限；
    2. 任务未设置时回退到本策略的默认上限；
    3. 两处都没有时返回 ``None``，表示这次执行不设上限。

    策略只回答"上限是多少"，不做执行、不改写任务，也不判断超时是否发生。
    """

    def __init__(self, timeout: float | None = None) -> None:
        self._timeout = validate_timeout(timeout)

    @property
    def timeout(self) -> float | None:
        """返回未单独设置上限的任务所使用的默认超时上限。"""
        return self._timeout

    def limit_for(self, task: Task) -> float | None:
        """返回该任务每一次执行适用的超时上限；``None`` 表示不设上限。"""
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        if task.timeout is not None:
            return task.timeout

        return self._timeout

    def is_limited(self, task: Task) -> bool:
        """返回该任务的每一次执行是否受超时上限约束。"""
        return self.limit_for(task) is not None


def timeout_for(task: Task, timeout: float | None = None) -> float | None:
    """使用默认超时策略返回任务每一次执行的上限。"""
    return TimeoutPolicy(timeout=timeout).limit_for(task)


def run_with_timeout(
    func: Callable[[], Any],
    timeout: float | None,
    *,
    task_name: str | None = None,
) -> Any:
    """运行 ``func``；到 ``timeout`` 秒的上限时仍未结束就放弃这次运行。

    ``timeout`` 为 ``None`` 时直接在当前线程调用 ``func``，返回值与异常都与
    不设超时完全一致。设置了上限时 ``func`` 在独立守护线程中运行，调用方一直
    等到上限时刻：

    - 在上限内结束：返回它的返回值，或原样抛出它自己的异常；
    - 到上限时刻仍未结束：抛出 :class:`TaskTimeoutError`，``func`` 继续在后台
      运行，它之后的返回值与异常都被丢弃。

    计时使用单调时钟，覆盖 ``func`` 自身的运行时间；线程创建的开销也计入
    这一次等待。等待按截止时刻推进而不是只等一次：``Event.wait`` 可能因为
    操作系统计时精度提前返回，若只等一次，一次刚好用完上限的执行会被误判为
    超时。
    """
    limit = validate_timeout(timeout)
    if limit is None:
        return func()

    outcome: list[Any] = []
    failure: list[BaseException] = []
    finished = threading.Event()

    def attempt() -> None:
        try:
            outcome.append(func())
        except BaseException as error:  # 交给调用方重放，保持异常语义不变
            failure.append(error)
        finally:
            finished.set()

    thread = threading.Thread(target=attempt, name=ATTEMPT_THREAD_NAME, daemon=True)
    started = time.monotonic()
    deadline = started + limit
    thread.start()

    completed = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if finished.wait(remaining):
            completed = True
            break

    elapsed = time.monotonic() - started

    if not completed:
        raise TaskTimeoutError(limit, elapsed, task_name)

    if failure:
        raise failure[0]

    return outcome[0]


class TimeoutExecutor(Executor):
    """在一次执行的调用外施加超时上限的执行器。

    状态流转、可执行状态校验、结果与异常的写回全部沿用 :class:`Executor`；
    本类只覆写 :meth:`Executor._invoke`，把任务代码的运行时间限制在策略给出
    的上限内。上限由 :class:`TimeoutPolicy` 解析：任务级 ``Task.timeout``
    优先，策略默认值兜底，两处都没有时不设上限，此时执行方式与
    :class:`Executor` 完全相同（仍在调用线程中同步执行）。

    超时的那次尝试以 ``FAILED`` 结束、``task.error`` 为
    :class:`TaskTimeoutError`；是否重试由 Worker 与 :class:`RetryPolicy`
    决定，本执行器不感知重试。
    """

    def __init__(self, timeout_policy: TimeoutPolicy | None = None) -> None:
        if timeout_policy is not None and not isinstance(timeout_policy, TimeoutPolicy):
            raise TypeError("timeout_policy 必须是 TimeoutPolicy 实例或 None")

        self._timeout_policy = TimeoutPolicy() if timeout_policy is None else timeout_policy

    @property
    def timeout_policy(self) -> TimeoutPolicy:
        """返回本执行器使用的超时策略。"""
        return self._timeout_policy

    def timeout_for(self, task: Task) -> float | None:
        """返回该任务每一次执行适用的超时上限。"""
        return self._timeout_policy.limit_for(task)

    def _invoke(self, task: Task) -> Any:
        invoke = super()._invoke
        limit = self.timeout_for(task)
        if limit is None:
            return invoke(task)

        return run_with_timeout(lambda: invoke(task), limit, task_name=task.name)
