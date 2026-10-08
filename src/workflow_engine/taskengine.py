"""面向调用方的统一任务引擎入口。

``TaskEngine`` 只负责装配既有原语并转发请求：任务仍由 ``Task`` 描述，
队列由 ``TaskQueue`` 保存，生命周期由执行器更新，失败重试由
``RetryPolicy`` 决定，超时由 ``TimeoutExecutor`` 处理，而实际消费仍由
``Worker`` 或 ``ConcurrentWorker`` 完成。此模块不新增业务状态机或调度
策略，因此是系统的组装边界，而不是新的业务实现层。
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Callable, Any
from threading import RLock

from .concurrency import ConcurrentWorker
from .queue import TaskQueue
from .retry import RetryPolicy
from .scheduler import Scheduler
from .task import Task
from .timeout import TimeoutExecutor, TimeoutPolicy
from .worker import Worker
from .persistence import SQLiteTaskStore, PersistentExecutor
from .task import TaskStatus


class TaskEngine:
    """组合现有执行原语的统一入口。

    ``max_workers=1`` 使用同步 ``Worker``；更大的值使用固定线程池
    ``ConcurrentWorker``。无论是否配置默认超时策略，均使用
    ``TimeoutExecutor``，从而任务自身设置的 ``Task.timeout`` 总会生效；未
    设置任何超时时，它的行为与普通 ``Executor`` 一致。
    """

    def __init__(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        timeout_policy: TimeoutPolicy | None = None,
        max_workers: int = 1,
        database_path: str | Path | None = None,
        task_registry: dict[str, Callable[..., Any]] | None = None,
        recover_interrupted: bool = False,
    ) -> None:
        if not isinstance(recover_interrupted, bool):
            raise TypeError("recover_interrupted 必须是布尔值")
        if retry_policy is not None and not isinstance(retry_policy, RetryPolicy):
            raise TypeError("retry_policy 必须是 RetryPolicy 实例或 None")
        if timeout_policy is not None and not isinstance(timeout_policy, TimeoutPolicy):
            raise TypeError("timeout_policy 必须是 TimeoutPolicy 实例或 None")
        if not isinstance(max_workers, int) or isinstance(max_workers, bool):
            raise TypeError("max_workers 必须是正整数")
        if max_workers <= 0:
            raise ValueError("max_workers 必须是正整数")

        queue = TaskQueue()
        self._store = None
        self._submission_lock = RLock()
        self._retry_policy = retry_policy
        if database_path is not None:
            if task_registry is None:
                raise ValueError("持久化模式需要 task_registry")
            self._store = SQLiteTaskStore(database_path, task_registry)
            if recover_interrupted:
                self._store.recover_interrupted()
            for task in self._store.pending():
                queue.enqueue(task)
            executor = PersistentExecutor(self._store, retry_policy, timeout_policy)
            # PersistentExecutor commits the retry decision with the outcome.
            retry_policy = None
        else:
            if task_registry is not None or recover_interrupted:
                raise ValueError("task_registry/recover_interrupted 需要 database_path")
            executor = TimeoutExecutor(timeout_policy)
        if max_workers == 1:
            worker: Worker = Worker(queue, executor, retry_policy)
        else:
            worker = ConcurrentWorker(
                queue,
                executor,
                max_workers=max_workers,
                retry_policy=retry_policy,
            )

        self._scheduler = Scheduler(queue, worker)
        self._queue = queue

    def submit(self, task: Task) -> Task:
        """提交一个任务，并返回原任务以便调用方继续追踪其状态。

        该方法不执行任务；调用 :meth:`start` 才会进入既有 Worker 流程。
        """
        if self._store is None:
            self._scheduler.submit(task)
            return task
        with self._submission_lock:
            limit = self._retry_policy.limit_for(task) if self._retry_policy is not None else 0
            stored = self._store.submit(task, limit)
            if stored.status in (TaskStatus.PENDING, TaskStatus.RETRYING) and not self._queue.contains(stored):
                self._scheduler.submit(stored)
            return stored

    def get_task(self, task_id: str) -> Task | None:
        """读取已持久化任务的当前状态、结果和计数。"""
        if self._store is None:
            raise RuntimeError("get_task 需要持久化模式")
        return self._store.get(task_id)

    def submit_many(self, tasks: Iterable[Task]) -> list[Task]:
        """按迭代顺序提交多个任务，返回相同的任务对象列表。"""
        return [self.submit(task) for task in tasks]

    def start(self) -> list[Task]:
        """处理当前已提交的任务，并返回每一次尝试的任务对象。"""
        return self._scheduler.start()

    @property
    def pending_count(self) -> int:
        """返回当前尚未被 Worker 取走的任务数量。"""
        return self._queue.size()
