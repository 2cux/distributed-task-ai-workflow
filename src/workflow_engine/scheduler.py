"""最小任务调度入口。

``Scheduler`` 是任务提交方与执行方之间的边界：它接收任务并将任务放入
队列，再显式请求 Worker 开始处理当前队列。它不执行任务函数、不管理任务
状态，也不决定优先级、重试、并发或跨进程分配；这些能力属于 Worker、
Executor 或后续阶段的组件。
"""

from __future__ import annotations

from .queue import TaskQueue
from .task import Task
from .worker import Worker


class Scheduler:
    """接收任务并启动一个 Worker 的最小调度协调者。"""

    def __init__(self, queue: TaskQueue, worker: Worker) -> None:
        if not isinstance(queue, TaskQueue):
            raise TypeError("queue 必须是 TaskQueue 实例")
        if not isinstance(worker, Worker):
            raise TypeError("worker 必须是 Worker 实例")
        if worker._queue is not queue:
            raise ValueError("worker 必须消费 scheduler 使用的 queue")

        self._queue = queue
        self._worker = worker

    def submit(self, task: Task) -> None:
        """接收一个待执行任务并交由队列保存。

        提交本身不执行任务；调用 :meth:`start` 后才会由 Worker 处理。
        """
        self._queue.enqueue(task)

    def start(self) -> list[Task]:
        """请求 Worker 同步处理当前队列中的全部任务。"""
        return self._worker.run()
