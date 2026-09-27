"""Worker 生命周期状态。"""

from __future__ import annotations

from enum import Enum


class WorkerStatus(str, Enum):
    """Worker 的可观察生命周期状态。

    ``STOPPED`` 是终态；一个 Worker 实例只支持一次 ``run()``。若需要再次
    消费队列，应创建新的 Worker 实例。
    """

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
