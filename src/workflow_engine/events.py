"""Immutable, ordered snapshots of a task's lifecycle (UTC timestamps)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .task import Task, TaskStatus


class TaskEventType(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    INTERRUPTED = "INTERRUPTED"
    RESTORED = "RESTORED"


@dataclass(frozen=True)
class TaskEvent:
    task_id: str
    sequence: int
    event_type: TaskEventType
    timestamp: str
    previous_status: TaskStatus | None
    status: TaskStatus
    attempt_count: int
    retry_count: int
    # Store JSON rather than a mutable dict; callers receive a detached copy.
    _details_json: str = field(default="{}", repr=False)

    @property
    def details(self) -> dict[str, Any]:
        return json.loads(self._details_json)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def snapshot_details(task: Task) -> dict[str, Any]:
    details: dict[str, Any] = {}
    if task.error is not None:
        try:
            message = str(task.error)
        except BaseException:
            message = "<unrepresentable error>"
        details.update(error_type=type(task.error).__name__, error_message=message)
        if isinstance(task.error, TimeoutError) and hasattr(task.error, "elapsed"):
            details.update(timeout=task.error.timeout, elapsed=task.error.elapsed)
    if task.status.value == "SUCCESS":
        try:
            details["result_repr"] = repr(task.result)
        except BaseException:
            details["result_repr"] = "<unrepresentable result>"
    return details
