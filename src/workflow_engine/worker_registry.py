"""Persistent Worker identity and heartbeat observations.

This module does not claim tasks, fence running external side effects, or
perform automatic recovery. An overdue heartbeat means *suspected* failure.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
from pathlib import Path
import sqlite3
import time
from collections.abc import Callable
from uuid import uuid4


class WorkerRegistryError(RuntimeError):
    """Base class for registry state and ownership errors."""


class WorkerAlreadyActive(WorkerRegistryError):
    """A worker ID still has an active session."""


class WorkerSessionMismatch(WorkerRegistryError):
    """A missing or superseded session attempted to change registry state."""


@dataclass(frozen=True)
class WorkerRecord:
    worker_id: str
    session_id: str
    status: str
    registered_at: float
    last_heartbeat_at: float
    stopped_at: float | None


class WorkerRegistry:
    """SQLite-backed Worker registration, heartbeats and observation only.

    Use the task store's *same* SQLite path. Each registration receives a new
    session ID, so callbacks from an earlier stopped session cannot update the
    record after that worker ID is reused. No heartbeat timeout takes over a
    live task or changes its owner.
    """

    def __init__(self, database_path: str | Path, *, clock: Callable[[], float] = time.time) -> None:
        if str(database_path) == ":memory:":
            raise ValueError("WorkerRegistry 需要磁盘数据库路径")
        if not callable(clock):
            raise TypeError("clock 必须可调用")
        self.database_path = str(Path(database_path).resolve())
        self._clock = clock
        with self._connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS worker_registry (
                worker_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'STOPPED')),
                registered_at REAL NOT NULL,
                last_heartbeat_at REAL NOT NULL,
                stopped_at REAL
            )""")

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self):
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise

    def _now(self) -> float:
        value = self._clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("clock 必须返回有限非负 Unix 时间戳")
        return float(value)

    @staticmethod
    def _id(value: str, name: str) -> str:
        if not isinstance(value, str) or not value or not value.strip():
            raise ValueError(f"{name} 必须是非空字符串")
        return value

    @staticmethod
    def _record(row: sqlite3.Row) -> WorkerRecord:
        return WorkerRecord(**dict(row))

    def register(self, worker_id: str) -> WorkerRecord:
        """Start a session; an ACTIVE ID must be explicitly stopped first.

        Do not infer stop or safety to resume tasks from a missed heartbeat.
        """
        self._id(worker_id, "worker_id")
        now = self._now()
        session_id = uuid4().hex
        with self._transaction() as db:
            old = db.execute("SELECT status FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
            if old is not None and old["status"] == "ACTIVE":
                raise WorkerAlreadyActive(f"Worker {worker_id!r} 仍处于 ACTIVE 状态")
            if old is None:
                db.execute("""INSERT INTO worker_registry
                    (worker_id, session_id, status, registered_at, last_heartbeat_at, stopped_at)
                    VALUES (?, ?, 'ACTIVE', ?, ?, NULL)""", (worker_id, session_id, now, now))
            else:
                db.execute("""UPDATE worker_registry SET session_id=?, status='ACTIVE',
                    registered_at=?, last_heartbeat_at=?, stopped_at=NULL WHERE worker_id=?""",
                    (session_id, now, now, worker_id))
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
        return self._record(row)

    def heartbeat(self, worker_id: str, session_id: str) -> WorkerRecord:
        """Update an ACTIVE session; reject old and unknown session tokens."""
        self._id(worker_id, "worker_id")
        self._id(session_id, "session_id")
        now = self._now()
        with self._transaction() as db:
            updated = db.execute("""UPDATE worker_registry
                SET last_heartbeat_at=MAX(last_heartbeat_at, ?)
                WHERE worker_id=? AND session_id=? AND status='ACTIVE'""",
                (now, worker_id, session_id)).rowcount
            if updated != 1:
                raise WorkerSessionMismatch("Worker 会话不存在、已停止或已被替换")
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
        return self._record(row)

    def stop(self, worker_id: str, session_id: str) -> WorkerRecord:
        """Mark a matching ACTIVE session as stopped (never stop another session)."""
        self._id(worker_id, "worker_id")
        self._id(session_id, "session_id")
        now = self._now()
        with self._transaction() as db:
            updated = db.execute("""UPDATE worker_registry SET status='STOPPED', stopped_at=?,
                last_heartbeat_at=MAX(last_heartbeat_at, ?)
                WHERE worker_id=? AND session_id=? AND status='ACTIVE'""",
                (now, now, worker_id, session_id)).rowcount
            if updated != 1:
                raise WorkerSessionMismatch("Worker 会话不存在、已停止或已被替换")
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
        return self._record(row)

    def get(self, worker_id: str) -> WorkerRecord | None:
        self._id(worker_id, "worker_id")
        with self._connection() as db:
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
        return self._record(row) if row is not None else None

    def list_workers(self) -> list[WorkerRecord]:
        with self._connection() as db:
            rows = db.execute("SELECT * FROM worker_registry ORDER BY worker_id").fetchall()
        return [self._record(row) for row in rows]

    def is_overdue(self, worker_id: str, timeout_seconds: float) -> bool:
        """Observation only: overdue is not permission to replay owned tasks."""
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds 必须是有限正数")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds 必须是有限正数")
        record = self.get(worker_id)
        if record is None:
            raise KeyError(worker_id)
        return record.status == "ACTIVE" and self._now() - record.last_heartbeat_at > timeout_seconds
