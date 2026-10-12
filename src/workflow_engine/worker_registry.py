"""Persistent worker identities and staged suspicion, fencing and drain confirmation.

Fencing revokes engine writes, not external side effects. Heartbeat expiry never
proves that a callable stopped and therefore never authorizes task replay.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
from pathlib import Path
import sqlite3
import socket
import time
from collections.abc import Callable
from uuid import uuid4


class WorkerRegistryError(RuntimeError):
    """Base class for registry state and ownership errors."""


class WorkerAlreadyActive(WorkerRegistryError):
    """A worker ID still has an active session."""


class WorkerSessionMismatch(WorkerRegistryError):
    """A missing or superseded session attempted to change registry state."""


class WorkerNotReady(WorkerRegistryError):
    """A suspected or overdue worker may not claim new work until heartbeat resumes."""


@dataclass(frozen=True)
class WorkerRecord:
    worker_id: str
    session_id: str
    status: str
    registered_at: float
    last_heartbeat_at: float
    stopped_at: float | None


@dataclass(frozen=True)
class WorkerSessionRecord:
    worker_id: str
    session_id: str
    state: str
    last_heartbeat_at: float
    heartbeat_timeout: float
    stopped_evidence: str | None
    stop_requested: int = 0
    host_id: str = 'legacy'


class WorkerRegistry:
    """SQLite-backed registration, session history and recovery coordination.

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
            db.execute("""CREATE TABLE IF NOT EXISTS worker_sessions (
                session_id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('ACTIVE','SUSPECT','FENCED','STOPPING','STOPPED')),
                last_heartbeat_at REAL NOT NULL,
                heartbeat_timeout REAL NOT NULL,
                stopped_evidence TEXT,
                stop_requested INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS worker_session_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                state TEXT NOT NULL,
                timestamp REAL NOT NULL,
                reason TEXT NOT NULL
            )""")
        with self._transaction() as db:
            if 'host_id' not in {row[1] for row in db.execute('PRAGMA table_info(worker_sessions)')}:
                db.execute("ALTER TABLE worker_sessions ADD COLUMN host_id TEXT NOT NULL DEFAULT 'legacy'")
        # Upgrade PR #1 databases without replacing their worker identity table.
        with self._transaction() as db:
            db.execute("""INSERT OR IGNORE INTO worker_sessions
                (session_id,worker_id,state,last_heartbeat_at,heartbeat_timeout,stopped_evidence)
                SELECT session_id, worker_id, status, last_heartbeat_at, 30, NULL
                FROM worker_registry""")

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

    def register(self, worker_id: str, *, heartbeat_timeout: float = 30,
                 host_id: str | None = None) -> WorkerRecord:
        """Start a session; an ACTIVE ID must be explicitly stopped first.

        Do not infer stop or safety to resume tasks from a missed heartbeat.
        """
        self._id(worker_id, "worker_id")
        host_id = socket.gethostname() if host_id is None else self._id(host_id, 'host_id')
        self._seconds(heartbeat_timeout, "heartbeat_timeout")
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
            db.execute("""INSERT INTO worker_sessions
                (session_id,worker_id,state,last_heartbeat_at,heartbeat_timeout,stopped_evidence,host_id)
                VALUES (?,?,'ACTIVE',?,?,NULL,?)""",
                       (session_id, worker_id, now, heartbeat_timeout, host_id))
            self._event(db, session_id, "ACTIVE", "registered")
        return self._record(row)

    def heartbeat(self, worker_id: str, session_id: str) -> WorkerRecord:
        """Refresh a live/draining session; a fenced session cannot resurrect itself."""
        self._id(worker_id, "worker_id")
        self._id(session_id, "session_id")
        now = self._now()
        with self._transaction() as db:
            session = self._session(db, worker_id, session_id)
            if session["state"] not in ('ACTIVE', 'SUSPECT', 'STOPPING'):
                raise WorkerSessionMismatch("Worker 会话已撤权或停止")
            updated = db.execute("""UPDATE worker_registry
                SET last_heartbeat_at=MAX(last_heartbeat_at, ?)
                WHERE worker_id=? AND session_id=? AND status='ACTIVE'""",
                (now, worker_id, session_id)).rowcount
            if updated != 1:
                raise WorkerSessionMismatch("Worker 会话不存在、已停止或已被替换")
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
            state = 'STOPPING' if session['stop_requested'] else 'ACTIVE'
            db.execute("UPDATE worker_sessions SET last_heartbeat_at=MAX(last_heartbeat_at,?), state=? WHERE session_id=?",
                       (now, state, session_id))
            if session['state'] == 'SUSPECT':
                self._event(db, session_id, state, "heartbeat resumed")
        return self._record(row)

    def stop(self, worker_id: str, session_id: str) -> WorkerRecord:
        """Mark a matching ACTIVE session as stopped (never stop another session)."""
        self._id(worker_id, "worker_id")
        self._id(session_id, "session_id")
        now = self._now()
        with self._transaction() as db:
            session = self._session(db, worker_id, session_id)
            if session['state'] == 'FENCED' or self._has_running(db, session_id):
                raise WorkerRegistryError("必须先停止实际调用，再使用 confirm_stopped 确认停止")
            updated = db.execute("""UPDATE worker_registry SET status='STOPPED', stopped_at=?,
                last_heartbeat_at=MAX(last_heartbeat_at, ?)
                WHERE worker_id=? AND session_id=? AND status='ACTIVE'""",
                (now, now, worker_id, session_id)).rowcount
            if updated != 1:
                raise WorkerSessionMismatch("Worker 会话不存在、已停止或已被替换")
            row = db.execute("SELECT * FROM worker_registry WHERE worker_id=?", (worker_id,)).fetchone()
            db.execute("UPDATE worker_sessions SET state='STOPPED', stopped_evidence='graceful stop' WHERE session_id=?",
                       (session_id,))
            self._event(db, session_id, 'STOPPED', 'graceful stop')
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

    @staticmethod
    def _seconds(value: float, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} 必须是有限正数")
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} 必须是有限正数")

    def _event(self, db, session_id: str, state: str, reason: str) -> None:
        db.execute("INSERT INTO worker_session_events(session_id,state,timestamp,reason) VALUES (?,?,?,?)",
                   (session_id, state, self._now(), reason))

    def _session(self, db, worker_id: str, session_id: str):
        row = db.execute("""SELECT s.* FROM worker_sessions s JOIN worker_registry r
            ON r.worker_id=s.worker_id AND r.session_id=s.session_id
            WHERE s.worker_id=? AND s.session_id=?""", (worker_id, session_id)).fetchone()
        if row is None:
            raise WorkerSessionMismatch("Worker 会话不存在或已被替换")
        return row

    @staticmethod
    def _has_running(db, session_id: str) -> bool:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='tasks'").fetchone()
        if not exists or 'worker_session' not in {row[1] for row in db.execute('PRAGMA table_info(tasks)')}:
            return False
        return db.execute("SELECT 1 FROM tasks WHERE status='RUNNING' AND worker_session=?",
                          (session_id,)).fetchone() is not None

    def _require_claimable(self, db, session_id: str) -> None:
        row = db.execute("SELECT * FROM worker_sessions WHERE session_id=?", (session_id,)).fetchone()
        if row is None:
            raise WorkerSessionMismatch("未知 worker 会话")
        self._session(db, row['worker_id'], session_id)
        if row['state'] == 'SUSPECT' or (row['state'] == 'ACTIVE' and
                self._now() - row['last_heartbeat_at'] > row['heartbeat_timeout']):
            raise WorkerNotReady("心跳过期或疑似失联，暂停领取")
        if row['state'] != 'ACTIVE':
            raise WorkerSessionMismatch("Worker 已停止领取或已撤权")

    def get_session(self, session_id: str) -> WorkerSessionRecord | None:
        self._id(session_id, 'session_id')
        with self._connection() as db:
            row = db.execute('SELECT * FROM worker_sessions WHERE session_id=?', (session_id,)).fetchone()
        return WorkerSessionRecord(**dict(row)) if row else None

    def list_sessions(self) -> list[WorkerSessionRecord]:
        """Include prior incarnations, so reused IDs never hide unresolved tasks."""
        with self._connection() as db:
            rows = db.execute('SELECT * FROM worker_sessions ORDER BY worker_id, rowid').fetchall()
        return [WorkerSessionRecord(**dict(row)) for row in rows]

    def get_session_events(self, session_id: str) -> list[dict]:
        with self._connection() as db:
            rows = db.execute('SELECT * FROM worker_session_events WHERE session_id=? ORDER BY sequence',
                              (session_id,)).fetchall()
        return [dict(row) for row in rows]

    def detect_overdue(self) -> list[WorkerSessionRecord]:
        """Phase 1: mark suspicion only; retain running attempts and their tokens."""
        with self._transaction() as db:
            now = self._now()
            rows = db.execute("""SELECT * FROM worker_sessions WHERE state IN ('ACTIVE','STOPPING')
                AND last_heartbeat_at+heartbeat_timeout<?""", (now,)).fetchall()
            for row in rows:
                db.execute("UPDATE worker_sessions SET state='SUSPECT' WHERE session_id=?", (row['session_id'],))
                self._event(db, row['session_id'], 'SUSPECT', 'heartbeat overdue; execution may still be alive')
            return [WorkerSessionRecord(**{**dict(row), 'state': 'SUSPECT'}) for row in rows]

    def fence(self, worker_id: str, session_id: str) -> WorkerSessionRecord:
        """Phase 2: revoke claims and result commits, without declaring execution stopped."""
        with self._transaction() as db:
            row = self._session(db, worker_id, session_id)
            if row['state'] not in ('SUSPECT', 'FENCED'):
                raise WorkerRegistryError("只有疑似失联会话可以撤权；恢复的心跳会取消此次撤权")
            if row['state'] != 'FENCED':
                db.execute("UPDATE worker_sessions SET state='FENCED' WHERE session_id=?", (session_id,))
                self._event(db, session_id, 'FENCED', 'claims and result commits revoked')
            return WorkerSessionRecord(**{**dict(row), 'state': 'FENCED'})

    def request_stop(self, worker_id: str, session_id: str) -> None:
        """Stop new claims; allow the current attempt to commit while draining."""
        with self._transaction() as db:
            row = self._session(db, worker_id, session_id)
            if row['state'] in ('ACTIVE', 'SUSPECT'):
                db.execute("UPDATE worker_sessions SET state='STOPPING', stop_requested=1 WHERE session_id=?", (session_id,))
                self._event(db, session_id, 'STOPPING', 'graceful stop requested')

    def confirm_stopped(self, worker_id: str, session_id: str, *, evidence: str) -> WorkerSessionRecord:
        """Phase 3: trusted supervisor attests ALL old business invocations have stopped.

        Evidence must come from process exit/join or a runtime drain acknowledgement,
        never from another timeout. Fencing SQLite writes cannot kill external effects.
        """
        self._id(evidence, 'evidence')
        with self._transaction() as db:
            row = self._session(db, worker_id, session_id)
            if row['state'] not in ('FENCED', 'STOPPING', 'STOPPED'):
                raise WorkerRegistryError("必须先撤权或请求停止，再确认旧调用已退出")
            if row['state'] != 'STOPPED':
                db.execute("UPDATE worker_sessions SET state='STOPPED', stopped_evidence=? WHERE session_id=?",
                           (evidence, session_id))
                db.execute("UPDATE worker_registry SET status='STOPPED', stopped_at=? WHERE worker_id=? AND session_id=?",
                           (self._now(), worker_id, session_id))
                self._event(db, session_id, 'STOPPED', evidence)
            updated = db.execute('SELECT * FROM worker_sessions WHERE session_id=?', (session_id,)).fetchone()
            return WorkerSessionRecord(**dict(updated))

