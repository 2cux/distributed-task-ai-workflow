"""SQLite task journal. No pickle, dynamic imports, or automatic live-worker takeover."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import threading
from typing import Callable, Any
import uuid

from .executor import EXECUTABLE_STATUSES
from .retry import RetryPolicy
from .task import Task, TaskStatus
from .timeout import TimeoutExecutor, TimeoutPolicy, TaskTimeoutError, run_with_timeout


class PersistenceError(RuntimeError):
    """Task data cannot be persisted, or a stale execution tried to commit."""


class IdempotencyConflict(PersistenceError):
    """An existing task ID was submitted with a different task definition."""


class UncertainExecutionError(PersistenceError):
    """Execution was interrupted; business effects must be reconciled before replay."""


class StoredTaskError(RuntimeError):
    """Portable representation of an exception from an earlier process."""


def _json(value: Any) -> str:
    # Reject silently lossy conversions (e.g. integer dictionary keys, tuples).
    def validate(item: Any) -> None:
        if item is None or type(item) in (str, int, bool, float):
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                validate(child)
            return
        raise PersistenceError("持久化数据必须是 JSON 类型（字典键必须是字符串）")
    try:
        validate(value)
        return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)
    except (ValueError, TypeError, RecursionError) as error:
        raise PersistenceError("任务数据不能安全序列化为 JSON") from error


class SQLiteTaskStore:
    """Durable definitions, results and attempt counters; one short transaction per write.

    Registry names are stable deployment contracts and must retain the same meaning
    across restarts. Each operation owns its connection, including concurrent workers.
    """

    def __init__(self, path: str | Path, registry: dict[str, Callable[..., Any]]) -> None:
        if str(path) == ":memory:":
            raise ValueError("持久化需要磁盘数据库路径")
        if not isinstance(registry, dict) or not all(
            isinstance(key, str) and key and callable(value) for key, value in registry.items()
        ):
            raise TypeError("registry 必须将非空名称映射到可调用对象")
        self.path = str(Path(path).resolve())
        self.registry = dict(registry)
        with self._connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                definition TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                result TEXT,
                error TEXT,
                last_error TEXT,
                owner TEXT,
                retry_limit INTEGER NOT NULL
            )""")

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
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

    def _definition(self, task: Task) -> str:
        names = [name for name, func in self.registry.items() if func is task.callable]
        if len(names) != 1:
            raise PersistenceError("持久化任务的 callable 必须在 registry 中以唯一名称注册")
        return _json(dict(name=task.name, handler=names[0], args=list(task.args),
                          kwargs=task.kwargs, priority=task.priority,
                          max_retries=task.max_retries, timeout=task.timeout,
                          idempotent=task.idempotent))

    def submit(self, task: Task, retry_limit: int = 0) -> Task:
        if not isinstance(task, Task):
            raise TypeError("task 必须是 Task 实例")
        if type(retry_limit) is not int or retry_limit < 0:
            raise ValueError("retry_limit 必须是非负整数")
        if task.max_retries is not None:
            retry_limit = min(retry_limit, task.max_retries)
        definition = self._definition(task)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task.id,)).fetchone()
            if row is not None:
                if row["definition"] != definition:
                    raise IdempotencyConflict(f"任务 {task.id!r} 的定义与已提交任务不同")
                return self._decode(row)
            if task.status is not TaskStatus.PENDING or task.attempt_count or task.retry_count:
                raise ValueError("首次持久化提交只接受尚未执行的 PENDING 任务")
            db.execute("INSERT INTO tasks(id, definition, status, retry_limit) VALUES(?,?,?,?)",
                       (task.id, definition, TaskStatus.PENDING.value, retry_limit))
        return task

    def _decode(self, row) -> Task:
        definition = json.loads(row["definition"])
        handler = definition.pop("handler")
        try:
            func = self.registry[handler]
        except KeyError as error:
            raise PersistenceError(f"恢复任务缺少注册函数 {handler!r}") from error
        definition["args"] = tuple(definition["args"])
        task = Task(id=row["id"], callable=func, status=TaskStatus(row["status"]),
                    retry_count=row["retry_count"], **definition)
        task._attempt_count = row["attempt_count"]
        task._persisted_retry_limit = row["retry_limit"]
        task.result = json.loads(row["result"]) if row["result"] is not None else None
        for field in ("error", "last_error"):
            if row[field] is not None:
                data = json.loads(row[field])
                cls = UncertainExecutionError if data["type"] == "UncertainExecutionError" else StoredTaskError
                setattr(task, field, cls(f"{data['type']}: {data['message']}"))
        return task

    def get(self, task_id: str) -> Task | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            return None if row is None else self._decode(row)

    def pending(self) -> list[Task]:
        with self._connection() as db:
            rows = db.execute("SELECT * FROM tasks WHERE status IN ('PENDING','RETRYING') ORDER BY sequence").fetchall()
            return [self._decode(row) for row in rows]

    def claim(self, task: Task, owner: str) -> Task | None:
        if task.status not in EXECUTABLE_STATUSES:
            raise ValueError("只能领取 PENDING 或 RETRYING 任务")
        with self._transaction() as db:
            changed = db.execute("""UPDATE tasks SET status='RUNNING', owner=?,
                attempt_count=attempt_count+1, result=NULL, error=NULL
                WHERE id=? AND status=? AND attempt_count=? AND retry_count=?""",
                (owner, task.id, task.status.value, task.attempt_count, task.retry_count)).rowcount
            if not changed:
                return None
            return self._decode(db.execute("SELECT * FROM tasks WHERE id=?", (task.id,)).fetchone())

    @staticmethod
    def _error(error: BaseException | None) -> str | None:
        if error is None:
            return None
        return _json({"type": type(error).__name__, "message": str(error)})

    def finish(self, task: Task, owner: str) -> None:
        if task.status not in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.RETRYING):
            raise ValueError("只能提交终态或已安排的重试")
        result = _json(task.result)
        with self._transaction() as db:
            changed = db.execute("""UPDATE tasks SET status=?, retry_count=?, result=?,
                error=?, last_error=?, owner=NULL, sequence=(SELECT MAX(sequence)+1 FROM tasks)
                WHERE id=? AND status='RUNNING' AND owner=? AND attempt_count=?""",
                (task.status.value, task.retry_count, result, self._error(task.error),
                 self._error(task.last_error), task.id, owner, task.attempt_count)).rowcount
            if not changed:
                raise PersistenceError("执行所有权已失效，拒绝旧执行器覆盖任务")

    def recover_interrupted(self) -> list[Task]:
        """Call ONLY after all previous workers using this database have stopped.

        No lease expiry guesses: a slow live worker must never be replayed. Unsafe
        interrupted tasks are quarantined; idempotent ones consume a retry budget.
        """
        recovered = []
        with self._transaction() as db:
            rows = db.execute("SELECT * FROM tasks WHERE status='RUNNING' ORDER BY sequence").fetchall()
            for row in rows:
                task = self._decode(row)
                task.error = UncertainExecutionError("进程中断，业务执行结果不确定")
                task.status = TaskStatus.FAILED
                if task.idempotent and task.retry_count < row["retry_limit"]:
                    task.last_error = task.error
                    task.mark_retrying()
                db.execute("""UPDATE tasks SET status=?, retry_count=?, error=?, last_error=?,
                    owner=NULL, sequence=(SELECT MAX(sequence)+1 FROM tasks) WHERE id=?""", (task.status.value, task.retry_count,
                    self._error(task.error), self._error(task.last_error), task.id))
                recovered.append(task)
        return recovered


class PersistentExecutor(TimeoutExecutor):
    """Claim before business code; atomically commit outcome and retry decision."""

    def __init__(self, store: SQLiteTaskStore, retry_policy: RetryPolicy | None,
                 timeout_policy: TimeoutPolicy | None) -> None:
        super().__init__(timeout_policy)
        self.store = store
        self.retry_policy = retry_policy

    def execute(self, task: Task) -> Task:
        with task._lock:
            if task.status not in EXECUTABLE_STATUSES:
                raise ValueError("只能领取待执行任务")
            owner = uuid.uuid4().hex
            claimed = self.store.claim(task, owner)
            if claimed is None:
                # A different engine owns this task or already committed it.
                # Do not mirror RETRYING into this stale queue: only its owner enqueues.
                return self.store.get(task.id) or task
            task.status = TaskStatus.RUNNING
            # The committed definition is authoritative even if the caller mutated
            # its in-memory Task after submission.
            for field in ("name", "callable", "args", "kwargs", "priority", "max_retries",
                          "timeout", "idempotent", "last_error"):
                setattr(task, field, getattr(claimed, field))
            task._attempt_count = claimed.attempt_count
            task.retry_count = claimed.retry_count
            task.result = None
            task.error = None
            try:
                task.result = self._invoke(task)
                _json(task.result)
            except BaseException as error:
                task.result = None
                task.error = error
                task.status = TaskStatus.FAILED
                policy = RetryPolicy(claimed._persisted_retry_limit)
                if (not isinstance(error, PersistenceError)
                        and task.retry_count < claimed._persisted_retry_limit
                        and policy.should_retry(task)):
                    policy.begin_retry(task)
            else:
                task.status = TaskStatus.SUCCESS
            self.store.finish(task, owner)
        return task

    def _invoke(self, task: Task) -> Any:
        # Soft timeout cannot kill Python threads. Wait for the old invocation to
        # stop before committing failure/retrying so its business code cannot overlap.
        finished = threading.Event()
        def invoke():
            try:
                return task.callable(*task.args, **task.kwargs)
            finally:
                finished.set()
        try:
            return run_with_timeout(invoke, self.timeout_for(task), task_name=task.name)
        except TaskTimeoutError:
            finished.wait()
            raise
