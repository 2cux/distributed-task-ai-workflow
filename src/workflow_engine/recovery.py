"""Durable recovery control plane, independent of business execution.

SQLite transactions elect each action, rather than electing a long-lived leader.
Heartbeats authorize suspicion/fencing only. Replay requires a trusted supervisor
to attest that the exact old session's invocations have stopped.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from threading import Event, RLock, Thread, current_thread
from uuid import uuid4

from .events import TaskEventType
from .persistence import SQLiteTaskStore, PersistenceError, _json
from .reliable_worker import _join_timeout
from .retry import RetryPolicy
from .task import TaskStatus
from .worker_registry import WorkerSessionRecord


class RecoveryConflict(PersistenceError):
    """A stale decision or unsafe replay was rejected without changing state."""


@dataclass(frozen=True)
class RecoveryIncident:
    id: str
    kind: str
    task_id: str
    session_id: str | None
    workflow_id: str | None
    host_id: str | None
    state: str
    attempt_count: int
    reason: str
    created_at: float
    updated_at: float
    resolution: str | None


@dataclass(frozen=True)
class RecoveryReport:
    recovered_task_ids: tuple[str, ...] = ()
    workflow_ids: tuple[str, ...] = ()
    incident_ids: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


class RecoveryJournal:
    """Schema shared by every engine, including ones without a running coordinator."""

    def __init__(self, store):
        with store._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS recovery_incidents (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, task_id TEXT NOT NULL,
                session_id TEXT, workflow_id TEXT, host_id TEXT, state TEXT NOT NULL,
                attempt_count INTEGER NOT NULL, reason TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL, resolution TEXT,
                UNIQUE(task_id, attempt_count))""")
            db.execute("""CREATE TABLE IF NOT EXISTS recovery_holds (
                scope TEXT NOT NULL, target_id TEXT NOT NULL, source TEXT NOT NULL,
                PRIMARY KEY(scope, target_id, source))""")
            db.execute("""CREATE TABLE IF NOT EXISTS recovery_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL NOT NULL,
                action TEXT NOT NULL, target_id TEXT NOT NULL, operator TEXT NOT NULL,
                details TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS recovery_incidents_state ON recovery_incidents(state)")

    @staticmethod
    def retry_scheduled(store, db, task_id, attempt_count):
        """Keep legacy recovery entry points consistent with coordinator holds."""
        incident = db.execute("""SELECT * FROM recovery_incidents WHERE task_id=?
            AND attempt_count=? AND state='OPEN'""", (task_id, attempt_count)).fetchone()
        if incident:
            now = store.workers._now()
            db.execute("""UPDATE recovery_incidents SET state='RESOLVED',resolution='automatic retry scheduled',
                updated_at=? WHERE id=?""", (now, incident['id']))
            db.execute('DELETE FROM recovery_holds WHERE source=?', (incident['id'],))
            db.execute("""INSERT INTO recovery_events(timestamp,action,target_id,operator,details)
                VALUES(?,'INCIDENT_RESOLVED',?,'coordinator',?)""",
                (now, incident['id'], _json(dict(resolution='automatic retry scheduled'))))


# Used at BOTH selection and atomic claim boundaries; stale local queues cannot
# bypass a pause written by another process.
CLAIM_GUARD = """NOT EXISTS (SELECT 1 FROM recovery_holds h WHERE
    (h.scope='task' AND h.target_id=tasks.id) OR
    (h.scope='workflow' AND EXISTS (SELECT 1 FROM workflow_nodes n
        WHERE n.task_id=tasks.id AND n.workflow_id=h.target_id)))"""


class RecoverySystem:
    """Repeatable scans, isolated recovery, durable operator decisions and DAG repair.

    stop_verifier(session) returns evidence ONLY after process join, runtime drain
    acknowledgement, or trusted host isolation that also stops external effects.
    None means unknown. It is called outside database locks and can be repeated by
    concurrent coordinators; it must be bounded and safe to repeat.
    """

    def __init__(self, store: SQLiteTaskStore, *, interval: float = 1,
                 stop_verifier=None, fence_after: float = 0):
        if not isinstance(store, SQLiteTaskStore):
            raise TypeError('store must be SQLiteTaskStore')
        store.workers._seconds(interval, 'interval')
        _join_timeout(fence_after)
        if fence_after is None:
            raise ValueError('fence_after must be finite nonnegative seconds')
        if stop_verifier is not None and not callable(stop_verifier):
            raise TypeError('stop_verifier must be callable or None')
        self.store = store
        self.interval = interval
        self.stop_verifier = stop_verifier
        self.fence_after = fence_after
        self.last_error: Exception | None = None
        self.last_report = RecoveryReport()
        self._stop = Event()
        self._lock = RLock()
        self._thread = None

    def _event(self, db, action, target, operator='coordinator', **details):
        db.execute("""INSERT INTO recovery_events(timestamp,action,target_id,operator,details)
            VALUES(?,?,?,?,?)""", (self.store.workers._now(), action, target, operator, _json(details)))

    def _incident(self, db, row, kind, reason):
        now = self.store.workers._now()
        existing = db.execute('SELECT * FROM recovery_incidents WHERE task_id=? AND attempt_count=?',
                              (row['id'], row['attempt_count'])).fetchone()
        if existing and existing['state'] == 'RESOLVED' and existing['resolution'] not in (
                'heartbeat resumed', 'handler restored', 'automatic retry scheduled', 'old invocation committed outcome'):
            return existing['id']
        node = db.execute('SELECT workflow_id FROM workflow_nodes WHERE task_id=?', (row['id'],)).fetchone()
        workflow_id = node['workflow_id'] if node else None
        session = db.execute('SELECT host_id FROM worker_sessions WHERE session_id=?',
                             (row['worker_session'],)).fetchone()
        incident_id = existing['id'] if existing else uuid4().hex
        if existing:
            db.execute("""UPDATE recovery_incidents SET kind=?,reason=?,state='OPEN',
                updated_at=?,resolution=NULL WHERE id=?""", (kind, reason, now, incident_id))
        else:
            db.execute("INSERT INTO recovery_incidents VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?,NULL)",
                       (incident_id, kind, row['id'], row['worker_session'], workflow_id,
                        session['host_id'] if session else None, row['attempt_count'], reason, now, now))
        db.execute("INSERT OR IGNORE INTO recovery_holds VALUES('task',?,?)", (row['id'], incident_id))
        if workflow_id:
            db.execute("INSERT OR IGNORE INTO recovery_holds VALUES('workflow',?,?)", (workflow_id, incident_id))
        if not existing or existing['state'] != 'OPEN' or existing['kind'] != kind or existing['reason'] != reason:
            self._event(db, 'INCIDENT_OPENED', incident_id, kind=kind, task_id=row['id'], reason=reason)
        return incident_id

    def _close(self, db, incident, resolution, operator='coordinator'):
        db.execute("UPDATE recovery_incidents SET state='RESOLVED',resolution=?,updated_at=? WHERE id=?",
                   (resolution, self.store.workers._now(), incident['id']))
        db.execute('DELETE FROM recovery_holds WHERE source=?', (incident['id'],))
        self._event(db, 'INCIDENT_RESOLVED', incident['id'], operator, resolution=resolution)

    def _recover(self, db, row):
        # A missing handler or bad stored data cannot roll back another task's recovery.
        db.execute('SAVEPOINT recover_one')
        try:
            task = self.store._recover_rows(db, [row])[0]
            db.execute('RELEASE recover_one')
        except Exception as error:
            db.execute('ROLLBACK TO recover_one')
            db.execute('RELEASE recover_one')
            self._incident(db, row, 'RECOVERY_ERROR', str(error))
            return False, str(error)
        incident = db.execute('SELECT * FROM recovery_incidents WHERE task_id=? AND attempt_count=?',
                              (row['id'], row['attempt_count'])).fetchone()
        if task.status is TaskStatus.RETRYING:
            if incident and incident['state'] == 'OPEN':
                self._close(db, incident, 'automatic retry scheduled')
        else:
            updated = db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone()
            # Preserve session/host provenance when converting WAIT_STOP to manual work.
            self._incident(db, updated, 'TASK_FAILURE', 'interrupted outcome uncertain or retry budget exhausted')
        self._event(db, 'TASK_RECOVERED', row['id'], session_id=row['worker_session'], status=task.status.value)
        return True, None

    def tick(self) -> RecoveryReport:
        """One scan; crash at any boundary is safe to repeat from another process."""
        registry = self.store.workers
        recovered, workflows, errors = [], [], []
        with self.store._transaction() as db:
            now = registry._now()
            overdue = db.execute("""SELECT * FROM worker_sessions WHERE state IN ('ACTIVE','STOPPING')
                AND last_heartbeat_at+heartbeat_timeout<?""", (now,)).fetchall()
            for session in overdue:
                db.execute("UPDATE worker_sessions SET state='SUSPECT' WHERE session_id=?", (session['session_id'],))
                registry._event(db, session['session_id'], 'SUSPECT', 'recovery coordinator: heartbeat overdue')
            suspected = db.execute("""SELECT * FROM worker_sessions WHERE state='SUSPECT'
                AND last_heartbeat_at+heartbeat_timeout+?<=?""", (self.fence_after, now)).fetchall()
            for session in suspected:
                db.execute("UPDATE worker_sessions SET state='FENCED' WHERE session_id=?", (session['session_id'],))
                registry._event(db, session['session_id'], 'FENCED', 'recovery coordinator revoked database writes')
            completed = db.execute("""SELECT i.* FROM recovery_incidents i JOIN tasks t ON t.id=i.task_id
                WHERE i.state='OPEN' AND i.kind='WAIT_STOP' AND t.status IN ('SUCCESS','RETRYING')
                AND t.attempt_count=i.attempt_count""").fetchall()
            for incident in completed:
                self._close(db, incident, 'old invocation committed outcome')
            for row in db.execute("SELECT * FROM tasks WHERE status IN ('PENDING','RETRYING')").fetchall():
                handler = json.loads(row['definition'])['handler']
                if handler not in self.store.registry:
                    reason = f'handler unavailable: {handler}'
                    self._incident(db, row, 'RECOVERY_ERROR', reason)
                    errors.append(f"{row['id']}: {reason}")
                else:
                    incident = db.execute("""SELECT * FROM recovery_incidents WHERE task_id=?
                        AND attempt_count=? AND state='OPEN' AND kind='RECOVERY_ERROR'
                        AND reason LIKE 'handler unavailable:%'""", (row['id'], row['attempt_count'])).fetchone()
                    if incident:
                        self._close(db, incident, 'handler restored')
            rows = db.execute("SELECT * FROM tasks WHERE status IN ('RUNNING','FAILED') ORDER BY sequence").fetchall()
            for row in rows:
                if row['status'] == 'FAILED':
                    self._incident(db, row, 'TASK_FAILURE', row['error'] or 'task failed')
                    continue
                session = db.execute('SELECT * FROM worker_sessions WHERE session_id=?',
                                     (row['worker_session'],)).fetchone()
                legacy_stopped = None
                if row['worker_session'] is None:
                    legacy_stopped = db.execute("""SELECT 1 FROM recovery_events WHERE action='LEGACY_STOP_CONFIRMED'
                        AND target_id=? AND json_extract(details,'$.attempt_count')=?""",
                        (row['id'], row['attempt_count'])).fetchone()
                if (session and session['state'] == 'STOPPED' and session['stopped_evidence']) or legacy_stopped:
                    # Journal provenance before _recover_rows clears ownership.
                    self._incident(db, row, 'WAIT_STOP', 'old execution stopped; recovery pending')
                    ok, error = self._recover(db, row)
                    if ok:
                        recovered.append(row['id'])
                    if error:
                        errors.append(f"{row['id']}: {error}")
                elif session is None or session['state'] in ('SUSPECT', 'FENCED', 'STOPPED'):
                    self._incident(db, row, 'WAIT_STOP', 'trusted confirmation of old invocation exit required')
                elif session['state'] in ('ACTIVE', 'STOPPING'):
                    incident = db.execute("""SELECT * FROM recovery_incidents WHERE task_id=?
                        AND attempt_count=? AND kind='WAIT_STOP' AND state='OPEN'""",
                        (row['id'], row['attempt_count'])).fetchone()
                    if incident:
                        self._close(db, incident, 'heartbeat resumed')
        # Do not hold SQLite's write lock while asking a process/host supervisor.
        if self.stop_verifier is not None:
            for session in registry.list_sessions():
                if session.state != 'FENCED':
                    continue
                try:
                    evidence = self.stop_verifier(session)
                    if evidence is not None:
                        self.confirm_session_stopped(session.session_id, operator='stop_verifier', evidence=evidence)
                except Exception as error:
                    errors.append(f"{session.session_id}: stop verifier: {error}")
                    with self.store._transaction() as db:
                        self._event(db, 'STOP_VERIFICATION_FAILED', session.session_id, error=str(error))
        # Cover confirmations during this scan and commits made before an earlier
        # coordinator died. Each task rechecks RUNNING under the write lock.
        with self.store._transaction() as db:
            rows = db.execute("""SELECT t.* FROM tasks t JOIN worker_sessions s ON t.worker_session=s.session_id
                WHERE t.status='RUNNING' AND s.state='STOPPED' AND s.stopped_evidence IS NOT NULL
                ORDER BY t.sequence""").fetchall()
            for row in rows:
                self._incident(db, row, 'WAIT_STOP', 'old execution stopped; recovery pending')
                ok, error = self._recover(db, row)
                if ok:
                    recovered.append(row['id'])
                if error:
                    errors.append(f"{row['id']}: {error}")
            ids = [row['id'] for row in db.execute("SELECT id FROM workflow_runs WHERE status!='SUCCESS' ORDER BY rowid")]
            for key in ids:
                db.execute('SAVEPOINT recover_workflow')
                try:
                    self.store.workflows._advance(db, key)
                    db.execute('RELEASE recover_workflow')
                    workflows.append(key)
                except Exception as error:
                    db.execute('ROLLBACK TO recover_workflow')
                    db.execute('RELEASE recover_workflow')
                    errors.append(f'{key}: workflow reconciliation: {error}')
                    self._event(db, 'WORKFLOW_RECOVERY_FAILED', key, error=str(error))
            incidents = tuple(row['id'] for row in db.execute("SELECT id FROM recovery_incidents WHERE state='OPEN' ORDER BY rowid"))
        report = RecoveryReport(tuple(dict.fromkeys(recovered)), tuple(workflows), incidents, tuple(dict.fromkeys(errors)))
        with self._lock:
            self.last_report = report
        return report

    def list_incidents(self, *, open_only: bool = True) -> list[RecoveryIncident]:
        if not isinstance(open_only, bool):
            raise TypeError('open_only must be bool')
        with self.store._connection() as db:
            rows = db.execute('SELECT * FROM recovery_incidents' + (" WHERE state='OPEN'" if open_only else '') +
                              ' ORDER BY rowid').fetchall()
        return [RecoveryIncident(**dict(row)) for row in rows]

    def get_events(self, *, after_sequence: int = 0) -> list[dict]:
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError('after_sequence must be a nonnegative integer')
        with self.store._connection() as db:
            rows = db.execute('SELECT * FROM recovery_events WHERE sequence>? ORDER BY sequence', (after_sequence,)).fetchall()
        return [{**dict(row), 'details': json.loads(row['details'])} for row in rows]

    def _operator(self, operator, reason):
        self.store.workers._id(operator, 'operator')
        self.store.workers._id(reason, 'reason/evidence')

    def _confirm(self, db, session_id, operator, evidence):
        session = db.execute('SELECT * FROM worker_sessions WHERE session_id=?', (session_id,)).fetchone()
        if session is None or session['state'] not in ('FENCED', 'STOPPING', 'STOPPED'):
            raise RecoveryConflict('session must be fenced or draining before confirming exit')
        if session['state'] != 'STOPPED':
            db.execute("UPDATE worker_sessions SET state='STOPPED',stopped_evidence=? WHERE session_id=?", (evidence, session_id))
            db.execute("UPDATE worker_registry SET status='STOPPED',stopped_at=? WHERE session_id=?",
                       (self.store.workers._now(), session_id))
            self.store.workers._event(db, session_id, 'STOPPED', evidence)
            self._event(db, 'STOP_CONFIRMED', session_id, operator, evidence=evidence, host_id=session['host_id'])

    def confirm_session_stopped(self, session_id: str, *, operator: str, evidence: str):
        self._operator(operator, evidence)
        self.store.workers._id(session_id, 'session_id')
        with self.store._transaction() as db:
            self._confirm(db, session_id, operator, evidence)

    def confirm_host_stopped(self, host_id: str, *, session_ids, operator: str, evidence: str):
        """Attest an explicit snapshot, never all current/future sessions on a host."""
        self._operator(operator, evidence)
        self.store.workers._id(host_id, 'host_id')
        if not isinstance(session_ids, (list, tuple)) or not session_ids or len(set(session_ids)) != len(session_ids):
            raise ValueError('session_ids must be a nonempty unique list/tuple')
        for key in session_ids:
            self.store.workers._id(key, 'session_id')
        with self.store._transaction() as db:
            for key in session_ids:
                row = db.execute('SELECT host_id FROM worker_sessions WHERE session_id=?', (key,)).fetchone()
                if row is None or row['host_id'] != host_id:
                    raise RecoveryConflict('session does not belong to the specified host')
                self._confirm(db, key, operator, evidence)
            self._event(db, 'HOST_STOP_CONFIRMED', host_id, operator, session_ids=list(session_ids), evidence=evidence)

    def confirm_task_stopped(self, task_id: str, *, attempt_count: int, operator: str, evidence: str):
        """Offline legacy execution has no session; attest just its exact attempt."""
        self._operator(operator, evidence)
        if type(attempt_count) is not int or attempt_count < 1:
            raise ValueError('attempt_count must be positive')
        with self.store._transaction() as db:
            row = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if not row or row['status'] != 'RUNNING' or row['worker_session'] is not None or row['attempt_count'] != attempt_count:
                raise RecoveryConflict('legacy attempt no longer matches or belongs to a registered session')
            self._incident(db, row, 'WAIT_STOP', 'offline supervisor confirmed invocation exit')
            self._event(db, 'LEGACY_STOP_CONFIRMED', task_id, operator, attempt_count=attempt_count, evidence=evidence)
            self._recover(db, row)

    def _reset_workflow(self, db, workflow_id):
        if workflow_id is None:
            return
        # Only skipped (never dispatched) descendants are reopened. Failed
        # independent branches keep their own incidents, successes keep results.
        db.execute("UPDATE workflow_nodes SET state='WAITING',error=NULL WHERE workflow_id=? AND state='SKIPPED'",
                   (workflow_id,))
        db.execute("UPDATE workflow_runs SET status='RUNNING' WHERE id=?", (workflow_id,))

    def resolve_task(self, incident_id: str, *, action: str, operator: str, reason: str,
                     result=None, extra_retries: int = 0):
        """Resolve one failed attempt atomically with its audit and dispatch holds.

        Retry requires idempotency and consumes the persisted budget. Granting an
        extra budget is explicit and audited; non-idempotent effects must instead
        be reconciled externally and their verified result supplied via succeed.
        """
        self._operator(operator, reason)
        if action not in ('retry', 'succeed', 'fail'):
            raise ValueError('action must be retry, succeed or fail')
        if type(extra_retries) is not int or extra_retries < 0 or (extra_retries and action != 'retry'):
            raise ValueError('extra_retries must be a nonnegative integer for retry only')
        encoded = _json(result) if action == 'succeed' else None
        with self.store._transaction() as db:
            incident = db.execute('SELECT * FROM recovery_incidents WHERE id=?', (incident_id,)).fetchone()
            if not incident or incident['state'] != 'OPEN':
                raise RecoveryConflict('incident missing or already resolved')
            row = db.execute('SELECT * FROM tasks WHERE id=?', (incident['task_id'],)).fetchone()
            if row['attempt_count'] != incident['attempt_count']:
                raise RecoveryConflict('decision requires the same failed attempt; confirm exit and tick first')
            if row['status'] == 'RUNNING' and incident['kind'] == 'RECOVERY_ERROR':
                # A stopped invocation can be reconciled even when its handler
                # was removed from the deployment. No callable is needed to
                # record a verified external result or accept final failure.
                stopped = db.execute("""SELECT 1 FROM worker_sessions WHERE session_id=?
                    AND state='STOPPED' AND stopped_evidence IS NOT NULL""", (row['worker_session'],)).fetchone()
                if row['worker_session'] is None:
                    stopped = db.execute("""SELECT 1 FROM recovery_events WHERE action='LEGACY_STOP_CONFIRMED'
                        AND target_id=? AND json_extract(details,'$.attempt_count')=?""",
                        (row['id'], row['attempt_count'])).fetchone()
                if not stopped:
                    raise RecoveryConflict('old invocation exit has not been confirmed')
                db.execute("UPDATE tasks SET status='FAILED',error=? WHERE id=?",
                           (_json(dict(type='UncertainExecutionError', message='stopped invocation reconciled by operator')), row['id']))
                row = db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone()
                self.store._append_event(db, row, TaskEventType.INTERRUPTED, TaskStatus.RUNNING,
                                         details=dict(operator=operator, reason=reason, incident_id=incident_id))
            if row['status'] != 'FAILED':
                raise RecoveryConflict('decision requires the same failed attempt; confirm exit and tick first')
            status, retry_count, retry_at = row['status'], row['retry_count'], None
            limit = row['retry_limit'] + extra_retries
            if action == 'retry':
                if row['workflow_blocked']:
                    raise RecoveryConflict('input binding failed before dispatch; supply a verified result or accept failure')
                if not json.loads(row['definition'])['idempotent']:
                    raise RecoveryConflict('non-idempotent outcomes require external reconciliation')
                if retry_count >= limit:
                    raise RecoveryConflict('retry budget exhausted; an explicit extra_retries grant is required')
                task = self.store._decode(row, db)
                # Persisted effective budget overrides the original Task.max_retries.
                task.max_retries = limit
                RetryPolicy(limit, **task._persisted_retry_config).begin_retry(task, manual=True)
                status, retry_count, retry_at = task.status.value, task.retry_count, task.retry_at
            elif action == 'succeed':
                status = 'SUCCESS'
            db.execute("""UPDATE tasks SET status=?,retry_count=?,retry_limit=?,retry_at=?,result=?,
                last_error=COALESCE(error,last_error),error=CASE WHEN ?='fail' THEN error ELSE NULL END,
                owner=NULL,worker_session=NULL WHERE id=?""",
                (status, retry_count, limit, retry_at, encoded, action, row['id']))
            if incident['workflow_id'] and action != 'fail':
                db.execute("UPDATE workflow_nodes SET state=?,error=NULL WHERE task_id=?",
                           (status, row['id']))
                self._reset_workflow(db, incident['workflow_id'])
            updated = db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone()
            self.store._append_event(db, updated, TaskEventType.MANUAL_RESOLUTION, TaskStatus.FAILED,
                details=dict(action=action, operator=operator, reason=reason, incident_id=incident_id,
                             extra_retries=extra_retries, result=result if action == 'succeed' else None))
            if action == 'retry':
                self.store._append_event(db, updated, TaskEventType.RETRY_SCHEDULED, TaskStatus.FAILED,
                                         details=dict(reason='manual recovery', retry_limit=limit, retry_at=retry_at))
            self._close(db, incident, _json(dict(action=action, reason=reason, extra_retries=extra_retries)), operator)
            if incident['workflow_id']:
                self.store.workflows._advance(db, incident['workflow_id'])

    def pause_workflow(self, workflow_id: str, *, operator: str, reason: str):
        self._operator(operator, reason)
        with self.store._transaction() as db:
            if not db.execute('SELECT 1 FROM workflow_runs WHERE id=?', (workflow_id,)).fetchone():
                raise KeyError(workflow_id)
            db.execute("INSERT OR IGNORE INTO recovery_holds VALUES('workflow',?,'manual')", (workflow_id,))
            self._event(db, 'WORKFLOW_PAUSED', workflow_id, operator, reason=reason)
            self.store.workflows._advance(db, workflow_id)

    def resume_workflow(self, workflow_id: str, *, operator: str, reason: str):
        self._operator(operator, reason)
        with self.store._transaction() as db:
            if not db.execute('SELECT 1 FROM workflow_runs WHERE id=?', (workflow_id,)).fetchone():
                raise KeyError(workflow_id)
            db.execute("DELETE FROM recovery_holds WHERE scope='workflow' AND target_id=? AND source='manual'", (workflow_id,))
            self._event(db, 'WORKFLOW_RESUMED', workflow_id, operator, reason=reason)
            return self.store.workflows._advance(db, workflow_id)

    def start(self):
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise RuntimeError('RecoverySystem can only start once')
            self._thread = Thread(target=self._run, name='workflow-recovery', daemon=True)
            try:
                self._thread.start()
            except BaseException:
                self._thread = None
                self._stop.set()
                raise

    def _run(self):
        while not self._stop.is_set():
            try:
                report = self.tick()
                self.last_error = RecoveryConflict('; '.join(report.errors)) if report.errors else None
            except Exception as error:
                self.last_error = error
            self._stop.wait(self.interval)

    def shutdown(self, timeout: float | None = None) -> bool:
        _join_timeout(timeout)
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            if thread is current_thread():
                raise RuntimeError('RecoverySystem cannot join itself')
            thread.join(timeout)
        return thread is None or not thread.is_alive()
