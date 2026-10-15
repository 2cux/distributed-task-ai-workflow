"""Bounded, coherent read models for the external management API.

Queries deliberately read persisted JSON rather than decoding executable Tasks:
operators must be able to inspect runs even if a handler was removed on restart.
"""
from __future__ import annotations

from contextlib import contextmanager
import json

from .dag import WorkflowStatus
from .recovery import CLAIM_GUARD
from .task import TaskStatus
from .llm_usage import budget_status, workflow_usage


class ManagementQueries:
    def __init__(self, store, *, busy_timeout=2):
        self.store = store
        self.busy_timeout = busy_timeout
        with store._transaction() as db:
            db.execute('CREATE INDEX IF NOT EXISTS management_task_status ON tasks(status, sequence)')
            db.execute('CREATE INDEX IF NOT EXISTS management_workflow_status ON workflow_runs(status)')

    @contextmanager
    def snapshot(self):
        with self.store._connection() as db:
            db.execute(f'PRAGMA busy_timeout={int(self.busy_timeout * 1000)}')
            db.execute('BEGIN')
            yield db

    @staticmethod
    def page(rows, limit, decode=lambda row: dict(row)):
        more = len(rows) > limit
        selected = rows[:limit]
        return dict(items=[decode(row) for row in selected],
                    next_cursor=selected[-1]['cursor'] if more else None)

    @staticmethod
    def task(row):
        return dict(id=row['id'], definition=json.loads(row['definition']),
                    status=row['status'], attempt_count=row['attempt_count'],
                    retry_count=row['retry_count'], retry_limit=row['retry_limit'],
                    retry_at=row['retry_at'], scheduled_at=row['scheduled_at'],
                    workflow_blocked=bool(row['workflow_blocked']),
                    worker_session=row['worker_session'],
                    **{key: json.loads(row[key]) if row[key] is not None else None
                       for key in ('result', 'error', 'last_error')})

    @staticmethod
    def holds(db, scope, target):
        return [row['source'] for row in db.execute(
            'SELECT source FROM recovery_holds WHERE scope=? AND target_id=? ORDER BY source',
            (scope, target))]

    def workflow(self, db, row, *, detail=False):
        definition = json.loads(row['definition'])
        value = dict(id=row['id'], name=definition['name'], status=row['status'])
        if detail:
            value['definition'] = definition
            value['llm_usage'] = workflow_usage(db, row['id'])
            value['llm_budget'] = budget_status(definition.get('llm_budget'), value['llm_usage'])
            value['holds'] = self.holds(db, 'workflow', row['id'])
            value['nodes'] = [dict(id=node['node_id'], task_id=node['task_id'],
                                  status=(node['state'] if node['state'] in ('WAITING', 'SKIPPED', 'FAILED') else
                                          'READY' if node['task_status'] == 'PENDING' else node['task_status']),
                                  task_status=node['task_status'],
                                  attempt_count=node['attempt_count'], retry_count=node['retry_count'],
                                  result=json.loads(node['result']) if node['result'] is not None else None,
                                  error=node['error'] or (json.loads(node['task_error'])
                                                         if node['task_error'] else None))
                              for node in db.execute('''SELECT n.*, t.status AS task_status,
                                  t.result,t.error AS task_error,t.attempt_count,t.retry_count
                                  FROM workflow_nodes n JOIN tasks t ON t.id=n.task_id
                                  WHERE n.workflow_id=? ORDER BY n.rowid''', (row['id'],))]
        return value

    def list(self, resource, *, cursor=0, limit=50, status=None, workflow_id=None):
        tables = {'workflows': ('workflow_runs', 'id', 'status'),
                  'tasks': ('tasks', 'id', 'status'),
                  'workers': ('worker_registry', 'worker_id', 'status'),
                  'sessions': ('worker_sessions', 'session_id', 'state'),
                  'incidents': ('recovery_incidents', 'id', 'state')}
        table, _, state_column = tables[resource]
        filters, arguments = ['r.rowid>?'], [cursor]
        if status is not None:
            filters.append(f'r.{state_column}=?')
            arguments.append(status)
        if workflow_id is not None:
            filters.append('EXISTS (SELECT 1 FROM workflow_nodes n WHERE n.task_id=r.id AND n.workflow_id=?)')
            arguments.append(workflow_id)
        with self.snapshot() as db:
            rows = db.execute(f'SELECT r.*,r.rowid AS cursor FROM {table} r WHERE ' +
                              ' AND '.join(filters) + ' ORDER BY r.rowid LIMIT ?',
                              (*arguments, limit + 1)).fetchall()
            if resource == 'workflows':
                decode = lambda row: self.workflow(db, row)
            elif resource == 'tasks':
                decode = self.task
            else:
                decode = lambda row: {key: row[key] for key in row.keys() if key != 'cursor'}
            return self.page(rows, limit, decode)

    def get(self, resource, key):
        table, column = {'workflows': ('workflow_runs', 'id'), 'tasks': ('tasks', 'id'),
                         'workers': ('worker_registry', 'worker_id'),
                         'sessions': ('worker_sessions', 'session_id'),
                         'incidents': ('recovery_incidents', 'id')}[resource]
        with self.snapshot() as db:
            row = db.execute(f'SELECT * FROM {table} WHERE {column}=?', (key,)).fetchone()
            if row is None:
                raise KeyError(key)
            if resource == 'workflows':
                return self.workflow(db, row, detail=True)
            if resource == 'tasks':
                return {**self.task(row), 'holds': self.holds(db, 'task', key)}
            return dict(row)

    def events(self, resource, key=None, *, cursor=0, limit=50):
        with self.snapshot() as db:
            if resource in ('tasks', 'workflows', 'sessions'):
                table, column = {'tasks': ('tasks', 'id'), 'workflows': ('workflow_runs', 'id'),
                                 'sessions': ('worker_sessions', 'session_id')}[resource]
                if not db.execute(f'SELECT 1 FROM {table} WHERE {column}=?', (key,)).fetchone():
                    raise KeyError(key)
            if resource == 'workflows':
                rows = db.execute('''SELECT e.*,e.rowid AS cursor,n.node_id FROM task_events e
                    JOIN workflow_nodes n ON n.task_id=e.task_id
                    WHERE n.workflow_id=? AND e.rowid>? ORDER BY e.rowid LIMIT ?''',
                                  (key, cursor, limit + 1)).fetchall()
            elif resource == 'tasks':
                rows = db.execute('''SELECT *,sequence AS cursor FROM task_events
                    WHERE task_id=? AND sequence>? ORDER BY sequence LIMIT ?''',
                                  (key, cursor, limit + 1)).fetchall()
            elif resource == 'sessions':
                rows = db.execute('''SELECT *,sequence AS cursor FROM worker_session_events
                    WHERE session_id=? AND sequence>? ORDER BY sequence LIMIT ?''',
                                  (key, cursor, limit + 1)).fetchall()
            else:
                rows = db.execute('''SELECT *,sequence AS cursor FROM recovery_events
                    WHERE sequence>? ORDER BY sequence LIMIT ?''', (cursor, limit + 1)).fetchall()
            return self.page(rows, limit, lambda row: {
                **dict(row), **({'details': json.loads(row['details'])} if 'details' in row.keys() else {})})

    def overview(self):
        now = self.store.workers._now()
        with self.snapshot() as db:
            def counts(table, column):
                return {row[column]: row['count'] for row in db.execute(
                    f'SELECT {column},COUNT(*) AS count FROM {table} GROUP BY {column}')}
            tasks = {state.value: 0 for state in TaskStatus}
            tasks.update(counts('tasks', 'status'))
            workflows = {state.value: 0 for state in WorkflowStatus}
            workflows.update(counts('workflow_runs', 'status'))
            attempts = db.execute('SELECT COALESCE(SUM(attempt_count),0),COALESCE(SUM(retry_count),0) FROM tasks').fetchone()
            dispatchable = db.execute(f'''SELECT COUNT(*) FROM tasks WHERE status IN ('PENDING','RETRYING')
                AND workflow_blocked=0 AND (scheduled_at IS NULL OR scheduled_at<=? OR status='RETRYING')
                AND (retry_at IS NULL OR retry_at<=?) AND {CLAIM_GUARD}''', (now, now)).fetchone()[0]
            overdue = db.execute('''SELECT COUNT(*) FROM worker_sessions WHERE state IN ('ACTIVE','STOPPING','SUSPECT')
                AND last_heartbeat_at+heartbeat_timeout<?''', (now,)).fetchone()[0]
            return dict(tasks=tasks, workflows=workflows, sessions=counts('worker_sessions', 'state'),
                        attempts=attempts[0], retries=attempts[1], dispatchable_tasks=dispatchable,
                        overdue_sessions=overdue, open_incidents=db.execute(
                            "SELECT COUNT(*) FROM recovery_incidents WHERE state='OPEN'").fetchone()[0])

    def ready(self):
        with self.snapshot() as db:
            db.execute('SELECT id FROM workflow_runs LIMIT 1').fetchone()

