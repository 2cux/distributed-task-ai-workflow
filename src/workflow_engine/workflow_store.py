"""Atomic DAG journal and dependency reconciliation on the task database.

The workflow definition stays immutable. Task arguments are materialized once,
in the same transaction that opens their durable dispatch gate. Reconciliation
can run in multiple orchestrator processes, and can be repeated after a crash.
"""

from __future__ import annotations

import json
from uuid import NAMESPACE_URL, uuid5

from .dag import DAGWorkflow, NodeRun, NodeStatus, WorkflowRun, WorkflowStatus
from .events import TaskEventType
from .persistence import IdempotencyConflict, PersistenceError, _json
from .task import Task, TaskStatus


class SQLiteWorkflowStore:
    def __init__(self, tasks):
        self.tasks = tasks
        with tasks._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS workflow_runs (
                id TEXT PRIMARY KEY, definition TEXT NOT NULL, status TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS workflow_nodes (
                workflow_id TEXT NOT NULL REFERENCES workflow_runs(id),
                node_id TEXT NOT NULL, task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
                state TEXT NOT NULL, error TEXT,
                PRIMARY KEY(workflow_id, node_id))""")

    def submit(self, workflow: DAGWorkflow, retry_policy=None) -> WorkflowRun:
        if not isinstance(workflow, DAGWorkflow):
            raise TypeError("workflow must be DAGWorkflow")
        # Take a validated JSON snapshot; later caller mutation cannot change the graph.
        definition = _json(workflow.to_dict())
        workflow = DAGWorkflow.from_dict(json.loads(definition))
        with self.tasks._transaction() as db:
            existing = db.execute("SELECT * FROM workflow_runs WHERE id=?", (workflow.id,)).fetchone()
            if existing is not None:
                if existing['definition'] != definition:
                    raise IdempotencyConflict("workflow ID already has a different definition")
                return self._snapshot(db, existing)
            db.execute("INSERT INTO workflow_runs VALUES(?,?,?)",
                       (workflow.id, definition, WorkflowStatus.PENDING.value))
            for node in workflow.nodes:
                try:
                    handler = self.tasks.registry[node.handler]
                except KeyError as error:
                    raise PersistenceError(f"unregistered workflow handler {node.handler!r}") from error
                task_id = 'dag-' + uuid5(NAMESPACE_URL, _json([workflow.id, node.id])).hex
                if db.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone():
                    raise IdempotencyConflict("workflow task ID already belongs to another submission")
                task = Task(node.id, handler, id=task_id, args=node.args, kwargs=node.kwargs,
                            priority=node.priority, max_retries=node.max_retries, timeout=node.timeout,
                            idempotent=node.idempotent, scheduled_at=node.scheduled_at)
                limit = retry_policy.limit_for(task) if retry_policy else 0
                self.tasks._submit(db, task, self.tasks._definition(task), limit,
                                   retry_policy, workflow_blocked=True)
                db.execute("INSERT INTO workflow_nodes VALUES(?,?,?,?,NULL)",
                           (workflow.id, node.id, task_id, NodeStatus.WAITING.value))
            return self._advance(db, workflow.id)

    def advance(self, workflow_id: str) -> WorkflowRun:
        with self.tasks._transaction() as db:
            return self._advance(db, workflow_id)

    def _advance(self, db, workflow_id: str) -> WorkflowRun:
        run = db.execute("SELECT * FROM workflow_runs WHERE id=?", (workflow_id,)).fetchone()
        if run is None:
            raise KeyError(workflow_id)
        workflow = DAGWorkflow.from_dict(json.loads(run['definition']))
        rows = {row['node_id']: row for row in db.execute(
            """SELECT n.*, t.status AS task_status, t.result, t.definition AS task_definition,
                t.workflow_blocked FROM workflow_nodes n JOIN tasks t ON n.task_id=t.id
                WHERE n.workflow_id=?""", (workflow_id,))}
        definitions = {node.id: node for node in workflow.nodes}
        states = {}
        for key in workflow.topological_order():
            row, node = rows[key], definitions[key]
            state = NodeStatus(row['state'])
            error = row['error']
            if state is NodeStatus.WAITING:
                dependencies = [states[dependency] for dependency in node.dependencies]
                if any(status in (NodeStatus.FAILED, NodeStatus.SKIPPED) for status in dependencies):
                    state = NodeStatus.SKIPPED
                    error = 'dependency failed or was skipped'
                elif all(status is NodeStatus.SUCCESS for status in dependencies):
                    try:
                        kwargs = dict(node.kwargs)
                        for argument, reference in node.inputs.items():
                            value = json.loads(rows[reference.node_id]['result'])
                            for part in reference.path:
                                if type(part) is int and isinstance(value, list) and part >= 0:
                                    value = value[part]
                                elif isinstance(part, str) and isinstance(value, dict):
                                    value = value[part]
                                else:
                                    raise ValueError(f"invalid output path for {argument!r}")
                            kwargs[argument] = value
                        task_definition = json.loads(row['task_definition'])
                        task_definition['kwargs'] = kwargs
                        db.execute("UPDATE tasks SET definition=?, workflow_blocked=0 WHERE id=?",
                                   (_json(task_definition), row['task_id']))
                        state = NodeStatus.READY
                    except (KeyError, IndexError, TypeError, ValueError) as failure:
                        state = NodeStatus.FAILED
                        error = f"input binding failed: {failure}"
                        db.execute("UPDATE tasks SET status='FAILED', error=? WHERE id=?",
                                   (_json(dict(type='InputBindingError', message=error)), row['task_id']))
                        failed = db.execute("SELECT * FROM tasks WHERE id=?", (row['task_id'],)).fetchone()
                        self.tasks._append_event(db, failed, TaskEventType.FAILED, TaskStatus.PENDING,
                                                 details=dict(error_type='InputBindingError', error_message=error))
            elif state not in (NodeStatus.SKIPPED, NodeStatus.FAILED):
                state = NodeStatus.READY if row['task_status'] == 'PENDING' else NodeStatus(row['task_status'])
            states[key] = state
            db.execute("UPDATE workflow_nodes SET state=?, error=? WHERE workflow_id=? AND node_id=?",
                       (state.value, error, workflow_id, key))
        terminal = {NodeStatus.SUCCESS, NodeStatus.FAILED, NodeStatus.SKIPPED}
        if all(state in terminal for state in states.values()):
            status = WorkflowStatus.SUCCESS if all(state is NodeStatus.SUCCESS for state in states.values()) else WorkflowStatus.FAILED
        else:
            status = WorkflowStatus.RUNNING
        db.execute("UPDATE workflow_runs SET status=? WHERE id=?", (status.value, workflow_id))
        return self._snapshot(db, db.execute("SELECT * FROM workflow_runs WHERE id=?", (workflow_id,)).fetchone())

    def _snapshot(self, db, run) -> WorkflowRun:
        definition = json.loads(run['definition'])
        nodes = {}
        for row in db.execute("""SELECT n.*, t.status AS task_status, t.result,
                t.error AS task_error, t.attempt_count, t.retry_count FROM workflow_nodes n
                JOIN tasks t ON n.task_id=t.id WHERE n.workflow_id=?""", (run['id'],)):
            state = NodeStatus(row['state'])
            if state not in (NodeStatus.WAITING, NodeStatus.SKIPPED, NodeStatus.FAILED):
                state = NodeStatus.READY if row['task_status'] == 'PENDING' else NodeStatus(row['task_status'])
            error = row['error']
            if error is None and row['task_error']:
                stored = json.loads(row['task_error'])
                error = f"{stored['type']}: {stored['message']}"
            nodes[row['node_id']] = NodeRun(row['node_id'], row['task_id'], state,
                json.loads(row['result']) if row['result'] is not None else None,
                error, row['attempt_count'], row['retry_count'])
        return WorkflowRun(run['id'], definition['name'], WorkflowStatus(run['status']), nodes)

    def get(self, workflow_id: str) -> WorkflowRun | None:
        with self.tasks._connection() as db:
            db.execute('BEGIN')
            row = db.execute("SELECT * FROM workflow_runs WHERE id=?", (workflow_id,)).fetchone()
            return None if row is None else self._snapshot(db, row)

    def active_ids(self) -> list[str]:
        with self.tasks._connection() as db:
            return [row['id'] for row in db.execute(
                "SELECT id FROM workflow_runs WHERE status IN ('PENDING','RUNNING') ORDER BY rowid")]
