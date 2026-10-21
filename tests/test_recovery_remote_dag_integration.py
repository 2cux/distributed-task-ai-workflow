"""Hard remote exits after an external commit, fenced results and durable DAG repair."""

from contextlib import closing
import copy
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from workflow_engine import (
    DAGNode, DAGWorkflow, NodeStatus, OutputRef, RetryPolicy, StaleExecutionError,
    TaskEngine, TaskEventType, TaskStatus, WorkerRegistry, WorkflowStatus,
)
from workflow_engine.remote import RemoteTaskStore


WORKER_SCRIPT = r'''
from contextlib import closing
import os, sqlite3, sys, time
from pathlib import Path
from workflow_engine.remote import RemoteWorker

control, mode = Path(sys.argv[3]), sys.argv[4]

def wait_for(name):
    deadline = time.monotonic() + 20
    while not (control / name).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('supervisor did not release ' + name)
        time.sleep(.01)

def record(name, effect_key=None):
    # A separate business ledger, never the control plane's database.
    with closing(sqlite3.connect(control / 'effects.db')) as db:
        with db:
            db.execute('INSERT INTO calls VALUES (?, ?, ?)', (name, mode, os.getpid()))
            if effect_key is not None:
                db.execute('INSERT OR IGNORE INTO effects VALUES (?, ?)', (effect_key, 11))

def stable():
    record('stable')
    return 7

def effect(key):
    record('effect', key)
    (control / (mode + '-entered')).touch()
    wait_for('crash-now' if mode == 'crash' else 'return-result')
    if mode == 'crash':
        os._exit(37)
    return 11

def merge(left, right):
    record('merge')
    return left + right

worker = RemoteWorker(sys.argv[1], dict(stable=stable, effect=effect, merge=merge),
    sys.argv[2], mode + '-worker', host_id=mode + '-host',
    heartbeat_interval=.03, heartbeat_timeout=1, poll_interval=.01)
worker.start()
(control / (mode + '-registered')).touch()
try:
    wait_for('done')
finally:
    assert worker.shutdown(5)
assert worker.last_error is None, worker.last_error
'''


class RemoteDAGCrashRecoveryTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        directory = tempfile.TemporaryDirectory(dir=root, prefix='.remote-dag-crash-')
        self.addCleanup(directory.cleanup)
        self.control = Path(directory.name)
        self.database = self.control / 'central.db'
        self.environment = dict(os.environ, PYTHONPATH=str(root / 'src'))
        self.processes = []
        self.now = 100.0

        def central_only(*args, **kwargs):
            raise AssertionError('only remote processes may execute business handlers')

        self.registry = {name: (lambda *a, **kw: central_only(*a, **kw))
                         for name in ('stable', 'effect', 'merge')}
        self.engine = TaskEngine(database_path=self.database, task_registry=self.registry,
                                 retry_policy=RetryPolicy(1))
        self.engine._store.workers = WorkerRegistry(self.database, clock=lambda: self.now)
        self.recovery = self.engine.create_recovery_system()
        self.token = 'remote-dag-crash-local-test-token'
        self.service = self.engine.create_recovery_service(self.token).start()
        self.addCleanup(self.service.shutdown)
        self.addCleanup(self.cleanup_processes)
        self.remote = RemoteTaskStore(self.service.url, self.registry, self.token)
        with closing(sqlite3.connect(self.control / 'effects.db')) as db:
            with db:
                db.execute('CREATE TABLE calls (name TEXT, mode TEXT, pid INTEGER)')
                db.execute('CREATE TABLE effects (key TEXT PRIMARY KEY, value INTEGER)')

    def cleanup_processes(self):
        (self.control / 'done').touch()
        (self.control / 'return-result').touch()
        (self.control / 'crash-now').touch()
        for process in self.processes:
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)

    def spawn(self, mode):
        process = subprocess.Popen(
            [sys.executable, '-c', WORKER_SCRIPT, self.service.url, self.token,
             str(self.control), mode], env=self.environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.processes.append(process)
        return process

    def eventually(self, predicate):
        deadline = time.monotonic() + 10
        while not predicate():
            for process in self.processes:
                if process.poll() not in (None, 37):
                    _, stderr = process.communicate(timeout=5)
                    self.fail(f'worker exited {process.returncode}: {stderr.decode(errors="replace")}')
            if time.monotonic() >= deadline:
                self.fail('remote DAG recovery condition did not become true before deadline')
            time.sleep(.01)

    def scan(self):
        report = self.recovery.tick()
        self.assertEqual(report.errors, ())
        return report

    def reject_result(self, task, owner):
        before = self.engine.get_task(task.id)
        history = self.engine.get_events(task.id)
        workflow = self.engine.get_workflow('crashed-dag')
        task.status, task.result = TaskStatus.SUCCESS, 999
        with self.assertRaises(StaleExecutionError):
            self.remote.finish(task, owner)
        after = self.engine.get_task(task.id)
        self.assertEqual((after.status, after.result, after.attempt_count, after.retry_count),
                         (before.status, before.result, before.attempt_count, before.retry_count))
        self.assertEqual(self.engine.get_events(task.id), history)
        self.assertEqual(self.engine.get_workflow('crashed-dag'), workflow)

    def exercise_crash(self, *, idempotent, retries):
        flow = DAGWorkflow((
            DAGNode('stable', 'stable', priority=10),
            DAGNode('effect', 'effect', kwargs={'key': 'one-business-effect'},
                    idempotent=idempotent, max_retries=retries),
            DAGNode('merge', 'merge', inputs={'left': OutputRef('stable'),
                                             'right': OutputRef('effect')}),
        ), id='crashed-dag')
        submitted = self.engine.submit_workflow(flow)
        effect_id = submitted.nodes['effect'].task_id
        stable_id = submitted.nodes['stable'].task_id
        merge_id = submitted.nodes['merge'].task_id
        victim = self.spawn('crash')
        self.eventually(lambda: (self.control / 'crash-entered').exists())
        self.assertIsNone(victim.poll())
        self.assertEqual(self.engine.get_task(stable_id).status, TaskStatus.SUCCESS)
        completed_history = self.engine.get_events(stable_id)
        old = self.remote.get(effect_id)
        started = old.events[-1]
        self.assertEqual(started.event_type, TaskEventType.STARTED)
        old_owner, old_session = started.details['owner'], started.details['worker_session']
        self.assertEqual(old._remote_session_id, old_session)
        self.assertEqual((old.status, old.attempt_count, old.retry_count), (TaskStatus.RUNNING, 1, 0))
        self.assertEqual(self.engine.worker_registry.get_session(old_session).host_id, 'crash-host')
        self.reject_result(copy.copy(old), 'wrong-owner')

        # Crash only after the side effect has committed and the exact claim was observed.
        (self.control / 'crash-now').touch()
        _, stderr = victim.communicate(timeout=10)
        self.assertEqual(victim.returncode, 37, stderr.decode(errors='replace'))
        self.now = 102.0  # Advance the registry clock, without timing-dependent heartbeat sleeps.
        self.assertEqual(self.scan().recovered_task_ids, ())
        self.assertEqual(self.engine.worker_registry.get_session(old_session).state, 'FENCED')
        incidents = self.recovery.list_incidents()
        self.assertEqual(len(incidents), 1)
        incident = incidents[0]
        self.assertEqual((incident.kind, incident.task_id, incident.session_id,
                          incident.workflow_id, incident.host_id, incident.attempt_count),
                         ('WAIT_STOP', effect_id, old_session, flow.id, 'crash-host', 1))
        self.assertEqual(self.engine.get_workflow(flow.id).status, WorkflowStatus.PAUSED)
        self.reject_result(copy.copy(old), old_owner)

        replacement = self.spawn('replacement')
        self.eventually(lambda: (self.control / 'replacement-registered').exists())
        # Even a healthy replacement and repeated scans cannot substitute for stop proof.
        for _ in range(3):
            self.assertEqual(self.scan().recovered_task_ids, ())
            saved = self.engine.get_task(effect_id)
            self.assertEqual((saved.status, saved.attempt_count, saved.retry_count),
                             (TaskStatus.RUNNING, 1, 0))
            self.assertEqual(self.engine.get_task(merge_id).attempt_count, 0)
        self.assertFalse((self.control / 'replacement-entered').exists())
        self.assertEqual(self.recovery.list_incidents(), [incident])

        # Reopen the coordinator before attesting the exact process that was joined.
        self.engine = TaskEngine(database_path=self.database, task_registry=self.registry,
                                 retry_policy=RetryPolicy(99))
        self.engine._store.workers = WorkerRegistry(self.database, clock=lambda: self.now)
        self.recovery = self.engine.create_recovery_system()
        self.assertEqual(self.engine.get_workflow(flow.id).status, WorkflowStatus.PAUSED)
        evidence = f'joined exact worker pid={victim.pid}; exitcode=37; no background business calls'
        for _ in range(2):
            self.recovery.confirm_session_stopped(old_session, operator='test-supervisor', evidence=evidence)
        report = self.scan()
        self.assertEqual(report.recovered_task_ids, (effect_id,))
        self.assertEqual(self.engine.worker_registry.get_session(old_session).stopped_evidence, evidence)
        self.assertEqual(self.scan().recovered_task_ids, ())

        safe = idempotent and retries > 0
        if safe:
            self.eventually(lambda: (self.control / 'replacement-entered').exists())
            current = self.remote.get(effect_id)
            current_start = current.events[-1]
            new_owner = current_start.details['owner']
            new_session = current_start.details['worker_session']
            self.assertNotEqual(new_owner, old_owner)
            self.assertNotEqual(new_session, old_session)
            self.assertEqual(self.engine.worker_registry.get_session(new_session).host_id, 'replacement-host')
            self.assertEqual((current.status, current.attempt_count, current.retry_count),
                             (TaskStatus.RUNNING, 2, 1))
            self.assertEqual(self.engine.get_task(merge_id).attempt_count, 0)
            self.reject_result(copy.copy(old), old_owner)
            self.reject_result(copy.copy(current), old_owner)
            wrong_session = copy.copy(current)
            wrong_session._remote_session_id = old_session
            self.reject_result(wrong_session, new_owner)
            stale_attempt = copy.copy(old)
            stale_attempt._remote_session_id = new_session
            self.reject_result(stale_attempt, new_owner)
            self.assertFalse(self.recovery.list_incidents())
            (self.control / 'return-result').touch()

            def complete():
                self.scan()
                return self.engine.get_workflow(flow.id).status is WorkflowStatus.SUCCESS

            self.eventually(complete)
        else:
            saved = self.engine.get_task(effect_id)
            self.assertEqual((saved.status, saved.attempt_count, saved.retry_count), (TaskStatus.FAILED, 1, 0))
            self.assertIn('UncertainExecutionError', str(saved.error))
            failure = self.recovery.list_incidents()[0]
            self.assertEqual((failure.id, failure.kind), (incident.id, 'TASK_FAILURE'))
            self.assertEqual(self.engine.get_workflow(flow.id).status, WorkflowStatus.PAUSED)
            self.recovery.resolve_task(failure.id, action='fail', operator='test-supervisor',
                                       reason='accept uncertain outcome without unsafe or unbudgeted replay')
            self.scan()
            self.assertEqual(self.engine.get_workflow(flow.id).status, WorkflowStatus.FAILED)
            self.assertFalse((self.control / 'replacement-entered').exists())

        (self.control / 'done').touch()
        _, stderr = replacement.communicate(timeout=10)
        self.assertEqual(replacement.returncode, 0, stderr.decode(errors='replace'))
        final = self.engine.get_workflow(flow.id)
        self.assertEqual(final.nodes['stable'].status, NodeStatus.SUCCESS)
        self.assertEqual((final.nodes['stable'].attempt_count, final.nodes['stable'].result), (1, 7))
        self.assertEqual(self.engine.get_events(stable_id), completed_history)
        self.assertEqual(final.nodes['effect'].status, NodeStatus.SUCCESS if safe else NodeStatus.FAILED)
        self.assertEqual((final.nodes['effect'].attempt_count, final.nodes['effect'].retry_count),
                         (2, 1) if safe else (1, 0))
        self.assertEqual(final.nodes['merge'].status, NodeStatus.SUCCESS if safe else NodeStatus.SKIPPED)
        self.assertEqual(final.nodes['merge'].attempt_count, 1 if safe else 0)
        self.assertEqual(final.nodes['merge'].result, 18 if safe else None)
        for node in final.nodes.values():
            task = self.engine.get_task(node.task_id)
            self.assertEqual((task.result, task.attempt_count, task.retry_count),
                             (node.result, node.attempt_count, node.retry_count))
            if node.status is not NodeStatus.SKIPPED:
                self.assertEqual(task.status.value, node.status.value)
        self.reject_result(copy.copy(old), old_owner)
        history = {node.task_id: self.engine.get_events(node.task_id) for node in final.nodes.values()}
        for events in history.values():
            self.assertEqual([event.sequence for event in events], list(range(1, len(events) + 1)))
        self.assertEqual([event.event_type for event in history[effect_id]], [
            TaskEventType.CREATED, TaskEventType.SUBMITTED, TaskEventType.STARTED,
            TaskEventType.INTERRUPTED,
            *([TaskEventType.RETRY_SCHEDULED, TaskEventType.STARTED, TaskEventType.SUCCEEDED]
              if safe else [TaskEventType.MANUAL_RESOLUTION]),
        ])
        interrupted = history[effect_id][3]
        self.assertEqual((interrupted.details['owner'], interrupted.details['worker_session']),
                         (old_owner, old_session))
        if safe:
            self.assertEqual(history[effect_id][4].details['retry_limit'], 1)
        self.assertFalse(self.recovery.list_incidents())
        audit = self.recovery.get_events()
        self.assertEqual(sum(e['action'] == 'STOP_CONFIRMED' for e in audit), 1)
        self.assertEqual(sum(e['action'] == 'TASK_RECOVERED' for e in audit), 1)
        self.assertTrue(all(s.state == 'STOPPED' for s in self.engine.worker_registry.list_sessions()))
        with closing(sqlite3.connect(self.control / 'effects.db')) as db:
            self.assertEqual(db.execute('SELECT * FROM effects').fetchall(), [('one-business-effect', 11)])
            self.assertEqual(db.execute('SELECT name, mode, pid FROM calls ORDER BY rowid').fetchall(),
                             [('stable', 'crash', victim.pid), ('effect', 'crash', victim.pid)] +
                             ([('effect', 'replacement', replacement.pid),
                               ('merge', 'replacement', replacement.pid)] if safe else []))

        self.service.shutdown()
        reopened = TaskEngine(database_path=self.database, task_registry=self.registry,
                              retry_policy=RetryPolicy(99))
        self.assertEqual(reopened.get_workflow(flow.id), final)
        self.assertEqual(reopened.submit_workflow(flow), final)
        recovered = reopened.create_recovery_system()
        self.assertEqual(recovered.tick().recovered_task_ids, ())
        self.assertEqual(recovered.last_report.errors, ())
        self.assertEqual(recovered.get_events(), audit)
        self.assertFalse(recovered.list_incidents())
        self.assertEqual(reopened.start(), [])
        for task_id, events in history.items():
            self.assertEqual(reopened.get_events(task_id), events)
        with closing(sqlite3.connect(self.database)) as db:
            owners = db.execute('''SELECT COUNT(*) FROM tasks
                WHERE owner IS NOT NULL OR worker_session IS NOT NULL''').fetchone()[0]
            self.assertEqual(owners, 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM recovery_holds').fetchone()[0], 0)
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_remote_crash_retries_safe_node_and_recovers_dag_after_restart(self):
        self.exercise_crash(idempotent=True, retries=1)

    def test_remote_crash_does_not_replay_non_idempotent_dag_node(self):
        self.exercise_crash(idempotent=False, retries=1)

    def test_remote_crash_does_not_reset_exhausted_dag_retry_budget(self):
        self.exercise_crash(idempotent=True, retries=0)


if __name__ == '__main__':
    unittest.main()
