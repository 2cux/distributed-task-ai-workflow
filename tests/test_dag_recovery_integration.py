"""Independent process-crash and atomic journal integration tests."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, IdempotencyConflict, NodeStatus,
                             PersistenceError, TaskEngine, WorkflowOrchestrator, WorkflowStatus)


class DAGRecoveryIntegrationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.dag-recovery-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'tasks.db'

    def crash(self, script, *args):
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        process = subprocess.run([sys.executable, '-c', script, str(self.path), *args],
                                 env=env, capture_output=True, timeout=15)
        self.assertEqual(process.returncode, 23, process.stderr.decode(errors='replace'))

    def test_crash_after_success_commit_before_downstream_dispatch(self):
        self.crash('''
import os, sys
from workflow_engine import DAGNode, DAGWorkflow, OutputRef, TaskEngine, WorkflowOrchestrator, SQLiteTaskStore
def identity(value): return value
registry = {'abs': abs, 'identity': identity}
engine = TaskEngine(database_path=sys.argv[1], task_registry=registry)
flow = DAGWorkflow((DAGNode('root', 'abs', args=(-9,)),
                    DAGNode('tail', 'identity', inputs={'value': OutputRef('root')})), id='committed')
WorkflowOrchestrator(engine).submit(flow)
finish = SQLiteTaskStore.finish
def crash(self, task, owner):
    finish(self, task, owner)
    os._exit(23)
SQLiteTaskStore.finish = crash
TaskEngine(database_path=sys.argv[1], task_registry=registry).start()
''')
        observed = []

        def identity(value):
            observed.append(value)
            return value

        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs, 'identity': identity})
        orchestrator = WorkflowOrchestrator(engine)
        saved = orchestrator.get('committed')
        self.assertEqual(saved.nodes['root'].status, NodeStatus.SUCCESS)
        self.assertEqual(saved.nodes['tail'].status, NodeStatus.WAITING)
        result = orchestrator.run('committed', timeout=5)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(observed, [9])
        self.assertEqual(result.nodes['root'].attempt_count, 1)

    def test_crash_after_binding_and_dispatch_commit_does_not_lose_ready_task(self):
        self.crash('''
import os, sys
from workflow_engine import DAGNode, DAGWorkflow, OutputRef, TaskEngine, WorkflowOrchestrator
def identity(value): return value
registry = {'abs': abs, 'identity': identity}
engine = TaskEngine(database_path=sys.argv[1], task_registry=registry)
flow = DAGWorkflow((DAGNode('root', 'abs', args=(-6,)),
                    DAGNode('tail', 'identity', inputs={'value': OutputRef('root')})), id='dispatched')
orchestrator = WorkflowOrchestrator(engine)
orchestrator.submit(flow)
TaskEngine(database_path=sys.argv[1], task_registry=registry).start()
orchestrator.tick(flow.id)
os._exit(23)
''')
        def identity(value):
            return value
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs, 'identity': identity})
        orchestrator = WorkflowOrchestrator(engine)
        saved = orchestrator.get('dispatched')
        self.assertEqual(saved.nodes['tail'].status, NodeStatus.READY)
        task = engine.get_task(saved.nodes['tail'].task_id)
        self.assertEqual(task.kwargs, {'value': 6})
        result = orchestrator.run('dispatched', timeout=5)
        self.assertEqual(result.nodes['tail'].result, 6)
        self.assertEqual(result.nodes['root'].attempt_count, 1)
        self.assertEqual(result.nodes['tail'].attempt_count, 1)

    def test_real_interruption_respects_idempotency_and_retry_budget(self):
        for safe in (True, False):
            with self.subTest(idempotent=safe):
                self.path = self.path.parent / f'effect-{safe}.db'
                self.crash('''
import os, sqlite3, sys
from contextlib import closing
from workflow_engine import DAGNode, DAGWorkflow, OutputRef, TaskEngine, WorkflowOrchestrator, RetryPolicy
def effect(key):
    with closing(sqlite3.connect(sys.argv[1])) as db:
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS effects (id TEXT PRIMARY KEY)')
            db.execute('INSERT OR IGNORE INTO effects VALUES(?)', (key,))
    os._exit(23)
def identity(value): return value
registry = {'effect': effect, 'identity': identity}
engine = TaskEngine(database_path=sys.argv[1], task_registry=registry, retry_policy=RetryPolicy(1))
flow = DAGWorkflow((DAGNode('effect', 'effect', args=('stable-effect-key',), idempotent=sys.argv[2]=='True'),
                    DAGNode('tail', 'identity', inputs={'value': OutputRef('effect')})), id='interrupted')
WorkflowOrchestrator(engine).submit(flow)
TaskEngine(database_path=sys.argv[1], task_registry=registry).start()
''', str(safe))

                def effect(key):
                    with closing(sqlite3.connect(self.path)) as db:
                        with db:
                            db.execute('INSERT OR IGNORE INTO effects VALUES(?)', (key,))
                        return db.execute('SELECT COUNT(*) FROM effects').fetchone()[0]

                def identity(value):
                    return value

                registry = {'effect': effect, 'identity': identity}
                before = WorkflowOrchestrator(TaskEngine(database_path=self.path, task_registry=registry))
                self.assertEqual(before.tick('interrupted')[0].nodes['effect'].status, NodeStatus.RUNNING)
                self.assertEqual(before.get('interrupted').nodes['tail'].attempt_count, 0)
                engine = TaskEngine(database_path=self.path, task_registry=registry, recover_interrupted=True)
                # Repeated offline recovery must not consume the budget twice.
                engine = TaskEngine(database_path=self.path, task_registry=registry, recover_interrupted=True)
                result = WorkflowOrchestrator(engine).run('interrupted', timeout=5)
                self.assertEqual(result.status, WorkflowStatus.SUCCESS if safe else WorkflowStatus.FAILED)
                self.assertEqual(result.nodes['effect'].attempt_count, 2 if safe else 1)
                self.assertEqual(result.nodes['effect'].retry_count, 1 if safe else 0)
                self.assertEqual(result.nodes['tail'].status, NodeStatus.SUCCESS if safe else NodeStatus.SKIPPED)
                with closing(sqlite3.connect(self.path)) as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM effects').fetchone()[0], 1)

    def test_atomic_submission_rollback_and_concurrent_duplicate_submission(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        orchestrator = WorkflowOrchestrator(engine)
        invalid = DAGWorkflow((DAGNode('valid', 'abs', args=(-1,)), DAGNode('invalid', 'missing')), id='invalid')
        with self.assertRaises(PersistenceError):
            orchestrator.submit(invalid)
        self.assertIsNone(orchestrator.get('invalid'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM task_events').fetchone()[0], 0)
        workflow = DAGWorkflow((DAGNode('root', 'abs', args=(-5,)),), id='duplicate')
        other = WorkflowOrchestrator(TaskEngine(database_path=self.path, task_registry={'abs': abs}))
        with ThreadPoolExecutor(2) as pool:
            submissions = [pool.submit(o.submit, workflow) for o in (orchestrator, other)]
            self.assertEqual(submissions[0].result(), submissions[1].result())
        with self.assertRaises(IdempotencyConflict):
            orchestrator.submit(DAGWorkflow((DAGNode('root', 'abs', args=(-99,)),), id='duplicate'))
        result = orchestrator.run(workflow.id, timeout=5)
        self.assertEqual(orchestrator.submit(workflow), result)
        self.assertEqual(result.nodes['root'].attempt_count, 1)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 1)

    def test_registered_worker_crash_requires_confirmed_stop_before_dag_replay(self):
        self.crash('''
import os, sys, time
from workflow_engine import DAGNode, DAGWorkflow, OutputRef, TaskEngine, WorkflowOrchestrator, RetryPolicy
def action(): os._exit(23)
def identity(value): return value
engine = TaskEngine(database_path=sys.argv[1], task_registry={'action': action, 'identity': identity},
                    retry_policy=RetryPolicy(1))
flow = DAGWorkflow((DAGNode('action', 'action', idempotent=True),
                    DAGNode('tail', 'identity', inputs={'value': OutputRef('action')})), id='registered')
WorkflowOrchestrator(engine).submit(flow)
engine.create_worker('crashed-worker', poll_interval=.005).start()
time.sleep(20)
''')
        calls = []

        def action():
            calls.append('action')
            return 10

        def identity(value):
            calls.append('tail')
            return value

        engine = TaskEngine(database_path=self.path, task_registry={'action': action, 'identity': identity})
        orchestrator = WorkflowOrchestrator(engine)
        session = engine.worker_registry.list_sessions()[0]
        self.assertEqual(orchestrator.tick('registered')[0].nodes['action'].status, NodeStatus.RUNNING)
        with self.assertRaises(PersistenceError):
            engine.recover_worker(session.session_id)
        with self.assertRaises(PersistenceError):
            TaskEngine(database_path=self.path, task_registry={'action': action, 'identity': identity},
                       recover_interrupted=True)
        engine.worker_registry.request_stop(session.worker_id, session.session_id)
        engine.worker_registry.confirm_stopped(session.worker_id, session.session_id,
            evidence='integration supervisor observed and joined exited subprocess with code 23')
        self.assertEqual(len(engine.recover_worker(session.session_id)), 1)
        self.assertEqual(engine.recover_worker(session.session_id), [])
        result = orchestrator.run('registered', timeout=5)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['action'].attempt_count, 2)
        self.assertEqual(result.nodes['action'].retry_count, 1)
        self.assertEqual(result.nodes['tail'].result, 10)
        self.assertEqual(calls, ['action', 'tail'])
