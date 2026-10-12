"""Real SQLite + registered consumers: fan-out/fan-in and dependency fencing."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from contextlib import closing
import os
import sqlite3
import subprocess
import sys
import tempfile
from threading import Barrier, Event, Lock
import time
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, NodeStatus, OutputRef, RetryPolicy,
                             TaskEngine, WorkflowOrchestrator, WorkflowStatus)


class DAGSchedulingIntegrationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.dag-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'tasks.db'

    def test_parallel_branches_join_once_across_two_orchestrators(self):
        barrier = Barrier(2)
        calls, lock = [], Lock()

        def branch(value):
            barrier.wait(5)  # Both roots must really execute concurrently.
            with lock:
                calls.append(value)
            return {'value': value}

        def join(left, right):
            with lock:
                calls.append('join')
            return left + right

        engine = TaskEngine(database_path=self.path, task_registry={'branch': branch, 'join': join}, max_workers=2)
        orchestrator = WorkflowOrchestrator(engine)
        workflow = DAGWorkflow((DAGNode('left', 'branch', args=(2,)), DAGNode('right', 'branch', args=(3,)),
            DAGNode('join', 'join', inputs={'left': OutputRef('left', ('value',)), 'right': OutputRef('right', ('value',))})))
        initial = orchestrator.submit(workflow)
        self.assertEqual(initial.nodes['join'].status, NodeStatus.WAITING)
        other = WorkflowOrchestrator(TaskEngine(database_path=self.path, task_registry={'branch': branch, 'join': join}))
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(orchestrator.run, workflow.id, timeout=10)
            second = pool.submit(other.run, workflow.id, execute=False, timeout=10)
            self.assertEqual(first.result(15).nodes['join'].result, 5)
            self.assertEqual(second.result(15).status, WorkflowStatus.SUCCESS)
        self.assertCountEqual(calls, [2, 3, 'join'])
        for node in orchestrator.get(workflow.id).nodes.values():
            self.assertEqual(node.attempt_count, 1)

    def test_worker_cannot_claim_high_priority_downstream_before_dependency(self):
        entered, release = Event(), Event()
        observed = []

        def root():
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test gate')
            return 7

        def child(value):
            observed.append(value)
            return value

        engine = TaskEngine(database_path=self.path, task_registry={'root': root, 'child': child})
        orchestrator = WorkflowOrchestrator(engine)
        workflow = DAGWorkflow((DAGNode('root', 'root'), DAGNode('child', 'child', priority=100,
                                                       inputs={'value': OutputRef('root')})))
        saved = orchestrator.submit(workflow)
        worker = engine.create_worker('external', poll_interval=.005)
        self.addCleanup(worker.shutdown, 5)
        self.addCleanup(release.set)
        worker.start()
        self.assertTrue(entered.wait(5))
        child_task = engine.get_task(saved.nodes['child'].task_id)
        # Exercise both direct claiming and ordinary submission as bypass attempts.
        self.assertIsNone(worker.store.claim(child_task, 'bypass'))
        engine.submit(child_task)
        self.assertEqual(engine.get_task(child_task.id).attempt_count, 0)
        self.assertEqual(observed, [])
        release.set()
        result = orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(observed, [7])

    def test_retry_keeps_children_waiting_and_failure_skips_only_descendants(self):
        attempts = []

        def transient():
            attempts.append(1)
            if len(attempts) == 1:
                raise ValueError('transient')
            return 4

        def failure():
            raise ValueError('permanent')

        def absolute(x):
            return abs(x)

        registry = {'transient': transient, 'failure': failure, 'abs': abs, 'absolute': absolute}
        workflow = DAGWorkflow((
            DAGNode('retry', 'transient'), DAGNode('healthy', 'absolute', inputs={'x': OutputRef('retry')}),
            DAGNode('broken', 'failure', max_retries=0),
            DAGNode('blocked', 'abs', args=(-1,), depends_on=('broken',)),
            DAGNode('descendant', 'abs', args=(-2,), depends_on=('blocked',)),
        ))
        engine = TaskEngine(database_path=self.path, task_registry=registry,
                            retry_policy=RetryPolicy(1, initial_delay=.1), max_workers=2)
        orchestrator = WorkflowOrchestrator(engine)
        saved = orchestrator.submit(workflow)
        worker = engine.create_worker('retry-observer', poll_interval=.005)
        self.addCleanup(worker.shutdown, 5)
        worker.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = orchestrator.tick(workflow.id)[0]
            if state.nodes['retry'].status is NodeStatus.RETRYING:
                self.assertEqual(state.nodes['healthy'].status, NodeStatus.WAITING)
                self.assertEqual(state.nodes['healthy'].attempt_count, 0)
                break
            time.sleep(.002)
        else:
            self.fail('did not observe retry backoff')
        result = orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.FAILED)
        self.assertEqual(result.nodes['healthy'].result, 4)
        self.assertEqual(result.nodes['retry'].attempt_count, 2)
        for key in ('blocked', 'descendant'):
            self.assertEqual(result.nodes[key].status, NodeStatus.SKIPPED)
            self.assertEqual(result.nodes[key].attempt_count, 0)
        self.assertEqual(engine.pending_count, 0)
        events = engine.get_events(saved.nodes['retry'].task_id)
        self.assertEqual(sum(e.event_type.value == 'RETRY_SCHEDULED' for e in events), 1)

    def test_remote_worker_processes_execute_dag_while_parent_only_orchestrates(self):
        def remote_only(*args, **kwargs):
            raise AssertionError('parent must never execute business code')

        # Unique registry functions are a deployment contract; child processes supply
        # the implementations under the same names. Parent execution is prohibited.
        def remote_join(*args, **kwargs):
            raise AssertionError('parent must never execute join')

        engine = TaskEngine(database_path=self.path,
                            task_registry={'root': remote_only, 'join': remote_join})
        orchestrator = WorkflowOrchestrator(engine)
        roots = tuple(DAGNode(f'root-{i}', 'root', args=(i,)) for i in range(4))
        workflow = DAGWorkflow((*roots, DAGNode('join', 'join', inputs={
            f'v{i}': OutputRef(f'root-{i}') for i in range(4)})), id='remote')
        orchestrator.submit(workflow)
        with closing(sqlite3.connect(self.path)) as db:
            with db:
                db.execute('CREATE TABLE effects (node INTEGER PRIMARY KEY, pid INTEGER NOT NULL)')
        script = '''
import os, sqlite3, sys, time
from contextlib import closing
from workflow_engine import TaskEngine, WorkflowOrchestrator
path = sys.argv[1]
def root(value):
    with closing(sqlite3.connect(path)) as db:
        with db:
            db.execute('INSERT INTO effects VALUES(?,?)', (value, os.getpid()))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with closing(sqlite3.connect(path)) as db:
            if db.execute('SELECT COUNT(*) FROM effects').fetchone()[0] >= 2:
                return value
        time.sleep(.005)
    raise TimeoutError('both process workers must start a root')
def join(v0, v1, v2, v3): return v0 + v1 + v2 + v3
engine = TaskEngine(database_path=path, task_registry={'root': root, 'join': join})
worker = engine.create_worker(sys.argv[2], heartbeat_interval=.05, heartbeat_timeout=5, poll_interval=.005)
worker.start()
try:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if engine.get_workflow('remote').terminal:
            break
        if worker.last_error is not None:
            raise worker.last_error
        time.sleep(.005)
    else:
        raise TimeoutError('remote workflow did not terminate')
finally:
    assert worker.shutdown(5)
'''
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        processes = []

        def cleanup():
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)

        self.addCleanup(cleanup)
        for i in range(2):
            processes.append(subprocess.Popen([sys.executable, '-c', script, str(self.path), f'remote-{i}'],
                                              env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        result = orchestrator.run(workflow.id, execute=False, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['join'].result, 6)
        for process in processes:
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, stderr.decode(errors='replace'))
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT node, pid FROM effects').fetchall()
        self.assertEqual({row[0] for row in rows}, {0, 1, 2, 3})
        self.assertEqual(len({row[1] for row in rows}), 2)
        self.assertTrue(all(node.attempt_count == 1 for node in result.nodes.values()))
