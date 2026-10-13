"""DAG -> recovery coordinator -> HTTP -> independent processes -> durable results."""

from contextlib import closing
from datetime import datetime
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time

from workflow_engine import (
    DAGNode, DAGWorkflow, NodeStatus, OutputRef, RetryPolicy,
    TaskEngine, TaskEventType, WorkflowStatus,
)


WORKER_SCRIPT = r'''
import os, sys, time
from pathlib import Path
from workflow_engine.remote import RemoteWorker

control = Path(sys.argv[3])
def retrieve(query):
    return {'context': query.upper(), 'pid': os.getpid()}

def model(context, name):
    try:
        with (control / name).open('x') as marker:
            marker.write(str(os.getpid()))
    except FileExistsError:
        pass
    deadline = time.monotonic() + 10
    while not all((control / peer).exists() for peer in ('a', 'b')):
        if time.monotonic() >= deadline:
            raise TimeoutError('parallel model peer did not enter')
        time.sleep(.01)
    if name == 'a' and not (control / 'failed-once').exists():
        (control / 'failed-once').touch()
        raise RuntimeError('transient model failure')
    return {'answer': context + '-' + name, 'pid': os.getpid()}

def join(left, right):
    return {'answers': [left['answer'], right['answer']],
            'pids': [left['pid'], right['pid']]}

def validate(combined):
    assert combined['answers'] == ['E2E-a', 'E2E-b']
    return dict(combined, valid=True)

worker = RemoteWorker(sys.argv[1],
    dict(retrieve=retrieve, model=model, join=join, validate=validate),
    sys.argv[2], sys.argv[4], host_id=sys.argv[4],
    heartbeat_interval=.1, heartbeat_timeout=10, poll_interval=.01)
worker.start()
try:
    deadline = time.monotonic() + 30
    while not (control / 'done').exists():
        if worker.last_error is not None:
            raise worker.last_error
        if time.monotonic() >= deadline:
            raise TimeoutError('central supervisor did not finish')
        time.sleep(.01)
finally:
    assert worker.shutdown(5)
assert worker.last_error is None, worker.last_error
'''


def test_remote_parallel_ai_dag_retry_events_and_restart():
    # Central handlers are contracts only; an accidental local invocation fails.
    def central_only(*args, **kwargs):
        raise AssertionError('business code must execute in remote processes')

    registry = {name: (lambda *a, **kw: central_only(*a, **kw))
                for name in ('retrieve', 'model', 'join', 'validate')}
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(dir=root, prefix='.e2e-remote-') as directory:
        control = Path(directory)
        database = control / 'central.db'
        engine = TaskEngine(database_path=database, task_registry=registry,
                            retry_policy=RetryPolicy(1, initial_delay=.05))
        flow = DAGWorkflow((
            DAGNode('retrieve', 'retrieve', kwargs={'query': 'e2e'},
                    scheduled_at=time.time() + .2),
            DAGNode('a', 'model', kwargs={'name': 'a'}, idempotent=True,
                    inputs={'context': OutputRef('retrieve', ('context',))}, timeout=12),
            DAGNode('b', 'model', kwargs={'name': 'b'},
                    inputs={'context': OutputRef('retrieve', ('context',))}, timeout=12),
            DAGNode('join', 'join', inputs={'left': OutputRef('a'), 'right': OutputRef('b')}),
            DAGNode('validate', 'validate', inputs={'combined': OutputRef('join')}),
        ), id='remote-ai-e2e')
        engine.submit_workflow(flow)
        assert engine.submit_workflow(flow) == engine.get_workflow(flow.id)
        token = 'local-end-to-end-test-token'
        service = engine.create_recovery_service(token).start()
        recovery = engine.create_recovery_system(interval=.02)
        processes = []
        try:
            recovery.start()
            env = dict(os.environ, PYTHONPATH=str(root / 'src'))
            for index in range(2):
                processes.append(subprocess.Popen(
                    [sys.executable, '-c', WORKER_SCRIPT, service.url, token,
                     str(control), f'remote-e2e-{index}'], env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE))
            deadline = time.monotonic() + 25
            while engine.get_workflow(flow.id).status is not WorkflowStatus.SUCCESS:
                for process in processes:
                    if process.poll() is not None:
                        _, stderr = process.communicate(timeout=5)
                        raise AssertionError(stderr.decode(errors='replace'))
                assert recovery.last_error is None, recovery.last_error
                assert time.monotonic() < deadline, engine.get_workflow(flow.id)
                time.sleep(.02)
            result = engine.get_workflow(flow.id)
            final = result.nodes['validate'].result
            assert final['valid'] is True
            assert final['answers'] == ['E2E-a', 'E2E-b']
            process_ids = {p.pid for p in processes}
            assert set(final['pids']) <= process_ids
            assert {int((control / name).read_text()) for name in ('a', 'b')} == process_ids
            assert result.nodes['a'].attempt_count == 2
            assert result.nodes['a'].retry_count == 1
            for name, node in result.nodes.items():
                assert node.status is NodeStatus.SUCCESS
                assert node.attempt_count == (2 if name == 'a' else 1)
                assert node.retry_count == (1 if name == 'a' else 0)
            (control / 'done').touch()
            for process in processes:
                _, stderr = process.communicate(timeout=10)
                assert process.returncode == 0, stderr.decode(errors='replace')
            assert recovery.shutdown(5)
            assert recovery.last_error is None
            assert recovery.list_incidents() == []
            sessions = engine.worker_registry.list_sessions()
            assert len(sessions) == 2
            assert all(s.state == 'STOPPED' for s in sessions)
            history = {node.task_id: engine.get_events(node.task_id)
                       for node in result.nodes.values()}
            events = history[result.nodes['a'].task_id]
            assert [e.event_type for e in events] == [
                TaskEventType.CREATED, TaskEventType.SUBMITTED, TaskEventType.STARTED,
                TaskEventType.FAILED, TaskEventType.RETRY_SCHEDULED,
                TaskEventType.STARTED, TaskEventType.SUCCEEDED,
            ]
            assert events[4].details['retry_delay'] == .05
            assert datetime.fromisoformat(events[5].timestamp).timestamp() >= events[4].details['retry_at']
            service.shutdown()
            reopened = TaskEngine(database_path=database, task_registry=registry)
            assert reopened.get_workflow(flow.id) == result
            assert reopened.submit_workflow(flow) == result
            assert reopened.start() == []
            assert reopened.create_recovery_system().tick().errors == ()
            for task_id, expected in history.items():
                assert reopened.get_events(task_id) == expected
            with closing(sqlite3.connect(database)) as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert db.execute('PRAGMA foreign_key_check').fetchall() == []
        finally:
            (control / 'done').touch()
            for process in processes:
                if process.poll() is None:
                    try:
                        process.communicate(timeout=7)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)
            recovery.shutdown(5)
            service.shutdown()
