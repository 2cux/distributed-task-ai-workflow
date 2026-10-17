"""Public input boundaries must not poison durable work or stop consumers."""
from pathlib import Path
import json
import tempfile
import unittest

from management_test_support import ManagementTestCase
from workflow_engine import (
    DAGNode, DAGWorkflow, LLMBudget, LLM_HANDLER, Task, TaskEngine, TaskQueue,
)


class ManagementNumericBoundaryTests(ManagementTestCase):
    def test_invalid_numeric_definitions_roll_back_and_service_still_executes(self):
        self.start_service()
        properties = self.call('/openapi.json')[1]['components']['schemas']['Node']['properties']
        self.assertEqual(properties['priority']['minimum'], -(2**63))
        self.assertEqual(properties['priority']['maximum'], 2**63-1)
        for field, values in (
            ('priority', (2**63, -(2**63)-1, 10**400)),
            ('max_retries', (2**63,)),
            ('timeout', (10**400,)),
            ('scheduled_at', (10**400,)),
        ):
            for value in values:
                with self.subTest(field=field, value=value):
                    definition = self.workflow('invalid')
                    # Verify rollback even after a valid earlier node was inserted.
                    definition['nodes'].append(dict(id='invalid', handler='abs', **{field: value}))
                    code, payload, _ = self.call('/api/v1/workflows', definition)
                    self.assertEqual(code, 400, payload)
                    self.assertEqual(self.call('/api/v1/tasks')[1]['items'], [])
                    self.assertEqual(self.call('/api/v1/workflows')[1]['items'], [])
        self.submit('healthy')
        worker = self.worker()
        self.wait_for(lambda: self.call('/api/v1/workflows/healthy')[1]['status'] == 'SUCCESS')
        self.assertTrue(worker.shutdown(5))
        self.assertIsNone(worker.last_error)
        saved = self.call('/api/v1/workflows/healthy')[1]
        self.assertEqual((saved['nodes'][0]['result'], saved['nodes'][0]['attempt_count']), (7, 1))
        self.assertTrue(self.service.shutdown(5))
        self.start_service()
        self.assertEqual(self.call('/api/v1/workflows/healthy')[1], saved)


class PersistentNumericBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix='.e2e-boundaries-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'tasks.db'

    def test_out_of_range_priority_is_rejected_before_it_can_poison_claims(self):
        engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        for priority in (2**63, -(2**63)-1):
            with self.subTest(priority=priority), self.assertRaises(ValueError):
                engine.submit(Task('oversized-priority', abs, args=(-1,), priority=priority))
        self.assertEqual(engine.pending_count, 0)
        healthy = engine.submit(Task('healthy', abs, args=(-7,)))
        engine.start()
        self.assertEqual(engine.get_task(healthy.id).result, 7)

    def test_supported_priority_extremes_do_not_starve_work_during_token_wait(self):
        engine = TaskEngine(database_path=self.path,
                            task_registry={LLM_HANDLER: lambda: None, 'business': lambda: 7})
        flow = DAGWorkflow((DAGNode('first', LLM_HANDLER, priority=2**63-1),
                            DAGNode('waiting', LLM_HANDLER, priority=2**63-1)),
                           llm_budget=LLMBudget(max_total_tokens=100))
        engine.submit_workflow(flow)
        engine.submit_workflow(DAGWorkflow((DAGNode('healthy', 'business', priority=-(2**63)),)))
        session = engine.worker_registry.register('boundary-worker')
        first = engine._store.claim_next(session.session_id, 'first')
        self.assertEqual(first.name, 'first')
        second = engine._store.claim_next(session.session_id, 'second')
        self.assertEqual(second.name, 'healthy')
        self.assertIsNone(engine._store.claim_next(session.session_id, 'third'))
        self.assertEqual(engine.get_workflow(flow.id).nodes['waiting'].attempt_count, 0)

    def test_legacy_oversized_priority_does_not_crash_consumer_cursor(self):
        engine = TaskEngine(database_path=self.path,
                            task_registry={LLM_HANDLER: lambda: None, 'business': lambda: 7})
        flow = DAGWorkflow((DAGNode('first', LLM_HANDLER), DAGNode('waiting', LLM_HANDLER)),
                           llm_budget=LLMBudget(max_total_tokens=100))
        submitted = engine.submit_workflow(flow)
        # Simulate data accepted by a previous release. New writes are rejected.
        with engine._store._transaction() as db:
            for node in submitted.nodes.values():
                row = db.execute('SELECT definition FROM tasks WHERE id=?', (node.task_id,)).fetchone()
                definition = json.loads(row['definition'])
                definition['priority'] = 2**100
                db.execute('UPDATE tasks SET definition=? WHERE id=?',
                           (json.dumps(definition), node.task_id))
        engine.submit_workflow(DAGWorkflow((DAGNode('healthy', 'business'),)))
        session = engine.worker_registry.register('legacy-worker')
        self.assertEqual(engine._store.claim_next(session.session_id, 'first').name, 'first')
        self.assertEqual(engine._store.claim_next(session.session_id, 'second').name, 'healthy')
        self.assertIsNone(engine._store.claim_next(session.session_id, 'third'))


class QueueWaitBoundaryTests(unittest.TestCase):
    def test_nonfinite_wait_is_rejected_even_if_stopped_or_ready(self):
        for value in (float('nan'), float('inf'), -float('inf')):
            for ready in (False, True):
                with self.subTest(timeout=value, ready=ready):
                    queue = TaskQueue()
                    if ready:
                        queue.enqueue(Task('ready', lambda: 1))
                    # A stop predicate avoids hanging a broken NaN implementation.
                    with self.assertRaises(ValueError):
                        queue.dequeue_wait(value, stop_requested=lambda: True)
                    self.assertEqual(queue.size(), int(ready))
