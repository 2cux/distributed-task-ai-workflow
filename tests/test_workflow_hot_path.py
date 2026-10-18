"""Operation-count regressions for database work on the coordinator's write path."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
from threading import Event
import time
import unittest
from unittest.mock import patch

from workflow_engine import DAGNode, DAGWorkflow, Task, TaskEngine


class WorkflowHotPathTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix='.workflow-hot-path-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'engine.db'
        self.engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})

    def flow(self, identifier):
        return self.engine.submit_workflow(DAGWorkflow((DAGNode('root', 'abs', args=(-1,)),), id=identifier))

    def tick_steps(self):
        """Count SQLite VM instructions, independent of CPU speed and timer noise."""
        original, steps = self.engine._store._connection, [0]
        @contextmanager
        def connection():
            with original() as db:
                db.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 100) or 0, 100)
                try:
                    yield db
                finally:
                    db.set_progress_handler(None, 0)
        with patch.object(self.engine._store, '_connection', connection):
            self.engine.create_recovery_system().tick()
        return steps[0]

    def test_idle_reconciliation_cost_does_not_scale_with_success_history(self):
        self.flow('active')
        small = self.tick_steps()
        with self.engine._store._transaction() as db:
            definition = db.execute('SELECT definition FROM workflow_runs').fetchone()[0]
            db.executemany("INSERT INTO workflow_runs VALUES(?,?,'SUCCESS')",
                           [(f'history-{index}', definition) for index in range(5000)])
        large = self.tick_steps()
        self.assertLessEqual(large, small + 1000, (small, large))

    def test_unchanged_reconciliation_does_not_write_state_again(self):
        self.flow('waiting-for-worker')
        with self.engine._store._transaction() as db:
            before = db.total_changes
            run = self.engine._store.workflows._advance(db, 'waiting-for-worker')
            self.assertEqual(run.status, 'RUNNING')
            self.assertEqual(db.total_changes, before)

    def test_coordinator_does_not_construct_unused_usage_snapshots(self):
        self.flow('ready')
        with patch.object(self.engine._store.workflows, '_snapshot', side_effect=AssertionError('unused snapshot')):
            report = self.engine.create_recovery_system().tick()
        self.assertEqual(report.errors, ())
        self.assertEqual(report.workflow_ids, ('ready',))

    def test_legacy_database_upgrade_preserves_unfinished_insertion_order(self):
        for identifier in ('z-first', 'a-second', 'm-third', 'finished'):
            self.flow(identifier)
        with self.engine._store._transaction() as db:
            db.execute('DROP INDEX IF EXISTS workflow_unfinished')
            db.execute("UPDATE workflow_runs SET status='SUCCESS' WHERE id='finished'")
            db.execute("UPDATE workflow_runs SET status='FAILED' WHERE id='a-second'")
            db.execute("UPDATE workflow_runs SET status='PAUSED' WHERE id='m-third'")
        reopened = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        with reopened._store._connection() as db:
            self.assertEqual(reopened._store.workflows.unfinished_ids(db), ['z-first', 'a-second', 'm-third'])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0], 4)
            self.assertIsNotNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='workflow_unfinished'").fetchone())

    def test_waiting_submission_commits_between_workflow_reconciliations(self):
        self.flow('first')
        self.flow('second')
        entered, release = Event(), Event()
        observed = []
        original = self.engine._store.workflows._advance

        def advance(db, key, **options):
            result = original(db, key, **options)
            if key == 'first':
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('workflow reconciliation gate')
            elif key == 'second':
                observed.append(db.execute("SELECT 1 FROM tasks WHERE id='waiting-submission'").fetchone() is not None)
            return result

        with patch.object(self.engine._store.workflows, '_advance', advance), ThreadPoolExecutor(2) as pool:
            coordinator = pool.submit(self.engine.create_recovery_system().tick)
            try:
                self.assertTrue(entered.wait(3))
                submission = pool.submit(self.engine.submit, Task('new', abs, args=(-7,), id='waiting-submission'))
                deadline = time.monotonic() + 3
                while True:
                    with self.engine._store._writer_condition:
                        waiting = len(self.engine._store._writer_waiters) >= 2
                    if waiting:
                        break
                    self.assertLess(time.monotonic(), deadline, 'submission did not reach writer queue')
                    time.sleep(.001)
            finally:
                release.set()
            report = coordinator.result(timeout=5)
            submission.result(timeout=5)
        self.assertEqual(report.errors, ())
        self.assertEqual(report.workflow_ids, ('first', 'second'))
        self.assertEqual(observed, [True])

    def test_workflow_error_rolls_back_only_its_changes_and_next_tick_can_retry(self):
        failed = self.flow('broken')
        self.flow('healthy')
        original = self.engine._store.workflows._advance

        def advance(db, key, **options):
            if key == 'broken':
                db.execute("UPDATE tasks SET status='FAILED' WHERE id=?", (failed.nodes['root'].task_id,))
                raise RuntimeError('injected reconciliation failure')
            return original(db, key, **options)

        recovery = self.engine.create_recovery_system()
        with patch.object(self.engine._store.workflows, '_advance', advance):
            report = recovery.tick()
        self.assertEqual(report.workflow_ids, ('healthy',))
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(self.engine.get_task(failed.nodes['root'].task_id).status, 'PENDING')
        self.assertEqual(recovery.get_events()[-1]['action'], 'WORKFLOW_RECOVERY_FAILED')
        self.assertEqual(recovery.tick().errors, ())


if __name__ == '__main__':
    unittest.main()
