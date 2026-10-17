"""Operation-count regressions for database work on the coordinator's write path."""
from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from workflow_engine import DAGNode, DAGWorkflow, TaskEngine


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


if __name__ == '__main__':
    unittest.main()
