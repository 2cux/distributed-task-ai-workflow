"""Abrupt process exit tests (no shutdown hooks or simulated exception)."""
import os
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from workflow_engine import TaskEngine, TaskStatus, UncertainExecutionError


class CrashRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "tasks.db"

    def crash(self, idempotent=False, retries=1):
        script = '''
import os, sqlite3, sys
from workflow_engine import Task, TaskEngine, RetryPolicy
path = sys.argv[1]
def effect(key):
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE IF NOT EXISTS effects (id TEXT PRIMARY KEY)")
        db.execute("INSERT OR IGNORE INTO effects VALUES (?)", (key,))
    os._exit(23)
engine = TaskEngine(database_path=path, task_registry={"effect": effect, "abs": abs}, retry_policy=RetryPolicy(1))
engine.submit(Task("effect", effect, id="key", args=("key",), priority=1,
                   idempotent=sys.argv[2] == "True", max_retries=int(sys.argv[3])))
engine.submit(Task("remaining", abs, id="remaining", args=(-5,)))
engine.start()
'''
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
        result = subprocess.run([sys.executable, "-c", script, str(self.path), str(idempotent), str(retries)],
                                env=env, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 23, result.stderr.decode(errors="replace"))

    def engine(self, recover=False):
        def effect(key):
            with closing(sqlite3.connect(self.path)) as db:
                with db:
                    db.execute("INSERT OR IGNORE INTO effects VALUES (?)", (key,))
                    return db.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
        return TaskEngine(database_path=self.path, task_registry={"effect": effect, "abs": abs},
                          recover_interrupted=recover)

    def test_default_startup_never_steals_running_task(self):
        self.crash()
        engine = self.engine()
        self.assertEqual(engine.get_task("key").status, TaskStatus.RUNNING)
        self.assertEqual([t.id for t in engine.start()], ["remaining"])
        self.assertEqual(engine.get_task("remaining").result, 5)

    def test_non_idempotent_interrupted_task_is_quarantined(self):
        self.crash()
        engine = self.engine(recover=True)
        task = engine.get_task("key")
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, UncertainExecutionError)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(task.retry_count, 0)
        self.assertEqual([t.id for t in engine.start()], ["remaining"])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM effects").fetchone()[0], 1)

    def test_idempotent_replay_has_one_business_effect(self):
        self.crash(idempotent=True)
        engine = self.engine(recover=True)
        self.assertEqual(engine.get_task("key").status, TaskStatus.RETRYING)
        # Recovery repeated before start must not consume the budget twice.
        engine = self.engine(recover=True)
        engine.start()
        task = engine.get_task("key")
        self.assertEqual(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, 1)
        self.assertEqual(task.attempt_count, 2)
        self.assertEqual(task.retry_count, 1)
        self.assertEqual(self.engine(recover=True).pending_count, 0)

    def test_interrupted_idempotent_task_respects_exhausted_budget(self):
        self.crash(idempotent=True, retries=0)
        engine = self.engine(recover=True)
        self.assertEqual(engine.get_task("key").status, TaskStatus.FAILED)
        self.assertEqual(engine.get_task("key").retry_count, 0)

    def test_crash_after_retry_commit_before_enqueue_recovers_retry(self):
        script = '''
import os, sys
from workflow_engine import Task, TaskEngine, RetryPolicy, SQLiteTaskStore
def action():
    raise ValueError("first attempt failed")
finish = SQLiteTaskStore.finish
def crash_after_commit(self, task, owner):
    finish(self, task, owner)
    os._exit(23)
SQLiteTaskStore.finish = crash_after_commit
engine = TaskEngine(database_path=sys.argv[1], task_registry={"action": action}, retry_policy=RetryPolicy(1))
engine.submit(Task("action", action, id="retry"))
engine.start()
'''
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
        result = subprocess.run([sys.executable, "-c", script, str(self.path)], env=env,
                                capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 23, result.stderr.decode(errors="replace"))
        def action():
            return 7
        engine = TaskEngine(database_path=self.path, task_registry={"action": action})
        restored = engine.get_task("retry")
        self.assertEqual(restored.status, TaskStatus.RETRYING)
        self.assertEqual(restored.retry_count, 1)
        self.assertIn("first attempt failed", str(restored.last_error))
        engine.start()
        restored = engine.get_task("retry")
        self.assertEqual(restored.result, 7)
        self.assertEqual(restored.attempt_count, 2)
