"""Commit failures and soft-timeout overlap checks, independent of crash tests."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from workflow_engine import Task, TaskEngine, TaskStatus, SQLiteTaskStore, RetryPolicy


class PersistentFailureBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "tasks.db"

    def test_commit_failure_does_not_reexecute_completed_business_code(self):
        calls = []
        def action():
            calls.append(1)
            return 7
        options = dict(database_path=self.path, task_registry={"action": action})
        engine = TaskEngine(**options, retry_policy=RetryPolicy(3))
        engine.submit(Task("action", action, id="key"))
        with patch.object(SQLiteTaskStore, "finish", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                engine.start()
        restarted = TaskEngine(**options)
        self.assertEqual(restarted.get_task("key").status, TaskStatus.RUNNING)
        self.assertEqual(restarted.start(), [])
        self.assertEqual(calls, [1])

    def test_timeout_does_not_release_claim_or_overlap_retry(self):
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        lock = threading.Lock()
        active = 0
        peak = 0
        calls = 0
        def action():
            nonlocal active, peak, calls
            with lock:
                active += 1
                peak = max(peak, active)
                calls += 1
                attempt = calls
            try:
                if attempt == 1:
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("test did not release invocation")
                return 1
            finally:
                with lock:
                    active -= 1
        engine = TaskEngine(database_path=self.path, task_registry={"action": action}, retry_policy=RetryPolicy(1))
        task = engine.submit(Task("action", action, timeout=0.01))
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(engine.start)
            try:
                self.assertTrue(entered.wait(2))
                # Waiting beyond the deadline must still leave the claim RUNNING.
                with self.assertRaises(TimeoutError):
                    future.result(timeout=0.05)
                self.assertFalse(future.done())
                self.assertEqual(engine.get_task(task.id).status, TaskStatus.RUNNING)
            finally:
                release.set()
            future.result(timeout=5)
        self.assertEqual(peak, 1)

    def test_mutating_memory_definition_cannot_change_committed_work(self):
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs})
        task = engine.submit(Task("abs", abs, args=(-5,)))
        task.callable = lambda: 99
        task.args = ()
        engine.start()
        self.assertEqual(task.result, 5)
