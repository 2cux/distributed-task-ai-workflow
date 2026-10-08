"""Independent duplicate submission and cross-engine ownership tests."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest

from workflow_engine import Task, TaskEngine, SQLiteTaskStore, IdempotencyConflict, TaskStatus, PersistenceError


class PersistentIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "tasks.db"
        self.calls = 0
        self.lock = threading.Lock()
        def action(value=1):
            with self.lock:
                self.calls += 1
            return value
        self.action = action
        self.registry = {"action": action}

    def engine(self, **kwargs):
        return TaskEngine(database_path=self.path, task_registry=self.registry, **kwargs)

    def test_same_id_is_deduplicated_before_and_after_restart(self):
        engine = self.engine()
        task = Task("action", self.action, id="business-key")
        self.assertIs(engine.submit(task), task)
        engine.submit(Task("action", self.action, id="business-key"))
        self.assertEqual(engine.pending_count, 1)
        engine.start()
        restarted = self.engine()
        duplicate = restarted.submit(Task("action", self.action, id="business-key"))
        self.assertEqual(duplicate.status, TaskStatus.SUCCESS)
        self.assertEqual(duplicate.result, 1)
        self.assertEqual(restarted.start(), [])
        self.assertEqual(self.calls, 1)

    def test_same_id_with_different_definition_is_rejected(self):
        engine = self.engine()
        engine.submit(Task("action", self.action, id="same", args=(1,)))
        with self.assertRaises(IdempotencyConflict):
            engine.submit(Task("action", self.action, id="same", args=(2,)))

    def test_independent_engines_cannot_execute_same_task(self):
        first = self.engine(max_workers=3)
        for i in range(12):
            first.submit(Task("action", self.action, id=str(i), args=(i,)))
        second = self.engine(max_workers=3)
        barrier = threading.Barrier(2)
        def start(engine):
            barrier.wait(timeout=5)
            return engine.start()
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(start, engine) for engine in (first, second)]
            for future in futures:
                future.result(timeout=15)
        self.assertEqual(self.calls, 12)
        for i in range(12):
            self.assertEqual(first.get_task(str(i)).attempt_count, 1)

    def test_concurrent_submissions_only_create_one_record(self):
        engines = [self.engine() for _ in range(4)]
        with ThreadPoolExecutor(4) as pool:
            futures = [pool.submit(engine.submit, Task("action", self.action, id="same")) for engine in engines]
            for future in futures:
                future.result(timeout=5)
        with ThreadPoolExecutor(4) as pool:
            for future in [pool.submit(engine.start) for engine in engines]:
                future.result(timeout=10)
        self.assertEqual(self.calls, 1)

    def test_stale_owner_cannot_overwrite_recovery(self):
        store = SQLiteTaskStore(self.path, self.registry)
        task = store.submit(Task("action", self.action))
        claimed = store.claim(task, "old-owner")
        store.recover_interrupted()
        claimed.status = TaskStatus.SUCCESS
        claimed.result = 7
        with self.assertRaises(PersistenceError):
            store.finish(claimed, "old-owner")
        self.assertEqual(store.get(task.id).status, TaskStatus.FAILED)
