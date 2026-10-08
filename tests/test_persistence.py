"""Independent disk persistence and serialization tests."""
import tempfile
from pathlib import Path
import unittest

from workflow_engine import Task, TaskEngine, TaskStatus, PersistenceError, RetryPolicy


def add(a, b=0):
    return a + b


class TaskPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "tasks.db"

    def engine(self, **kwargs):
        return TaskEngine(database_path=self.path, task_registry={"add": add}, **kwargs)

    def test_pending_and_success_survive_new_engine(self):
        original = self.engine().submit(Task("add", add, id="stable", args=(2,), kwargs={"b": 3}, priority=7))
        restarted = self.engine(max_workers=2)
        self.assertEqual(restarted.pending_count, 1)
        restored = restarted.start()[0]
        self.assertEqual(restored.id, original.id)
        self.assertEqual(restored.priority, 7)
        self.assertEqual(restored.kwargs, {"b": 3})
        finished = self.engine()
        self.assertEqual(finished.pending_count, 0)
        saved = finished.get_task("stable")
        self.assertEqual(saved.result, 5)
        self.assertEqual(saved.status, TaskStatus.SUCCESS)
        self.assertEqual(saved.attempt_count, 1)

    def test_json_and_registry_validation_is_before_submission(self):
        engine = self.engine()
        for task in (Task("lambda", lambda: 1), Task("object", add, args=(object(),)),
                     Task("nan", add, args=(float("nan"),)), Task("key", add, args=({1: "x"},))):
            with self.subTest(task=task.name), self.assertRaises(PersistenceError):
                engine.submit(task)
            self.assertIsNone(engine.get_task(task.id))
        self.assertEqual(engine.pending_count, 0)

    def test_unserializable_result_is_durable_failure_without_retry(self):
        def opaque():
            return object()
        engine = TaskEngine(database_path=self.path, task_registry={"opaque": opaque},
                            retry_policy=RetryPolicy(3))
        task = engine.submit(Task("opaque", opaque))
        engine.start()
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, PersistenceError)
        restored = engine.get_task(task.id)
        self.assertEqual(restored.status, TaskStatus.FAILED)
        self.assertEqual(restored.attempt_count, 1)
        self.assertIsNone(restored.result)

    def test_retry_budget_and_errors_survive_restart(self):
        def fail():
            raise ValueError("failure")
        options = dict(database_path=self.path, task_registry={"fail": fail})
        first = TaskEngine(**options, retry_policy=RetryPolicy(2))
        task = first.submit(Task("fail", fail))
        # Restart with a different default: the accepted budget must remain 2.
        restarted = TaskEngine(**options, retry_policy=RetryPolicy(9), max_workers=2)
        self.assertEqual(len(restarted.start()), 3)
        saved = restarted.get_task(task.id)
        self.assertEqual(saved.attempt_count, 3)
        self.assertEqual(saved.retry_count, 2)
        self.assertEqual(saved.status, TaskStatus.FAILED)
        self.assertIn("ValueError: failure", str(saved.error))
        self.assertIn("ValueError: failure", str(saved.last_error))

    def test_recovery_preserves_priority_and_fifo(self):
        engine = self.engine()
        for ident, priority in (("low", 0), ("first", 3), ("second", 3)):
            engine.submit(Task(ident, add, id=ident, args=(1,), priority=priority))
        self.assertEqual([t.id for t in self.engine().start()], ["first", "second", "low"])

    def test_no_retry_policy_does_not_retry_even_with_task_budget(self):
        def fail():
            raise ValueError("failure")
        engine = TaskEngine(database_path=self.path, task_registry={"fail": fail})
        task = engine.submit(Task("fail", fail, max_retries=3))
        self.assertEqual(len(engine.start()), 1)
        self.assertEqual(engine.get_task(task.id).retry_count, 0)
