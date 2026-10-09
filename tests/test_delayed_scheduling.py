"""首次延迟调度：描述、队列、执行和持久化的独立测试。"""

import math
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from workflow_engine import (Executor, IdempotencyConflict, RetryPolicy, SQLiteTaskStore,
                             Task, TaskEngine, TaskEventType, TaskQueue, TaskStatus,
                             Worker, WorkerLoop)


class ScheduledTaskTests(unittest.TestCase):
    def test_default_and_valid_deadlines(self):
        self.assertIsNone(Task("now", abs).scheduled_at)
        for deadline in (0, 100, 100.5):
            task = Task("later", abs, scheduled_at=deadline)
            self.assertEqual(task.scheduled_at, deadline)
            self.assertEqual((task.status, task.attempt_count, task.retry_count),
                             (TaskStatus.PENDING, 0, 0))

    def test_rejects_invalid_deadline_on_creation_and_assignment(self):
        task = Task("later", abs)
        for value, error in ((True, TypeError), ("100", TypeError),
                             (-1, ValueError), (math.nan, ValueError),
                             (math.inf, ValueError), (-math.inf, ValueError)):
            with self.subTest(value=value):
                with self.assertRaises(error):
                    Task("invalid", abs, scheduled_at=value)
                with self.assertRaises(error):
                    task.scheduled_at = value
        self.assertIsNone(task.scheduled_at)


class ScheduledQueueTests(unittest.TestCase):
    def test_future_high_priority_does_not_block_ready_task(self):
        queue = TaskQueue()
        future = Task("later", abs, priority=100, scheduled_at=102)
        ready = Task("now", abs)
        with patch("workflow_engine.queue.time.time", return_value=100), \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as clock:
            queue.enqueue(future)
            queue.enqueue(ready)
            self.assertEqual(queue.size(), 2)
            self.assertIs(queue.dequeue(), ready)
            self.assertIsNone(queue.dequeue())
            self.assertIn(future, queue)
            self.assertEqual(queue.next_ready_delay(), 2)
            with self.assertRaises(ValueError):
                queue.enqueue(future)
            clock.return_value = 11.999
            self.assertIsNone(queue.dequeue())
            clock.return_value = 12
            self.assertIs(queue.dequeue(), future)
        self.assertTrue(queue.is_empty())
        self.assertEqual(future.events[-1].details["scheduled_at"], 102)

    def test_due_tasks_keep_priority_and_submission_fifo(self):
        queue = TaskQueue()
        tasks = [Task(str(i), abs, priority=p, scheduled_at=due)
                 for i, (p, due) in enumerate(((1, 103), (2, 102), (1, 101)))]
        with patch("workflow_engine.queue.time.time", return_value=100), \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as clock:
            for task in tasks:
                queue.enqueue(task)
            clock.return_value = 13
            self.assertEqual([queue.dequeue() for _ in tasks], [tasks[1], tasks[0], tasks[2]])

    def test_past_deadline_is_immediately_ready(self):
        queue = TaskQueue()
        task = Task("past", abs, scheduled_at=0)
        queue.enqueue(task)
        self.assertIs(queue.dequeue(), task)

    def test_wait_timeout_preserves_pending_task(self):
        queue = TaskQueue()
        task = Task("later", abs, scheduled_at=time.time() + 60)
        queue.enqueue(task)
        self.assertIsNone(queue.dequeue_wait(timeout=0.01))
        self.assertIn(task, queue)
        self.assertEqual(task.attempt_count, 0)


class ScheduledExecutionTests(unittest.TestCase):
    def test_sync_and_concurrent_engine_wait_until_due(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                order, starts = [], []
                deadline = time.time() + 0.04

                def later():
                    starts.append(time.time())
                    order.append("later")
                    return "done"

                engine = TaskEngine(max_workers=workers)
                task = engine.submit(Task("later", later, priority=100, scheduled_at=deadline))
                engine.submit(Task("now", lambda: order.append("now")))
                self.assertEqual(engine.pending_count, 2)
                self.assertEqual(len(engine.start()), 2)
                self.assertEqual(order, ["now", "later"])
                self.assertGreaterEqual(starts[0], deadline)
                self.assertEqual((task.status, task.result, task.retry_count),
                                 (TaskStatus.SUCCESS, "done", 0))

    def test_retry_uses_its_own_deadline(self):
        queue = TaskQueue()
        task = Task("failure", lambda: 1 / 0, scheduled_at=102)
        worker = Worker(queue, Executor(), RetryPolicy(1, initial_delay=5))
        with patch("workflow_engine.queue.time.time", return_value=100) as wall, \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as clock:
            queue.enqueue(task)
            wall.return_value, clock.return_value = 102, 12
            worker.process_next()
            self.assertEqual((task.status, task.retry_at, task.retry_count),
                             (TaskStatus.RETRYING, 107, 1))
            self.assertIsNone(worker.process_next())
            wall.return_value, clock.return_value = 107, 17
            worker.process_next()
            self.assertEqual((task.status, task.attempt_count), (TaskStatus.FAILED, 2))

    def test_loop_wakes_for_new_ready_task_and_stop_preserves_future(self):
        queue = TaskQueue()
        future = Task("future", abs, args=(-1,), scheduled_at=time.time() + 60)
        queue.enqueue(future)
        loop = WorkerLoop(Worker(queue, Executor()))
        ran = threading.Event()
        loop.start()
        try:
            queue.enqueue(Task("ready", ran.set))
            self.assertTrue(ran.wait(1))
        finally:
            stopped = loop.shutdown(1)
        self.assertTrue(stopped)
        self.assertIn(future, queue)
        self.assertEqual((future.status, future.attempt_count), (TaskStatus.PENDING, 0))


class PersistentSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix=".task-tests-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "tasks.db"

    def test_restart_preserves_deadline_and_claim_checks_stored_time(self):
        options = dict(database_path=self.path, task_registry={"abs": abs})
        with patch("workflow_engine.queue.time.time", return_value=100), \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as clock:
            first = TaskEngine(**options)
            task = first.submit(Task("later", abs, args=(-3,), scheduled_at=102))
            restarted = TaskEngine(**options)
            saved = restarted.get_task(task.id)
            self.assertEqual(saved.scheduled_at, 102)
            self.assertIsNone(restarted._queue.dequeue())
            saved.scheduled_at = 0  # Caller mutation cannot bypass the database guard.
            self.assertIsNone(restarted._store.claim(saved, "early"))
            self.assertEqual(restarted.get_task(task.id).attempt_count, 0)
            clock.return_value = 12
            with patch("workflow_engine.persistence.time.time", return_value=102):
                restarted.start()
        saved = restarted.get_task(task.id)
        self.assertEqual((saved.status, saved.result, saved.scheduled_at),
                         (TaskStatus.SUCCESS, 3, 102))
        submitted = next(e for e in saved.events if e.event_type is TaskEventType.SUBMITTED)
        self.assertEqual(submitted.details["scheduled_at"], 102)

    def test_duplicate_definition_includes_deadline(self):
        store = SQLiteTaskStore(self.path, {"abs": abs})
        store.submit(Task("later", abs, id="same", scheduled_at=102))
        same = store.submit(Task("later", abs, id="same", scheduled_at=102))
        self.assertEqual(same.scheduled_at, 102)
        with self.assertRaises(IdempotencyConflict):
            store.submit(Task("later", abs, id="same", scheduled_at=103))

    def test_old_database_upgrade_preserves_immediate_idempotent_submission(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TABLE tasks (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE, definition TEXT NOT NULL, status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0, retry_count INTEGER NOT NULL DEFAULT 0,
                result TEXT, error TEXT, last_error TEXT, owner TEXT, retry_limit INTEGER NOT NULL
            )""")
            db.execute("""INSERT INTO tasks(id, definition, status, retry_limit)
                VALUES ('legacy', ?, 'PENDING', 0)""", (
                '{"args": [-3], "handler": "abs", "idempotent": false, '
                '"kwargs": {}, "max_retries": null, "name": "old", '
                '"priority": 0, "timeout": null}',))
            db.commit()
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs})
        restored = engine.submit(Task("old", abs, id="legacy", args=(-3,)))
        self.assertIsNone(restored.scheduled_at)
        self.assertEqual(len(engine.start()), 1)
        self.assertEqual(engine.get_task("legacy").result, 3)


if __name__ == "__main__":
    unittest.main()
