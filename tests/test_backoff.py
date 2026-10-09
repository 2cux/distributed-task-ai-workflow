"""退避策略、延迟队列和执行链路的独立行为测试。"""

import math
from contextlib import closing
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from workflow_engine import (ConcurrentWorker, Executor, RetryPolicy, SQLiteTaskStore,
                             Task, TaskEngine, TaskEventType, TaskQueue, TaskStatus,
                             Worker, WorkerLoop)


class BackoffPolicyTests(unittest.TestCase):
    def test_default_keeps_immediate_retry(self):
        policy = RetryPolicy(3)
        self.assertEqual([policy.delay_for(n) for n in (1, 2, 100)], [0, 0, 0])
        task = Task("failure", abs, status=TaskStatus.FAILED)
        policy.begin_retry(task)
        self.assertIsNone(task.retry_at)

    def test_exponential_delay_is_capped(self):
        policy = RetryPolicy(5, initial_delay=0.5, backoff_factor=3, max_delay=4)
        self.assertEqual([policy.delay_for(n) for n in range(1, 6)], [0.5, 1.5, 4, 4, 4])
        self.assertEqual(policy.delay_for(10**100), 4)

    def test_factor_one_provides_fixed_delay(self):
        policy = RetryPolicy(5, initial_delay=2, backoff_factor=1)
        self.assertEqual([policy.delay_for(n) for n in (1, 2, 10)], [2, 2, 2])
        self.assertEqual(RetryPolicy(initial_delay=5, max_delay=3).delay_for(1), 3)
        self.assertEqual(RetryPolicy(initial_delay=5, max_delay=0).delay_for(1), 0)

    def test_rejects_invalid_configuration(self):
        for field in ("initial_delay", "backoff_factor", "max_delay"):
            for value in (True, "2", None):
                with self.subTest(field=field, value=value), self.assertRaises(TypeError):
                    RetryPolicy(**{field: value})
            for value in (-1, math.nan, math.inf, -math.inf):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    RetryPolicy(**{field: value})
        with self.assertRaises(ValueError):
            RetryPolicy(backoff_factor=0.5)
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                RetryPolicy().delay_for(value)

    def test_scheduling_records_deadline_without_sleep(self):
        task = Task("failure", abs, status=TaskStatus.FAILED, retry_count=1)
        policy = RetryPolicy(3, initial_delay=2, backoff_factor=3)
        with patch("workflow_engine.retry.time.time", return_value=100):
            policy.begin_retry(task)
        self.assertEqual(task.retry_at, 106)
        self.assertEqual(task.retry_count, 2)
        event = task.events[-1]
        self.assertIs(event.event_type, TaskEventType.RETRY_SCHEDULED)
        self.assertEqual(event.details["retry_delay"], 6)
        self.assertEqual(event.details["retry_at"], 106)
        with self.assertRaises(RuntimeError):
            policy.begin_retry(task)
        self.assertEqual((task.retry_count, task.retry_at), (2, 106))


class DelayedQueueTests(unittest.TestCase):
    def test_not_due_task_does_not_block_ready_peer_and_remains_counted(self):
        queue = TaskQueue()
        delayed = Task("delayed", abs, status=TaskStatus.FAILED, priority=100)
        peer = Task("peer", abs)
        with patch("workflow_engine.queue.time.time", return_value=100), \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as monotonic:
            RetryPolicy(1, initial_delay=2).begin_retry(delayed)
            queue.enqueue(delayed)
            queue.enqueue(peer)
            self.assertEqual(queue.size(), 2)
            self.assertIs(queue.dequeue(), peer)
            self.assertIsNone(queue.dequeue())
            self.assertFalse(queue.is_empty())
            self.assertIn(delayed, queue)
            with self.assertRaises(ValueError):
                queue.enqueue(delayed)
            self.assertEqual(queue.next_ready_delay(), 2)
            monotonic.return_value = 12
            self.assertIs(queue.dequeue(), delayed)
            self.assertTrue(queue.is_empty())

    def test_due_tasks_keep_priority_and_same_priority_fifo(self):
        queue = TaskQueue()
        tasks = [Task(str(n), abs, status=TaskStatus.FAILED, priority=priority)
                 for n, priority in enumerate((1, 2, 1))]
        with patch("workflow_engine.queue.time.time", return_value=100), \
             patch("workflow_engine.queue.time.monotonic", return_value=10) as monotonic:
            for task in tasks:
                RetryPolicy(1, initial_delay=2).begin_retry(task)
                queue.enqueue(task)
            monotonic.return_value = 12
            self.assertEqual([queue.dequeue() for _ in tasks], [tasks[1], tasks[0], tasks[2]])

    def test_wait_timeout_leaves_delayed_task_queued(self):
        task = Task("later", abs, status=TaskStatus.FAILED)
        RetryPolicy(1, initial_delay=60).begin_retry(task)
        queue = TaskQueue()
        queue.enqueue(task)
        self.assertIsNone(queue.dequeue_wait(timeout=0.01))
        self.assertIn(task, queue)


class BackoffExecutionTests(unittest.TestCase):
    def test_exhausted_budget_stops_after_delayed_retries(self):
        engine = TaskEngine(retry_policy=RetryPolicy(2, initial_delay=0.005))
        task = engine.submit(Task("failure", lambda: 1 / 0))
        self.assertEqual(len(engine.start()), 3)
        self.assertEqual((task.attempt_count, task.retry_count, task.status),
                         (3, 2, TaskStatus.FAILED))
        self.assertEqual(engine.pending_count, 0)

    def test_sync_and_concurrent_run_wait_for_retry_and_execute_peer_first(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                calls, order = [], []

                def flaky():
                    calls.append(time.monotonic())
                    order.append("attempt")
                    if len(calls) == 1:
                        raise RuntimeError("temporary")
                    return "recovered"

                engine = TaskEngine(max_workers=workers,
                    retry_policy=RetryPolicy(1, initial_delay=0.04))
                task = engine.submit(Task("flaky", flaky, priority=10))
                engine.submit(Task("peer", lambda: order.append("peer")))
                processed = engine.start()
                self.assertEqual(len(processed), 3)
                self.assertLess(order.index("peer"), len(order) - 1)
                self.assertGreaterEqual(calls[1] - calls[0], 0.04)
                self.assertEqual((task.status, task.result, task.retry_count),
                                 (TaskStatus.SUCCESS, "recovered", 1))
                self.assertIsNone(task.retry_at)

    def test_concurrent_retry_uses_free_slot_while_peer_still_runs(self):
        release, retried = threading.Event(), threading.Event()
        calls = 0

        def flaky():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary")
            retried.set()

        engine = TaskEngine(max_workers=2, retry_policy=RetryPolicy(1, initial_delay=0.02))
        engine.submit(Task("slow", lambda: release.wait(2)))
        engine.submit(Task("flaky", flaky))
        thread = threading.Thread(target=engine.start)
        thread.start()
        try:
            self.assertTrue(retried.wait(1))
            self.assertTrue(thread.is_alive())
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())

    def test_stop_wakes_backoff_wait_and_keeps_retry_for_next_worker(self):
        for mode in ("sync", "concurrent", "loop"):
            with self.subTest(mode=mode):
                scheduled = threading.Event()

                class ObservedQueue(TaskQueue):
                    def enqueue(self, task):
                        super().enqueue(task)
                        if task.status is TaskStatus.RETRYING:
                            scheduled.set()

                queue = ObservedQueue()
                task = Task("failure", lambda: 1 / 0)
                queue.enqueue(task)
                policy = RetryPolicy(1, initial_delay=60)
                worker = (ConcurrentWorker(queue, Executor(), max_workers=2, retry_policy=policy)
                          if mode == "concurrent" else Worker(queue, Executor(), policy))
                if mode == "loop":
                    loop = WorkerLoop(worker)
                    loop.start()
                    try:
                        self.assertTrue(scheduled.wait(1))
                    finally:
                        stopped = loop.shutdown(1)
                    self.assertTrue(stopped)
                else:
                    thread = threading.Thread(target=worker.run)
                    thread.start()
                    try:
                        self.assertTrue(scheduled.wait(1))
                    finally:
                        worker.stop()
                        thread.join(1)
                    self.assertFalse(thread.is_alive())
                self.assertEqual((task.attempt_count, task.retry_count), (1, 1))
                self.assertIn(task, queue)


class PersistentBackoffTests(unittest.TestCase):
    def test_legacy_database_migrates_with_immediate_retry(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-") as directory:
            path = Path(directory) / "legacy.db"
            # 原有数据库结构不包含 retry_at / retry_config。
            with closing(sqlite3.connect(path)) as db:
                db.execute("""CREATE TABLE tasks (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE, definition TEXT NOT NULL, status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0, retry_count INTEGER NOT NULL DEFAULT 0,
                    result TEXT, error TEXT, last_error TEXT, owner TEXT, retry_limit INTEGER NOT NULL
                )""")
            calls = 0

            def flaky():
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise RuntimeError("temporary")
                return "ok"

            engine = TaskEngine(database_path=path, task_registry={"flaky": flaky},
                                retry_policy=RetryPolicy(1))
            task = engine.submit(Task("flaky", flaky))
            engine.start()
            self.assertIs(engine.get_task(task.id).status, TaskStatus.SUCCESS)
            retry = next(e for e in engine.get_events(task.id)
                         if e.event_type is TaskEventType.RETRY_SCHEDULED)
            self.assertEqual(retry.details["retry_delay"], 0)
            self.assertIsNone(retry.details["retry_at"])
            with closing(sqlite3.connect(path)) as db:
                columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
                self.assertTrue({"retry_at", "retry_config"} <= columns)

    def test_restart_preserves_deadline_and_original_policy(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-") as directory:
            path = Path(directory) / "tasks.db"
            calls = 0

            def flaky():
                nonlocal calls
                calls += 1
                if calls <= 2:
                    raise RuntimeError("temporary")
                return "ok"

            options = dict(database_path=path, task_registry={"flaky": flaky})
            first = TaskEngine(**options, retry_policy=RetryPolicy(2, initial_delay=0.03))
            task = first.submit(Task("flaky", flaky))
            # 冻结时间验证恢复边界，避免数据库 I/O 耗时让短延迟提前到期。
            with patch("workflow_engine.retry.time.time", return_value=100), \
                 patch("workflow_engine.queue.time.monotonic", return_value=10):
                first._scheduler._worker.process_next()
                saved = first.get_task(task.id)
                deadline = saved.retry_at
                restarted = TaskEngine(**options, retry_policy=RetryPolicy(9))
                self.assertEqual(restarted.get_task(task.id).retry_at, deadline)
                self.assertIsNone(restarted._queue.dequeue())
            restarted.start()
            saved = restarted.get_task(task.id)
            self.assertEqual((saved.status, saved.retry_count, saved.result), (TaskStatus.SUCCESS, 2, "ok"))
            retries = [e for e in restarted.get_events(task.id)
                       if e.event_type is TaskEventType.RETRY_SCHEDULED]
            self.assertEqual([e.details["retry_delay"] for e in retries], [0.03, 0.06])
            self.assertEqual(retries[0].details["retry_at"], deadline)

    def test_persistent_claim_rejects_retry_before_deadline(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-") as directory:
            engine = TaskEngine(database_path=Path(directory) / "tasks.db",
                task_registry={"fail": abs}, retry_policy=RetryPolicy(1, initial_delay=60))
            task = engine.submit(Task("failure", abs, args=(None,)))
            engine._scheduler._worker.process_next()
            saved = engine.get_task(task.id)
            self.assertIsNone(engine._store.claim(saved, "premature"))
            self.assertEqual(engine.get_task(task.id).attempt_count, 1)

    def test_interrupted_recovery_uses_persisted_backoff(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".task-tests-") as directory:
            store = SQLiteTaskStore(Path(directory) / "tasks.db", {"abs": abs})
            task = store.submit(Task("work", abs, args=(-1,), idempotent=True), 2,
                retry_policy=RetryPolicy(2, initial_delay=3))
            store.claim(task, "dead-worker")
            with patch("workflow_engine.retry.time.time", return_value=100):
                recovered = store.recover_interrupted()[0]
            self.assertEqual((recovered.retry_count, recovered.retry_at), (1, 103))
            self.assertEqual(store.get(task.id).retry_at, 103)
