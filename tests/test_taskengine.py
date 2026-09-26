"""TaskEngine 的边界测试：只验证装配与转发，不重复原语自身的单元测试。"""

import threading
import unittest

from workflow_engine import (
    RetryPolicy,
    Task,
    TaskEngine,
    TaskStatus,
    TaskTimeoutError,
    TimeoutPolicy,
)


class TaskEngineTests(unittest.TestCase):
    def test_submit_only_enqueues_until_start_and_returns_the_same_task(self) -> None:
        observed: list[str] = []
        engine = TaskEngine()
        task = Task(name="queued", callable=lambda: observed.append("ran"))

        self.assertIs(engine.submit(task), task)
        self.assertEqual(engine.pending_count, 1)
        self.assertEqual(observed, [])

        self.assertEqual(engine.start(), [task])
        self.assertEqual(observed, ["ran"])
        self.assertEqual(engine.pending_count, 0)
        self.assertIs(task.status, TaskStatus.SUCCESS)

    def test_wires_retry_policy_without_reimplementing_retry_logic(self) -> None:
        attempts = 0

        def flaky() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("first attempt")
            return "recovered"

        engine = TaskEngine(retry_policy=RetryPolicy(max_retries=1))
        task = engine.submit(Task(name="flaky", callable=flaky))

        self.assertEqual(engine.start(), [task, task])
        self.assertEqual(attempts, 2)
        self.assertEqual(task.retry_count, 1)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "recovered")

    def test_wires_timeout_policy_and_task_level_timeout_precedence(self) -> None:
        engine = TaskEngine(timeout_policy=TimeoutPolicy(timeout=1))
        release = threading.Event()
        self.addCleanup(release.set)
        task = engine.submit(
            Task(name="limited", callable=lambda: release.wait(1), timeout=0.01)
        )

        self.assertEqual(engine.start(), [task])
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, TaskTimeoutError)
        self.assertEqual(task.error.timeout, 0.01)

    def test_submit_many_uses_the_existing_queue_order(self) -> None:
        observed: list[str] = []
        engine = TaskEngine()
        tasks = [
            Task(name="first", callable=lambda: observed.append("first")),
            Task(name="second", callable=lambda: observed.append("second")),
        ]

        self.assertEqual(engine.submit_many(tasks), tasks)
        self.assertEqual(engine.start(), tasks)
        self.assertEqual(observed, ["first", "second"])

    def test_selects_concurrent_worker_when_multiple_slots_are_requested(self) -> None:
        engine = TaskEngine(max_workers=2)
        started = threading.Event()
        release = threading.Event()
        started_count = 0
        count_lock = threading.Lock()

        def blocking_work() -> None:
            nonlocal started_count
            with count_lock:
                started_count += 1
                if started_count == 2:
                    started.set()
            release.wait(1)

        self.addCleanup(release.set)
        engine.submit_many(
            [
                Task(name="first", callable=blocking_work),
                Task(name="second", callable=blocking_work),
            ]
        )
        runner = threading.Thread(target=engine.start)
        runner.start()

        self.assertTrue(started.wait(0.5), "两个任务应由并发 Worker 同时开始")
        release.set()
        runner.join(1)
        self.assertFalse(runner.is_alive())

    def test_rejects_invalid_engine_configuration(self) -> None:
        with self.assertRaisesRegex(TypeError, "RetryPolicy"):
            TaskEngine(retry_policy=object())  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "TimeoutPolicy"):
            TaskEngine(timeout_policy=object())  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "max_workers"):
            TaskEngine(max_workers=True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "max_workers"):
            TaskEngine(max_workers=0)
