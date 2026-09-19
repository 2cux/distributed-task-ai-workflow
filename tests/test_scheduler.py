"""最小 Scheduler 测试。"""

import unittest

from workflow_engine import Executor, Scheduler, Task, TaskQueue, TaskStatus, Worker


class SchedulerTests(unittest.TestCase):
    def make_scheduler(self) -> tuple[Scheduler, TaskQueue]:
        queue = TaskQueue()
        return Scheduler(queue, Worker(queue, Executor())), queue

    def test_submit_accepts_task_without_executing_it(self) -> None:
        scheduler, queue = self.make_scheduler()
        task = Task(name="value", callable=lambda: "completed")

        self.assertIsNone(scheduler.submit(task))

        self.assertEqual(queue.size(), 1)
        self.assertIs(task.status, TaskStatus.PENDING)
        self.assertIsNone(task.result)

    def test_start_delegates_execution_to_worker_and_returns_processed_tasks(self) -> None:
        scheduler, queue = self.make_scheduler()
        observed: list[str] = []
        first = Task(name="first", callable=lambda: observed.append("first"))
        second = Task(name="second", callable=lambda: observed.append("second"))
        scheduler.submit(first)
        scheduler.submit(second)

        processed = scheduler.start()

        self.assertEqual(processed, [first, second])
        self.assertEqual(observed, ["first", "second"])
        self.assertTrue(queue.is_empty())
        self.assertTrue(all(task.status is TaskStatus.SUCCESS for task in processed))

    def test_start_on_empty_queue_does_not_execute_any_task(self) -> None:
        scheduler, queue = self.make_scheduler()

        self.assertEqual(scheduler.start(), [])
        self.assertTrue(queue.is_empty())

    def test_rejects_invalid_dependencies_or_mismatched_worker_queue(self) -> None:
        queue = TaskQueue()

        with self.assertRaisesRegex(TypeError, "TaskQueue"):
            Scheduler(object(), Worker(queue, Executor()))  # type: ignore[arg-type]

        with self.assertRaisesRegex(TypeError, "Worker"):
            Scheduler(queue, object())  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "queue"):
            Scheduler(queue, Worker(TaskQueue(), Executor()))

    def test_submit_rejects_non_task_and_leaves_queue_unchanged(self) -> None:
        scheduler, queue = self.make_scheduler()

        with self.assertRaisesRegex(TypeError, "Task"):
            scheduler.submit(object())  # type: ignore[arg-type]

        self.assertTrue(queue.is_empty())
