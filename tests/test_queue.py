"""任务队列测试。"""

import unittest

from workflow_engine import Task, TaskQueue, TaskStatus


def no_operation() -> None:
    """测试用任务函数。"""


class TaskQueueTests(unittest.TestCase):
    def test_new_queue_is_empty_and_has_no_tasks(self) -> None:
        queue = TaskQueue()

        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)

    def test_enqueue_saves_task_and_updates_queue_state(self) -> None:
        queue = TaskQueue()
        task = Task(name="first", callable=no_operation)

        self.assertIsNone(queue.enqueue(task))

        self.assertFalse(queue.is_empty())
        self.assertEqual(queue.size(), 1)

    def test_dequeue_returns_tasks_in_first_in_first_out_order(self) -> None:
        queue = TaskQueue()
        first = Task(name="first", callable=no_operation)
        second = Task(name="second", callable=no_operation)
        queue.enqueue(first)
        queue.enqueue(second)

        self.assertIs(queue.dequeue(), first)
        self.assertEqual(queue.size(), 1)
        self.assertIs(queue.dequeue(), second)
        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)

    def test_dequeue_empty_queue_returns_none(self) -> None:
        queue = TaskQueue()

        self.assertIsNone(queue.dequeue())
        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)

    def test_enqueue_rejects_non_task_value_without_changing_queue(self) -> None:
        queue = TaskQueue()

        with self.assertRaisesRegex(TypeError, "task 必须是 Task 实例"):
            queue.enqueue(object())  # type: ignore[arg-type]

        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)

    def test_enqueue_rejects_task_that_is_not_waiting_to_run(self) -> None:
        queue = TaskQueue()
        running = Task(name="running", callable=no_operation, status=TaskStatus.RUNNING)
        succeeded = Task(name="succeeded", callable=no_operation, status=TaskStatus.SUCCESS)
        failed = Task(name="failed", callable=no_operation, status=TaskStatus.FAILED)

        for task in (running, succeeded, failed):
            with self.subTest(status=task.status):
                with self.assertRaisesRegex(ValueError, "PENDING 或 RETRYING"):
                    queue.enqueue(task)

        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)

    def test_enqueue_accepts_retrying_task(self) -> None:
        queue = TaskQueue()
        task = Task(name="retrying", callable=no_operation, status=TaskStatus.RETRYING, retry_count=1)

        queue.enqueue(task)

        self.assertEqual(queue.size(), 1)
        self.assertIn(task, queue)
        self.assertIs(queue.dequeue(), task)

    def test_enqueue_rejects_task_already_in_queue(self) -> None:
        queue = TaskQueue()
        task = Task(name="once", callable=no_operation)
        queue.enqueue(task)

        with self.assertRaisesRegex(ValueError, "已经在队列中"):
            queue.enqueue(task)

        self.assertEqual(queue.size(), 1)

    def test_dequeue_allows_the_same_task_to_be_enqueued_again(self) -> None:
        queue = TaskQueue()
        task = Task(name="retryable", callable=no_operation)
        queue.enqueue(task)

        self.assertIs(queue.dequeue(), task)

        queue.enqueue(task)

        self.assertEqual(queue.size(), 1)

    def test_contains_reports_queue_membership(self) -> None:
        queue = TaskQueue()
        queued = Task(name="queued", callable=no_operation)
        other = Task(name="other", callable=no_operation)
        queue.enqueue(queued)

        self.assertTrue(queue.contains(queued))
        self.assertIn(queued, queue)
        self.assertFalse(queue.contains(other))
        self.assertNotIn(other, queue)

        with self.assertRaisesRegex(TypeError, "Task"):
            queue.contains("task")  # type: ignore[arg-type]
