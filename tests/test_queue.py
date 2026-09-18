"""任务队列测试。"""

import unittest

from workflow_engine import Task, TaskQueue


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

        with self.assertRaisesRegex(TypeError, "Task"):
            queue.enqueue(object())  # type: ignore[arg-type]

        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)
