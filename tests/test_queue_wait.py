"""TaskQueue 阻塞等待原语的独立测试。"""

import threading
import time
import unittest

from workflow_engine import Task, TaskQueue


class TaskQueueWaitTests(unittest.TestCase):
    def test_waiting_consumer_is_woken_by_enqueue(self) -> None:
        queue = TaskQueue()
        waiting = threading.Event()
        received: list[Task | None] = []

        def consume() -> None:
            waiting.set()
            received.append(queue.dequeue_wait())

        consumer = threading.Thread(target=consume)
        consumer.start()
        self.assertTrue(waiting.wait(timeout=1))
        self.assertTrue(consumer.is_alive())

        task = Task(name="queued-later", callable=lambda: None)
        queue.enqueue(task)

        consumer.join(timeout=1)
        self.assertFalse(consumer.is_alive())
        self.assertEqual(received, [task])

    def test_wait_returns_none_after_the_requested_timeout(self) -> None:
        queue = TaskQueue()
        started_at = time.monotonic()

        result = queue.dequeue_wait(timeout=0.02)

        self.assertIsNone(result)
        self.assertGreaterEqual(time.monotonic() - started_at, 0.015)

    def test_wait_rejects_invalid_timeout_values(self) -> None:
        queue = TaskQueue()

        with self.assertRaisesRegex(TypeError, "timeout"):
            queue.dequeue_wait(timeout=True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "timeout"):
            queue.dequeue_wait(timeout=-0.01)
        with self.assertRaisesRegex(TypeError, "stop_requested"):
            queue.dequeue_wait(stop_requested=object())  # type: ignore[arg-type]
