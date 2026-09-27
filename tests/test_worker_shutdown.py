"""WorkerLoop 优雅关闭的独立行为测试。"""

import threading
import time
import unittest

from workflow_engine import Executor, Task, TaskQueue, Worker, WorkerLoop, WorkerStatus


class WorkerShutdownTests(unittest.TestCase):
    def test_shutdown_waits_for_current_task_and_preserves_pending_work(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor())
        loop = WorkerLoop(worker)
        started = threading.Event()
        release = threading.Event()
        completed: list[str] = []

        def current_work() -> None:
            started.set()
            release.wait(timeout=1)
            completed.append("current")

        current = Task(name="current", callable=current_work)
        pending = Task(name="pending", callable=lambda: completed.append("pending"))
        queue.enqueue(current)
        queue.enqueue(pending)
        loop.start()
        self.assertTrue(started.wait(timeout=1))

        self.assertFalse(loop.shutdown(timeout=0.01))
        self.assertIs(loop.status, WorkerStatus.STOPPING)
        self.assertIs(worker.status, WorkerStatus.STOPPING)
        release.set()

        self.assertTrue(loop.join(timeout=1))
        self.assertEqual(completed, ["current"])
        self.assertIn(pending, queue)
        self.assertIs(loop.status, WorkerStatus.STOPPED)
        self.assertIs(worker.status, WorkerStatus.STOPPED)

    def test_shutdown_of_an_idle_loop_returns_promptly(self) -> None:
        loop = WorkerLoop(Worker(TaskQueue(), Executor()))
        loop.start()

        started_at = time.monotonic()

        self.assertTrue(loop.shutdown(timeout=0.5))

        self.assertLess(time.monotonic() - started_at, 0.5)
        self.assertIs(loop.status, WorkerStatus.STOPPED)

    def test_stopping_the_underlying_worker_also_stops_its_loop(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor())
        loop = WorkerLoop(worker)
        loop.start()

        worker.stop()

        self.assertTrue(loop.join(timeout=0.5))
        self.assertIs(loop.status, WorkerStatus.STOPPED)
        self.assertIs(worker.status, WorkerStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
