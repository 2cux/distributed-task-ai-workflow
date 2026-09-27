"""Worker 生命周期的独立测试。"""

import threading
import unittest

from workflow_engine import Executor, Task, TaskQueue, Worker, WorkerStatus


class WorkerLifecycleTests(unittest.TestCase):
    def test_worker_is_created_before_it_is_started(self) -> None:
        worker = Worker(TaskQueue(), Executor())

        self.assertIs(worker.status, WorkerStatus.CREATED)

    def test_run_exposes_running_to_task_and_finishes_stopped(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor())
        observed: list[WorkerStatus] = []
        queue.enqueue(Task(name="observe", callable=lambda: observed.append(worker.status)))

        worker.run()

        self.assertEqual(observed, [WorkerStatus.RUNNING])
        self.assertIs(worker.status, WorkerStatus.STOPPED)

    def test_stop_requests_graceful_shutdown_after_current_task(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor())
        started = threading.Event()
        release = threading.Event()
        completed: list[Task] = []

        def blocking_work() -> None:
            started.set()
            release.wait(timeout=1)

        current = Task(name="current", callable=blocking_work)
        pending = Task(name="pending", callable=lambda: None)
        queue.enqueue(current)
        queue.enqueue(pending)
        runner = threading.Thread(target=lambda: completed.extend(worker.run()))
        runner.start()
        self.assertTrue(started.wait(timeout=1))

        worker.stop()
        self.assertIs(worker.status, WorkerStatus.STOPPING)
        release.set()
        runner.join(timeout=1)

        self.assertFalse(runner.is_alive())
        self.assertEqual(completed, [current])
        self.assertEqual(queue.size(), 1)
        self.assertIs(worker.status, WorkerStatus.STOPPED)

    def test_stopped_worker_cannot_be_started_again(self) -> None:
        worker = Worker(TaskQueue(), Executor())
        worker.stop()

        with self.assertRaisesRegex(RuntimeError, "STOPPED"):
            worker.run()
