"""WorkerLoop 的独立后台线程与生命周期测试。"""

import threading
import time
import unittest

from workflow_engine import Executor, Task, TaskQueue, Worker, WorkerLoop, WorkerStatus


class WorkerLoopTests(unittest.TestCase):
    def test_loop_executes_submitted_task_on_its_own_thread(self) -> None:
        queue = TaskQueue()
        completed = threading.Event()
        execution_threads: list[int] = []
        caller_thread = threading.get_ident()
        loop = WorkerLoop(Worker(queue, Executor()), idle_wait=0.001)

        loop.start()
        queue.enqueue(
            Task(
                name="background",
                callable=lambda: (execution_threads.append(threading.get_ident()), completed.set()),
            )
        )

        self.assertTrue(completed.wait(timeout=1))
        loop.stop()
        self.assertTrue(loop.join(timeout=1))
        self.assertEqual(len(execution_threads), 1)
        self.assertNotEqual(execution_threads[0], caller_thread)
        self.assertEqual(loop.processed_count, 1)
        self.assertIs(loop.status, WorkerStatus.STOPPED)

    def test_idle_loop_is_woken_immediately_when_a_task_is_enqueued(self) -> None:
        queue = TaskQueue()
        completed = threading.Event()
        loop = WorkerLoop(Worker(queue, Executor()), idle_wait=10)
        loop.start()

        queue.enqueue(Task(name="wake-on-enqueue", callable=completed.set))

        self.assertTrue(completed.wait(timeout=0.5))
        loop.stop()
        self.assertTrue(loop.join(timeout=1))

    def test_stop_wakes_a_loop_waiting_indefinitely_for_work(self) -> None:
        loop = WorkerLoop(Worker(TaskQueue(), Executor()), idle_wait=10)
        loop.start()

        started_at = time.monotonic()
        loop.stop()

        self.assertTrue(loop.join(timeout=0.5))
        self.assertLess(time.monotonic() - started_at, 0.5)
        self.assertIs(loop.status, WorkerStatus.STOPPED)

    def test_task_enqueued_after_stop_remains_pending(self) -> None:
        queue = TaskQueue()
        completed = threading.Event()
        loop = WorkerLoop(Worker(queue, Executor()), idle_wait=10)
        loop.start()
        loop.stop()

        task = Task(name="submitted-after-stop", callable=completed.set)
        queue.enqueue(task)

        self.assertTrue(loop.join(timeout=1))
        self.assertFalse(completed.is_set())
        self.assertIn(task, queue)

    def test_stop_wakes_an_idle_loop_without_waiting_for_poll_timeout(self) -> None:
        loop = WorkerLoop(Worker(TaskQueue(), Executor()), idle_wait=5)
        loop.start()

        started_at = time.monotonic()
        loop.stop()

        self.assertTrue(loop.join(timeout=0.5))
        self.assertLess(time.monotonic() - started_at, 0.5)
        self.assertIs(loop.status, WorkerStatus.STOPPED)

    def test_stop_allows_current_task_to_finish_but_not_the_next_task(self) -> None:
        queue = TaskQueue()
        started = threading.Event()
        release = threading.Event()
        completed: list[str] = []

        def blocking_work() -> None:
            started.set()
            release.wait(timeout=1)
            completed.append("current")

        queue.enqueue(Task(name="current", callable=blocking_work))
        queue.enqueue(Task(name="pending", callable=lambda: completed.append("pending")))
        loop = WorkerLoop(Worker(queue, Executor()), idle_wait=0.001)
        loop.start()
        self.assertTrue(started.wait(timeout=1))

        loop.stop()
        self.assertIs(loop.status, WorkerStatus.STOPPING)
        release.set()

        self.assertTrue(loop.join(timeout=1))
        self.assertEqual(completed, ["current"])
        self.assertEqual(queue.size(), 1)
        self.assertEqual(loop.processed_count, 1)

    def test_only_one_concurrent_start_can_transition_the_lifecycle(self) -> None:
        loop = WorkerLoop(Worker(TaskQueue(), Executor()), idle_wait=0.001)
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        outcomes_lock = threading.Lock()

        def start() -> None:
            barrier.wait()
            try:
                loop.start()
            except RuntimeError:
                outcome = "rejected"
            else:
                outcome = "started"
            with outcomes_lock:
                outcomes.append(outcome)

        starters = [threading.Thread(target=start) for _ in range(2)]
        for starter in starters:
            starter.start()
        for starter in starters:
            starter.join(timeout=1)

        self.assertEqual(outcomes.count("started"), 1)
        self.assertEqual(outcomes.count("rejected"), 1)
        loop.stop()
        self.assertTrue(loop.join(timeout=1))

    def test_rejects_invalid_dependencies_and_timing_values(self) -> None:
        with self.assertRaisesRegex(TypeError, "Worker"):
            WorkerLoop(object())  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "idle_wait"):
            WorkerLoop(Worker(TaskQueue(), Executor()), idle_wait=-0.1)

