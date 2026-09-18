"""同步 Worker 测试。"""

import unittest
import threading

from workflow_engine import Executor, Task, TaskQueue, TaskStatus, Worker


class WorkerTests(unittest.TestCase):
    def test_process_next_dequeues_and_executes_one_task_synchronously(self) -> None:
        queue = TaskQueue()
        execution_threads: list[int] = []
        calling_thread = threading.get_ident()

        def work(value: int) -> int:
            execution_threads.append(threading.get_ident())
            return value * 2

        task = Task(name="double", callable=work, args=(3,))
        queue.enqueue(task)
        worker = Worker(queue, Executor())

        returned_task = worker.process_next()

        self.assertIs(returned_task, task)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, 6)
        self.assertEqual(queue.size(), 0)
        self.assertEqual(execution_threads, [calling_thread])

    def test_process_next_returns_none_for_an_empty_queue(self) -> None:
        worker = Worker(TaskQueue(), Executor())

        self.assertIsNone(worker.process_next())

    def test_run_processes_tasks_in_queue_order_and_drains_queue(self) -> None:
        queue = TaskQueue()
        observed: list[str] = []

        def work(name: str) -> str:
            observed.append(name)
            return name.upper()

        first = Task(name="first", callable=work, args=("first",))
        second = Task(name="second", callable=work, args=("second",))
        queue.enqueue(first)
        queue.enqueue(second)

        processed = Worker(queue, Executor()).run()

        self.assertEqual(processed, [first, second])
        self.assertEqual(observed, ["first", "second"])
        self.assertTrue(queue.is_empty())
        self.assertEqual([task.result for task in processed], ["FIRST", "SECOND"])

    def test_run_continues_after_a_failed_task(self) -> None:
        queue = TaskQueue()

        def fail() -> None:
            raise RuntimeError("expected failure")

        failed = Task(name="fail", callable=fail)
        succeeding = Task(name="succeed", callable=lambda: "done")
        queue.enqueue(failed)
        queue.enqueue(succeeding)

        processed = Worker(queue, Executor()).run()

        self.assertEqual(processed, [failed, succeeding])
        self.assertIs(failed.status, TaskStatus.FAILED)
        self.assertIsInstance(failed.error, RuntimeError)
        self.assertIs(succeeding.status, TaskStatus.SUCCESS)
        self.assertEqual(succeeding.result, "done")
        self.assertTrue(queue.is_empty())

    def test_rejects_invalid_dependencies(self) -> None:
        with self.assertRaisesRegex(TypeError, "TaskQueue"):
            Worker(object(), Executor())  # type: ignore[arg-type]

        with self.assertRaisesRegex(TypeError, "Executor"):
            Worker(TaskQueue(), object())  # type: ignore[arg-type]
