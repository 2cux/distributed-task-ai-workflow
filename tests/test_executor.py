"""同步任务执行器测试。"""

import unittest

from workflow_engine import Executor, Task, TaskStatus, execute


class ExecutorTests(unittest.TestCase):
    def test_executes_pending_task_and_stores_result(self) -> None:
        observed_statuses: list[TaskStatus] = []
        task: Task

        def add(left: int, right: int) -> int:
            observed_statuses.append(task.status)
            return left + right

        task = Task(name="add", callable=add, args=(2,), kwargs={"right": 3})

        returned_task = Executor().execute(task)

        self.assertIs(returned_task, task)
        self.assertEqual(observed_statuses, [TaskStatus.RUNNING])
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, 5)
        self.assertIsNone(task.error)

    def test_records_exception_and_marks_task_failed(self) -> None:
        expected_error = RuntimeError("database unavailable")

        def fail() -> None:
            raise expected_error

        task = Task(name="fail", callable=fail)

        Executor().execute(task)

        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIs(task.error, expected_error)
        self.assertIsNone(task.result)

    def test_rejects_non_pending_tasks_without_mutating_them(self) -> None:
        task = Task(name="done", callable=lambda: None, status=TaskStatus.SUCCESS, result="kept")

        with self.assertRaisesRegex(ValueError, "PENDING"):
            Executor().execute(task)

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "kept")

    def test_executes_retrying_task_and_resets_the_previous_error(self) -> None:
        previous = RuntimeError("first attempt")
        task = Task(
            name="flaky",
            callable=lambda: "recovered",
            status=TaskStatus.RETRYING,
            retry_count=1,
            error=previous,
            last_error=previous,
        )

        returned = Executor().execute(task)

        self.assertIs(returned, task)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "recovered")
        self.assertIsNone(task.error)
        self.assertIs(task.last_error, previous)

    def test_rejects_failed_task_without_consuming_a_retry(self) -> None:
        task = Task(name="done", callable=lambda: None, status=TaskStatus.FAILED)

        with self.assertRaisesRegex(ValueError, "RETRYING"):
            Executor().execute(task)

        self.assertIs(task.status, TaskStatus.FAILED)

    def test_rejects_non_task_value(self) -> None:
        with self.assertRaisesRegex(TypeError, "Task"):
            Executor().execute(object())  # type: ignore[arg-type]

    def test_module_helper_executes_task(self) -> None:
        task = Task(name="value", callable=lambda: "done")

        self.assertIs(execute(task), task)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "done")
