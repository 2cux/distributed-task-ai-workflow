"""Task 创建行为测试。"""

import unittest

from workflow_engine.task import Task, TaskStatus


def add(left: int, right: int = 0) -> int:
    return left + right


class TaskCreationTests(unittest.TestCase):
    def test_creates_task_with_default_fields(self) -> None:
        task = Task(name="add", callable=add)

        self.assertIsInstance(task.id, str)
        self.assertTrue(task.id)
        self.assertEqual(task.name, "add")
        self.assertIs(task.callable, add)
        self.assertEqual(task.args, ())
        self.assertEqual(task.kwargs, {})
        self.assertEqual(task.priority, 0)
        self.assertIs(task.status, TaskStatus.PENDING)
        self.assertIsNone(task.result)
        self.assertIsNone(task.error)
        self.assertIsNone(task.max_retries)
        self.assertEqual(task.retry_count, 0)
        self.assertIsNone(task.last_error)
        self.assertIsNone(task.remaining_retries)

    def test_creates_task_with_explicit_fields(self) -> None:
        error = RuntimeError("previous failure")
        last_error = RuntimeError("earlier failure")

        task = Task(
            id="task-42",
            name="add numbers",
            callable=add,
            args=(2,),
            kwargs={"right": 3},
            priority=10,
            status=TaskStatus.FAILED,
            result=5,
            error=error,
            max_retries=3,
            retry_count=1,
            last_error=last_error,
        )

        self.assertEqual(task.id, "task-42")
        self.assertEqual(task.name, "add numbers")
        self.assertIs(task.callable, add)
        self.assertEqual(task.args, (2,))
        self.assertEqual(task.kwargs, {"right": 3})
        self.assertEqual(task.priority, 10)
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertEqual(task.result, 5)
        self.assertIs(task.error, error)
        self.assertEqual(task.max_retries, 3)
        self.assertEqual(task.retry_count, 1)
        self.assertIs(task.last_error, last_error)
        self.assertEqual(task.remaining_retries, 2)

    def test_generates_distinct_ids_for_new_tasks(self) -> None:
        first = Task(name="first", callable=add)
        second = Task(name="second", callable=add)

        self.assertNotEqual(first.id, second.id)

    def test_rejects_invalid_required_identity_and_callable_fields(self) -> None:
        for field, value, error_type in (
            ("id", "", ValueError),
            ("id", 1, ValueError),
            ("name", "", ValueError),
            ("name", None, ValueError),
            ("callable", "not callable", TypeError),
        ):
            with self.subTest(field=field, value=value):
                kwargs = {"name": "add", "callable": add}
                kwargs[field] = value
                with self.assertRaises(error_type):
                    Task(**kwargs)

    def test_rejects_invalid_argument_and_priority_fields(self) -> None:
        cases = (
            ("args", [1], TypeError),
            ("args", "value", TypeError),
            ("kwargs", [("right", 3)], TypeError),
            ("kwargs", {1: 3}, TypeError),
            ("priority", True, TypeError),
            ("priority", "high", TypeError),
        )

        for field, value, error_type in cases:
            with self.subTest(field=field, value=value):
                with self.assertRaises(error_type):
                    Task(name="add", callable=add, **{field: value})

    def test_rejects_invalid_status_and_error_combinations(self) -> None:
        with self.assertRaises(TypeError):
            Task(name="add", callable=add, status="PENDING")

        with self.assertRaises(TypeError):
            Task(name="add", callable=add, error="failure")

        with self.assertRaises(ValueError):
            Task(
                name="add",
                callable=add,
                status=TaskStatus.SUCCESS,
                error=RuntimeError("failure"),
            )

    def test_rejects_invalid_retry_fields(self) -> None:
        cases = (
            ("max_retries", True, TypeError),
            ("max_retries", "3", TypeError),
            ("max_retries", -1, ValueError),
            ("retry_count", True, TypeError),
            ("retry_count", "1", TypeError),
            ("retry_count", -1, ValueError),
            ("last_error", "failure", TypeError),
        )

        for field, value, error_type in cases:
            with self.subTest(field=field, value=value):
                with self.assertRaises(error_type):
                    Task(name="add", callable=add, **{field: value})

    def test_rejects_retry_count_above_max_retries(self) -> None:
        with self.assertRaisesRegex(ValueError, "retry_count"):
            Task(name="add", callable=add, max_retries=1, retry_count=2)

    def test_rejects_retrying_status_without_consumed_retry(self) -> None:
        with self.assertRaisesRegex(ValueError, "retry_count"):
            Task(name="add", callable=add, status=TaskStatus.RETRYING)

        retrying = Task(name="add", callable=add, status=TaskStatus.RETRYING, retry_count=1)

        self.assertIs(retrying.status, TaskStatus.RETRYING)

    def test_remaining_retries_never_goes_negative(self) -> None:
        exhausted = Task(name="add", callable=add, status=TaskStatus.FAILED, max_retries=2, retry_count=2)
        overridden = Task(name="add", callable=add, status=TaskStatus.FAILED, max_retries=5, retry_count=2)

        self.assertEqual(exhausted.remaining_retries, 0)
        self.assertFalse(exhausted.can_retry())
        self.assertEqual(overridden.remaining_retries, 3)
        self.assertTrue(overridden.can_retry())

    def test_can_retry_depends_on_status_and_remaining_budget(self) -> None:
        unlimited = Task(name="add", callable=add, status=TaskStatus.FAILED)
        budgeted = Task(name="add", callable=add, status=TaskStatus.FAILED, max_retries=1)
        exhausted = Task(name="add", callable=add, status=TaskStatus.FAILED, max_retries=1, retry_count=1)
        succeeded = Task(name="add", callable=add, status=TaskStatus.SUCCESS)

        self.assertTrue(unlimited.can_retry())
        self.assertTrue(budgeted.can_retry())
        self.assertFalse(exhausted.can_retry())
        self.assertFalse(succeeded.can_retry())

    def test_mark_retrying_consumes_one_retry_and_updates_status(self) -> None:
        task = Task(name="add", callable=add, status=TaskStatus.FAILED, max_retries=2)

        task.mark_retrying()

        self.assertIs(task.status, TaskStatus.RETRYING)
        self.assertEqual(task.retry_count, 1)
        self.assertEqual(task.remaining_retries, 1)

    def test_mark_retrying_rejects_non_failed_task_without_mutating_it(self) -> None:
        task = Task(name="add", callable=add)

        with self.assertRaisesRegex(ValueError, "FAILED"):
            task.mark_retrying()

        self.assertIs(task.status, TaskStatus.PENDING)
        self.assertEqual(task.retry_count, 0)

    def test_repr_includes_retry_information_only_when_relevant(self) -> None:
        self.assertNotIn("retry_count", repr(Task(name="add", callable=add)))

        described = Task(
            name="add",
            callable=add,
            status=TaskStatus.FAILED,
            error=RuntimeError("failure"),
            max_retries=2,
            retry_count=1,
        )

        self.assertIn("status=FAILED", repr(described))
        self.assertIn("retry_count=1", repr(described))
        self.assertIn("max_retries=2", repr(described))
