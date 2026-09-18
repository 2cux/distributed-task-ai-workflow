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

    def test_creates_task_with_explicit_fields(self) -> None:
        error = RuntimeError("previous failure")

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
