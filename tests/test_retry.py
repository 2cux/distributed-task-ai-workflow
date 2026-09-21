"""重试策略原语测试。"""

import unittest

from workflow_engine import RetryPolicy, RetryPolicyError, Task, TaskStatus, should_retry


def no_operation() -> None:
    """测试用任务函数。"""


def failed_task(
    *,
    max_retries: int | None = None,
    retry_count: int = 0,
    error: BaseException | None = None,
    name: str = "failed",
) -> Task:
    """构造一个已失败的任务。"""
    return Task(
        name=name,
        callable=no_operation,
        status=TaskStatus.FAILED,
        error=error,
        max_retries=max_retries,
        retry_count=retry_count,
    )


class RetryPolicyConfigurationTests(unittest.TestCase):
    def test_defaults_to_no_retry(self) -> None:
        policy = RetryPolicy()

        self.assertEqual(policy.max_retries, 0)

    def test_exposes_configured_default(self) -> None:
        self.assertEqual(RetryPolicy(max_retries=3).max_retries, 3)

    def test_rejects_invalid_max_retries(self) -> None:
        for value, error_type in ((True, TypeError), ("3", TypeError), (-1, ValueError)):
            with self.subTest(value=value):
                with self.assertRaises(error_type):
                    RetryPolicy(max_retries=value)  # type: ignore[arg-type]


class RetryLimitResolutionTests(unittest.TestCase):
    def test_falls_back_to_policy_default_when_task_has_no_limit(self) -> None:
        policy = RetryPolicy(max_retries=2)

        self.assertEqual(policy.limit_for(failed_task()), 2)

    def test_task_limit_overrides_policy_default(self) -> None:
        policy = RetryPolicy(max_retries=2)

        self.assertEqual(policy.limit_for(failed_task(max_retries=5)), 5)
        self.assertEqual(policy.limit_for(failed_task(max_retries=0)), 0)

    def test_uses_error_when_no_policy_default_is_configured(self) -> None:
        policy = RetryPolicy()

        self.assertFalse(policy.can_retry(failed_task()))


class RetryDecisionTests(unittest.TestCase):
    def test_retries_failed_task_while_budget_remains(self) -> None:
        policy = RetryPolicy(max_retries=2)

        self.assertTrue(policy.should_retry(failed_task()))
        self.assertTrue(policy.should_retry(failed_task(retry_count=1)))

    def test_stops_retrying_when_budget_is_exhausted(self) -> None:
        policy = RetryPolicy(max_retries=2)

        self.assertFalse(policy.should_retry(failed_task(retry_count=2)))
        self.assertFalse(policy.should_retry(failed_task(retry_count=3)))

    def test_task_level_zero_limit_disables_retry(self) -> None:
        policy = RetryPolicy(max_retries=3)

        self.assertFalse(policy.should_retry(failed_task(max_retries=0)))

    def test_only_failed_tasks_are_eligible_for_retry(self) -> None:
        policy = RetryPolicy(max_retries=3)
        statuses = (
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.RETRYING,
            TaskStatus.SUCCESS,
        )

        for status in statuses:
            with self.subTest(status=status):
                task = Task(
                    name="task",
                    callable=no_operation,
                    status=status,
                    retry_count=1 if status is TaskStatus.RETRYING else 0,
                )

                self.assertFalse(policy.should_retry(task))

    def test_rejects_non_task_value(self) -> None:
        with self.assertRaisesRegex(TypeError, "Task"):
            RetryPolicy(max_retries=1).should_retry(object())  # type: ignore[arg-type]


class BeginRetryTests(unittest.TestCase):
    def test_consumes_one_retry_and_marks_task_waiting(self) -> None:
        policy = RetryPolicy(max_retries=2)
        task = failed_task(error=RuntimeError("first attempt"))

        returned = policy.begin_retry(task)

        self.assertIs(returned, task)
        self.assertIs(task.status, TaskStatus.RETRYING)
        self.assertEqual(task.retry_count, 1)
        self.assertIsInstance(task.last_error, RuntimeError)

    def test_records_only_the_error_that_triggered_the_retry(self) -> None:
        first = RuntimeError("first attempt")
        second = RuntimeError("second attempt")
        policy = RetryPolicy(max_retries=3)
        task = failed_task(error=first)

        policy.begin_retry(task)
        task.status = TaskStatus.FAILED
        task.error = second
        policy.begin_retry(task)

        self.assertIs(task.error, second)
        self.assertIs(task.last_error, second)
        self.assertEqual(task.retry_count, 2)

    def test_keeps_previous_last_error_when_error_is_missing(self) -> None:
        original = RuntimeError("original")
        policy = RetryPolicy(max_retries=2)
        task = failed_task()
        task.last_error = original

        policy.begin_retry(task)

        self.assertIs(task.last_error, original)

    def test_rejects_task_without_remaining_retries(self) -> None:
        policy = RetryPolicy(max_retries=1)
        task = failed_task(retry_count=1)

        with self.assertRaisesRegex(RetryPolicyError, "无法重试"):
            policy.begin_retry(task)

        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertEqual(task.retry_count, 1)

    def test_rejects_task_that_did_not_fail(self) -> None:
        policy = RetryPolicy(max_retries=1)
        task = Task(name="pending", callable=no_operation)

        with self.assertRaises(RetryPolicyError):
            policy.begin_retry(task)

        self.assertIs(task.status, TaskStatus.PENDING)

    def test_module_helper_reports_retry_decision(self) -> None:
        self.assertTrue(should_retry(failed_task(), max_retries=1))
        self.assertFalse(should_retry(failed_task(), max_retries=0))
        self.assertFalse(should_retry(Task(name="pending", callable=no_operation), max_retries=1))
