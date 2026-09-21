"""任务超时原语测试：单次执行的超时上限。"""

import threading
import unittest

from workflow_engine import (
    RetryPolicy,
    Scheduler,
    Task,
    TaskQueue,
    TaskStatus,
    TaskTimeoutError,
    TimeoutExecutor,
    TimeoutPolicy,
    Worker,
    run_with_timeout,
    timeout_for,
)


def no_operation() -> None:
    """测试用任务函数。"""


class BlockingCallable:
    """阻塞到被释放为止的可调用对象，用来制造"跑不完"的一次执行。"""

    def __init__(self, result: str = "late") -> None:
        self.result = result
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def __call__(self) -> str:
        self.calls += 1
        self.started.set()
        try:
            self.release.wait(5)

            return self.result
        finally:
            self.finished.set()


class SlowFirstAttempt:
    """第一次执行跑不完，之后的执行立即返回。"""

    def __init__(self) -> None:
        self.calls = 0
        self.release = threading.Event()

    def __call__(self) -> str:
        self.calls += 1
        if self.calls == 1:
            self.release.wait(5)

        return f"attempt {self.calls}"


class TimeoutTestCase(unittest.TestCase):
    """提供"跑不完的可调用对象"这一公共夹具。"""

    def blocking_callable(self, result: str = "late") -> BlockingCallable:
        callable_ = BlockingCallable(result)
        # 被放弃的那次尝试会一直等在 release 上；测试结束时放行，避免线程
        # 停在测试进程里。
        self.addCleanup(callable_.release.set)

        return callable_

    def slow_first_attempt(self) -> SlowFirstAttempt:
        callable_ = SlowFirstAttempt()
        self.addCleanup(callable_.release.set)

        return callable_


class TaskTimeoutErrorTests(unittest.TestCase):
    def test_is_a_timeout_error_and_carries_the_attempt_facts(self) -> None:
        error = TaskTimeoutError(0.5, 0.512, "slow")

        self.assertIsInstance(error, TimeoutError)
        self.assertEqual(error.timeout, 0.5)
        self.assertEqual(error.elapsed, 0.512)
        self.assertEqual(error.task_name, "slow")
        self.assertIn("slow", str(error))
        self.assertIn("0.5", str(error))

    def test_describes_a_nameless_attempt(self) -> None:
        error = TaskTimeoutError(1, 1.25)

        self.assertIsNone(error.task_name)
        self.assertIn("本次执行", str(error))


class TimeoutValidationTests(unittest.TestCase):
    """任务字段与策略默认值对同一规则的判断必须一致。"""

    INVALID_LIMITS = (
        ("1", TypeError),
        (True, TypeError),
        (-1, ValueError),
        (0, ValueError),
        (float("nan"), ValueError),
        (float("inf"), ValueError),
    )

    def test_task_rejects_invalid_limits(self) -> None:
        for value, error_type in self.INVALID_LIMITS:
            with self.subTest(value=value):
                with self.assertRaises(error_type):
                    Task(name="task", callable=no_operation, timeout=value)  # type: ignore[arg-type]

    def test_policy_rejects_invalid_limits(self) -> None:
        for value, error_type in self.INVALID_LIMITS:
            with self.subTest(value=value):
                with self.assertRaises(error_type):
                    TimeoutPolicy(timeout=value)  # type: ignore[arg-type]

    def test_accepts_missing_and_positive_limits(self) -> None:
        self.assertIsNone(Task(name="task", callable=no_operation).timeout)
        self.assertEqual(Task(name="task", callable=no_operation, timeout=2).timeout, 2)
        self.assertEqual(Task(name="task", callable=no_operation, timeout=0.25).timeout, 0.25)
        self.assertIsNone(TimeoutPolicy().timeout)
        self.assertEqual(TimeoutPolicy(timeout=2).timeout, 2)


class TimeoutPolicyTests(unittest.TestCase):
    def test_defaults_to_no_limit(self) -> None:
        policy = TimeoutPolicy()

        self.assertIsNone(policy.timeout)
        self.assertIsNone(policy.limit_for(Task(name="task", callable=no_operation)))
        self.assertFalse(policy.is_limited(Task(name="task", callable=no_operation)))

    def test_falls_back_to_policy_default_when_task_has_no_limit(self) -> None:
        policy = TimeoutPolicy(timeout=1.5)

        self.assertEqual(policy.limit_for(Task(name="task", callable=no_operation)), 1.5)
        self.assertTrue(policy.is_limited(Task(name="task", callable=no_operation)))

    def test_task_limit_overrides_policy_default(self) -> None:
        policy = TimeoutPolicy(timeout=1.5)

        self.assertEqual(policy.limit_for(Task(name="task", callable=no_operation, timeout=0.5)), 0.5)
        self.assertEqual(policy.limit_for(Task(name="task", callable=no_operation, timeout=9)), 9)

    def test_rejects_non_task_value(self) -> None:
        with self.assertRaisesRegex(TypeError, "Task"):
            TimeoutPolicy().limit_for(object())  # type: ignore[arg-type]

    def test_module_helper_reports_the_attempt_limit(self) -> None:
        self.assertIsNone(timeout_for(Task(name="task", callable=no_operation)))
        self.assertEqual(timeout_for(Task(name="task", callable=no_operation), timeout=3), 3)
        self.assertEqual(
            timeout_for(Task(name="task", callable=no_operation, timeout=0.5), timeout=3),
            0.5,
        )


class TaskTimeoutFieldTests(TimeoutTestCase):
    def test_timeout_is_part_of_the_task_description(self) -> None:
        limited = Task(name="task", callable=no_operation, id="same", timeout=1)
        unlimited = Task(name="task", callable=no_operation, id="same")

        self.assertNotEqual(limited, unlimited)
        self.assertNotIn("timeout", repr(unlimited))
        self.assertIn("timeout=1", repr(limited))


class RunWithTimeoutTests(TimeoutTestCase):
    def test_returns_the_value_when_the_call_finishes_in_time(self) -> None:
        self.assertEqual(run_with_timeout(lambda: "done", 5), "done")
        self.assertIsNone(run_with_timeout(no_operation, 5))

    def test_runs_in_the_calling_thread_when_no_limit_is_configured(self) -> None:
        threads: list[int] = []

        def record() -> str:
            threads.append(threading.get_ident())

            return "done"

        self.assertEqual(run_with_timeout(record, None), "done")
        self.assertEqual(threads, [threading.get_ident()])

    def test_runs_in_another_thread_when_a_limit_is_configured(self) -> None:
        threads: list[int] = []

        def record() -> str:
            threads.append(threading.get_ident())

            return "done"

        self.assertEqual(run_with_timeout(record, 5), "done")
        self.assertEqual(len(threads), 1)
        self.assertNotEqual(threads[0], threading.get_ident())

    def test_raises_task_timeout_error_when_the_call_does_not_finish(self) -> None:
        callable_ = self.blocking_callable()

        with self.assertRaises(TaskTimeoutError) as caught:
            run_with_timeout(callable_, 0.05, task_name="blocked")

        self.assertEqual(caught.exception.timeout, 0.05)
        self.assertEqual(caught.exception.task_name, "blocked")
        # 上限是"至少给到"的：判定超时只能发生在上限时刻之后。
        self.assertGreaterEqual(caught.exception.elapsed, 0.05)
        self.assertLess(caught.exception.elapsed, 1.05)
        self.assertTrue(callable_.started.wait(5))
        self.assertEqual(callable_.calls, 1)

    def test_replays_the_callables_own_exception(self) -> None:
        expected = RuntimeError("boom")

        def boom() -> None:
            raise expected

        with self.assertRaises(RuntimeError) as caught:
            run_with_timeout(boom, 5)

        self.assertIs(caught.exception, expected)

    def test_each_call_gets_its_own_budget(self) -> None:
        """上一次调用用掉的时间不计入下一次调用。"""
        callable_ = self.blocking_callable(result="released")

        with self.assertRaises(TaskTimeoutError):
            run_with_timeout(callable_, 0.05)

        callable_.release.set()
        self.assertTrue(callable_.finished.wait(5))

        self.assertEqual(run_with_timeout(callable_, 5), "released")
        self.assertEqual(callable_.calls, 2)

    def test_validates_the_limit(self) -> None:
        with self.assertRaises(ValueError):
            run_with_timeout(no_operation, 0)

        with self.assertRaises(TypeError):
            run_with_timeout(no_operation, True)  # type: ignore[arg-type]


class TimeoutExecutorTests(TimeoutTestCase):
    def test_marks_the_attempt_failed_when_it_exceeds_the_task_limit(self) -> None:
        callable_ = self.blocking_callable()
        task = Task(name="blocked", callable=callable_, timeout=0.05)

        returned = TimeoutExecutor().execute(task)

        self.assertIs(returned, task)
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, TaskTimeoutError)
        self.assertEqual(task.error.timeout, 0.05)
        self.assertEqual(task.error.task_name, "blocked")
        self.assertIsNone(task.result)
        # 超时只是"一次失败"，不消耗重试次数，也不改动重试记账。
        self.assertEqual(task.retry_count, 0)
        self.assertIsNone(task.last_error)

    def test_uses_the_policy_default_when_the_task_has_no_limit(self) -> None:
        task = Task(name="blocked", callable=self.blocking_callable())

        TimeoutExecutor(TimeoutPolicy(timeout=0.05)).execute(task)

        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, TaskTimeoutError)
        self.assertEqual(task.error.timeout, 0.05)

    def test_task_limit_wins_over_a_generous_policy_default(self) -> None:
        task = Task(name="blocked", callable=self.blocking_callable(), timeout=0.05)

        TimeoutExecutor(TimeoutPolicy(timeout=5)).execute(task)

        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error.timeout, 0.05)

    def test_task_limit_can_extend_the_policy_default(self) -> None:
        task = Task(name="fast", callable=lambda: "ok", timeout=5)

        TimeoutExecutor(TimeoutPolicy(timeout=0.02)).execute(task)

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "ok")
        self.assertIsNone(task.error)

    def test_stores_the_result_when_the_attempt_finishes_in_time(self) -> None:
        task = Task(name="fast", callable=lambda: "ok", timeout=5)

        TimeoutExecutor().execute(task)

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "ok")
        self.assertIsNone(task.error)

    def test_runs_like_a_plain_executor_when_no_limit_is_configured(self) -> None:
        threads: list[int] = []

        def work() -> str:
            threads.append(threading.get_ident())

            return "done"

        task = Task(name="work", callable=work)

        TimeoutExecutor().execute(task)

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "done")
        # 不设上限时不引入额外线程，执行方式与 Executor 完全一致。
        self.assertEqual(threads, [threading.get_ident()])

    def test_keeps_the_lifecycle_rules_of_the_plain_executor(self) -> None:
        task = Task(
            name="done",
            callable=no_operation,
            status=TaskStatus.SUCCESS,
            result="kept",
            timeout=5,
        )

        with self.assertRaisesRegex(ValueError, "PENDING"):
            TimeoutExecutor().execute(task)

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "kept")

    def test_abandoned_attempt_does_not_overwrite_the_failed_state(self) -> None:
        callable_ = self.blocking_callable(result="late")
        task = Task(name="blocked", callable=callable_, timeout=0.05)

        TimeoutExecutor().execute(task)
        callable_.release.set()
        self.assertTrue(callable_.finished.wait(5))

        # 后台线程结束时这次尝试早已被判失败，它的返回值没有回灌任务状态的路径。
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIsNone(task.result)
        self.assertIsInstance(task.error, TaskTimeoutError)

    def test_exposes_the_resolved_limit(self) -> None:
        executor = TimeoutExecutor(TimeoutPolicy(timeout=1))

        self.assertIsInstance(executor.timeout_policy, TimeoutPolicy)
        self.assertEqual(executor.timeout_for(Task(name="task", callable=no_operation)), 1)
        self.assertEqual(executor.timeout_for(Task(name="task", callable=no_operation, timeout=2)), 2)
        self.assertIsNone(TimeoutExecutor().timeout_for(Task(name="task", callable=no_operation)))

    def test_rejects_invalid_timeout_policy(self) -> None:
        with self.assertRaisesRegex(TypeError, "TimeoutPolicy"):
            TimeoutExecutor(object())  # type: ignore[arg-type]


class TimeoutRetryTests(TimeoutTestCase):
    def test_each_attempt_gets_a_fresh_budget(self) -> None:
        callable_ = self.slow_first_attempt()
        queue = TaskQueue()
        worker = Worker(queue, TimeoutExecutor(TimeoutPolicy(timeout=0.2)), RetryPolicy(max_retries=1))
        task = Task(name="slow-first", callable=callable_)
        queue.enqueue(task)

        processed = worker.run()

        self.assertEqual(processed, [task, task])
        self.assertEqual(callable_.calls, 2)
        # 第一次尝试超时失败，第二次尝试重新拿到完整上限并成功。
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "attempt 2")
        self.assertIsNone(task.error)
        self.assertIsInstance(task.last_error, TaskTimeoutError)
        self.assertEqual(task.retry_count, 1)
        self.assertTrue(queue.is_empty())

    def test_timeout_exhausts_retries_like_any_other_failure(self) -> None:
        callable_ = self.blocking_callable()
        queue = TaskQueue()
        worker = Worker(queue, TimeoutExecutor(TimeoutPolicy(timeout=0.05)), RetryPolicy(max_retries=2))
        task = Task(name="blocked", callable=callable_)
        queue.enqueue(task)

        processed = worker.run()

        self.assertEqual(processed, [task, task, task])
        self.assertEqual(callable_.calls, 3)
        self.assertEqual(task.retry_count, 2)
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertIsInstance(task.error, TaskTimeoutError)
        self.assertIsInstance(task.last_error, TaskTimeoutError)
        self.assertTrue(queue.is_empty())

    def test_timeout_keeps_the_queue_flowing_for_other_tasks(self) -> None:
        observed: list[str] = []
        queue = TaskQueue()

        def succeeding() -> None:
            observed.append("succeeding")

        blocked = Task(name="blocked", callable=self.blocking_callable(), timeout=0.05)
        succeeding_task = Task(name="succeeding", callable=succeeding)
        queue.enqueue(blocked)
        queue.enqueue(succeeding_task)

        processed = Worker(queue, TimeoutExecutor(), RetryPolicy(max_retries=0)).run()

        self.assertEqual(processed, [blocked, succeeding_task])
        self.assertIs(blocked.status, TaskStatus.FAILED)
        self.assertIs(succeeding_task.status, TaskStatus.SUCCESS)
        self.assertEqual(observed, ["succeeding"])

    def test_timeout_runs_through_the_scheduler_without_changing_its_contract(self) -> None:
        callable_ = self.slow_first_attempt()
        queue = TaskQueue()
        worker = Worker(queue, TimeoutExecutor(), RetryPolicy(max_retries=1))
        scheduler = Scheduler(queue, worker)
        task = Task(name="slow-first", callable=callable_, timeout=0.2)
        scheduler.submit(task)

        processed = scheduler.start()

        self.assertEqual(processed, [task, task])
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.retry_count, 1)
        self.assertTrue(queue.is_empty())
