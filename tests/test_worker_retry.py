"""Worker 重试编排测试：失败 -> 判断剩余次数 -> 重新入队 -> 再执行。"""

import unittest

from workflow_engine import (
    Executor,
    RetryPolicy,
    Scheduler,
    Task,
    TaskQueue,
    TaskStatus,
    Worker,
)


class FlakyCallable:
    """前 ``failures`` 次调用失败，之后返回成功值。"""

    def __init__(self, failures: int, result: str = "ok") -> None:
        self.failures = failures
        self.result = result
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError(f"attempt {self.calls} failed")

        return self.result


class AlwaysFailingCallable:
    """每次调用都失败。"""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1
        raise RuntimeError(f"attempt {self.calls} failed")


def make_worker(retry_policy: RetryPolicy | None = None) -> tuple[Worker, TaskQueue]:
    queue = TaskQueue()

    return Worker(queue, Executor(), retry_policy), queue


class WorkerWithoutRetryPolicyTests(unittest.TestCase):
    def test_failed_task_stays_failed_when_no_policy_is_configured(self) -> None:
        worker, queue = make_worker()
        task = Task(name="fail", callable=AlwaysFailingCallable(), max_retries=3)
        queue.enqueue(task)

        processed = worker.run()

        self.assertEqual(processed, [task])
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertEqual(task.retry_count, 0)
        self.assertTrue(queue.is_empty())


class WorkerRetryTests(unittest.TestCase):
    def test_failed_task_is_requeued_within_the_same_run(self) -> None:
        callable_ = AlwaysFailingCallable()
        worker, queue = make_worker(RetryPolicy(max_retries=2))
        task = Task(name="flaky", callable=callable_)
        queue.enqueue(task)

        processed = worker.run()

        self.assertEqual(processed, [task, task, task])
        self.assertEqual(callable_.calls, 3)
        self.assertEqual(task.retry_count, 2)
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertTrue(queue.is_empty())

    def test_retry_succeeds_on_a_later_attempt_and_stops_retrying(self) -> None:
        callable_ = FlakyCallable(failures=2, result="recovered")
        worker, queue = make_worker(RetryPolicy(max_retries=5))
        task = Task(name="flaky", callable=callable_)
        queue.enqueue(task)

        processed = worker.run()

        self.assertEqual(processed, [task, task, task])
        self.assertEqual(task.retry_count, 2)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "recovered")
        self.assertIsNone(task.error)
        self.assertTrue(queue.is_empty())

    def test_exhausted_task_is_not_requeued_again(self) -> None:
        worker, queue = make_worker(RetryPolicy(max_retries=1))
        task = Task(name="flaky", callable=AlwaysFailingCallable())
        queue.enqueue(task)

        worker.run()

        self.assertEqual(task.retry_count, 1)
        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertEqual(queue.size(), 0)

    def test_task_level_limit_overrides_policy_default(self) -> None:
        worker, queue = make_worker(RetryPolicy(max_retries=5))
        limited = Task(name="limited", callable=AlwaysFailingCallable(), max_retries=0)
        queue.enqueue(limited)

        processed = worker.run()

        self.assertEqual(processed, [limited])
        self.assertEqual(limited.retry_count, 0)
        self.assertIs(limited.status, TaskStatus.FAILED)

    def test_last_error_keeps_the_error_that_triggered_the_final_retry(self) -> None:
        worker, queue = make_worker(RetryPolicy(max_retries=1))
        task = Task(name="flaky", callable=AlwaysFailingCallable())
        queue.enqueue(task)

        worker.run()

        self.assertIsInstance(task.last_error, RuntimeError)
        self.assertEqual(str(task.last_error), "attempt 1 failed")
        self.assertIsInstance(task.error, RuntimeError)
        self.assertEqual(str(task.error), "attempt 2 failed")

    def test_requeued_task_goes_to_the_tail_and_does_not_block_other_tasks(self) -> None:
        observed: list[str] = []

        def first() -> None:
            observed.append("first")
            raise RuntimeError("first failed")

        def second() -> None:
            observed.append("second")

        worker, queue = make_worker(RetryPolicy(max_retries=1))
        failing = Task(name="failing", callable=first)
        succeeding = Task(name="succeeding", callable=second)
        queue.enqueue(failing)
        queue.enqueue(succeeding)

        processed = worker.run()

        self.assertEqual(observed, ["first", "second", "first"])
        self.assertEqual(processed, [failing, succeeding, failing])
        self.assertIs(succeeding.status, TaskStatus.SUCCESS)
        self.assertIs(failing.status, TaskStatus.FAILED)

    def test_process_next_returns_the_requeued_task_and_keeps_it_in_the_queue(self) -> None:
        worker, queue = make_worker(RetryPolicy(max_retries=1))
        task = Task(name="flaky", callable=AlwaysFailingCallable())
        queue.enqueue(task)

        returned = worker.process_next()

        self.assertIs(returned, task)
        self.assertIs(task.status, TaskStatus.RETRYING)
        self.assertEqual(task.retry_count, 1)
        self.assertEqual(queue.size(), 1)
        self.assertIn(task, queue)

        worker.process_next()

        self.assertIs(task.status, TaskStatus.FAILED)
        self.assertTrue(queue.is_empty())

    def test_retry_runs_through_scheduler_without_changing_its_contract(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor(), RetryPolicy(max_retries=2))
        scheduler = Scheduler(queue, worker)
        task = Task(name="flaky", callable=FlakyCallable(failures=1))
        scheduler.submit(task)

        processed = scheduler.start()

        self.assertEqual(processed, [task, task])
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.retry_count, 1)
        self.assertTrue(queue.is_empty())

    def test_retrying_task_leaves_the_retry_lifecycle_on_success(self) -> None:
        queue = TaskQueue()
        worker = Worker(queue, Executor(), RetryPolicy(max_retries=1))

        class OnceFailing:
            def __init__(self) -> None:
                self.calls = 0

            def __call__(self) -> str:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("first attempt failed")

                return "recovered"

        task = Task(name="flaky", callable=OnceFailing())
        queue.enqueue(task)

        worker.process_next()

        self.assertIs(task.status, TaskStatus.RETRYING)

        worker.process_next()

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "recovered")
        self.assertIsNone(task.error)
        self.assertIsInstance(task.last_error, RuntimeError)
        self.assertEqual(task.retry_count, 1)
        self.assertTrue(queue.is_empty())

    def test_rejects_invalid_retry_policy(self) -> None:
        with self.assertRaisesRegex(TypeError, "RetryPolicy"):
            Worker(TaskQueue(), Executor(), retry_policy=object())  # type: ignore[arg-type]

    def test_refuses_to_requeue_a_task_that_is_already_queued(self) -> None:
        class RequeueingPolicy(RetryPolicy):
            """在 Worker 重新入队之前，抢先自己把任务放回队列的策略。"""

            def __init__(self, queue: TaskQueue) -> None:
                super().__init__(max_retries=1)
                self._queue = queue

            def begin_retry(self, task: Task) -> Task:
                task = super().begin_retry(task)
                self._queue.enqueue(task)

                return task

        queue = TaskQueue()
        worker = Worker(queue, Executor(), RequeueingPolicy(queue))
        task = Task(name="flaky", callable=AlwaysFailingCallable())
        queue.enqueue(task)

        # 队列自身拒绝重复入队，Worker 不会因为它而静默插入两份任务。
        with self.assertRaisesRegex(ValueError, "已经在队列中"):
            worker.process_next()

        self.assertEqual(queue.size(), 1)
        self.assertIn(task, queue)

    def test_executor_side_requeue_keeps_the_queue_and_task_state_coherent(self) -> None:
        class RequeueingExecutor(Executor):
            """在失败后自行把任务改写并放回队列的执行器。

            这是违反分工的执行器：它抢在重试策略之前完成了重新入队。Worker 的
            重试决策依据是本次执行确实失败，此时策略看到任务已是 RETRYING，
            于是不再重复入队，队列与任务状态保持一致。
            """

            def __init__(self, queue: TaskQueue) -> None:
                self._queue = queue

            def execute(self, task: Task) -> Task:
                task = super().execute(task)
                if task.status is TaskStatus.FAILED:
                    task.status = TaskStatus.RETRYING
                    task.retry_count = 1
                    self._queue.enqueue(task)

                return task

        queue = TaskQueue()
        worker = Worker(queue, RequeueingExecutor(queue), RetryPolicy(max_retries=2))
        task = Task(name="flaky", callable=AlwaysFailingCallable())
        queue.enqueue(task)

        returned = worker.process_next()

        self.assertIs(returned, task)
        self.assertIs(task.status, TaskStatus.RETRYING)
        self.assertEqual(task.retry_count, 1)
        self.assertEqual(queue.size(), 1)
        self.assertIn(task, queue)
