"""有界消费的独立测试类：槽位、优先级、重试及优雅关闭。"""

from contextlib import contextmanager
import threading
import unittest

from workflow_engine import (
    ConcurrentWorker, Executor, RetryPolicy, Task, TaskQueue, TaskStatus, WorkerStatus,
)


def blocking_task(name, started, release, **kwargs):
    def work():
        started.set()
        if not release.wait(5):
            raise TimeoutError("测试未释放任务")
    return Task(name=name, callable=work, **kwargs)


@contextmanager
def running(worker, *releases):
    """收集后台异常，并在断言失败时也释放任务、回收线程。"""
    processed = []
    errors = []

    def run():
        try:
            processed.extend(worker.run())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        yield processed
    finally:
        worker.stop()
        for release in releases:
            release.set()
        thread.join(5)
        if thread.is_alive():
            raise AssertionError("Worker 未完成优雅关闭")
        if errors:
            raise errors[0]


class BoundedConsumptionTests(unittest.TestCase):
    def test_only_free_slots_are_dequeued_and_each_completion_refills(self):
        queue = TaskQueue()
        starts = [threading.Event() for _ in range(3)]
        releases = [threading.Event() for _ in range(3)]
        tasks = [blocking_task(str(i), starts[i], releases[i]) for i in range(3)]
        tasks.extend(Task(name=str(i), callable=lambda: None) for i in range(3, 8))
        for task in tasks:
            queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor(), max_workers=2)

        with running(worker, *releases):
            self.assertTrue(starts[0].wait(2))
            self.assertTrue(starts[1].wait(2))
            self.assertEqual(queue.size(), 6)
            self.assertTrue(all(task in queue for task in tasks[2:]))
            # 首个任务仍阻塞，第二个完成就应补入第三个，不等待整批。
            releases[1].set()
            self.assertTrue(starts[2].wait(2))
            self.assertFalse(releases[0].is_set())
            self.assertEqual(queue.size(), 5)

        self.assertIs(worker.status, WorkerStatus.STOPPED)

    def test_empty_queue_and_single_slot(self):
        self.assertEqual(ConcurrentWorker(TaskQueue(), Executor()).run(), [])
        queue = TaskQueue()
        tasks = [Task(name=str(i), callable=lambda: None) for i in range(4)]
        for task in tasks:
            queue.enqueue(task)
        self.assertEqual(ConcurrentWorker(queue, Executor(), max_workers=1).run(), tasks)
        self.assertTrue(all(task.status is TaskStatus.SUCCESS for task in tasks))


class BoundedPriorityTests(unittest.TestCase):
    def test_initial_priority_and_fifo(self):
        queue = TaskQueue()
        tasks = [Task(name=str(i), callable=lambda: None, priority=p)
                 for i, p in enumerate([0, 10, 10, -1, 5])]
        for task in tasks:
            queue.enqueue(task)
        processed = ConcurrentWorker(queue, Executor(), max_workers=2).run()
        self.assertEqual(processed, [tasks[1], tasks[2], tasks[4], tasks[0], tasks[3]])

    def test_new_high_priority_task_gets_next_free_slot(self):
        queue = TaskQueue()
        starts = [threading.Event() for _ in range(3)]
        releases = [threading.Event() for _ in range(3)]
        current = [blocking_task(str(i), starts[i], releases[i]) for i in range(2)]
        pending = [Task(name=f"pending-{i}", callable=lambda: None) for i in range(3)]
        for task in current + pending:
            queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor(), max_workers=2)
        high = blocking_task("urgent", starts[2], releases[2], priority=100)

        with running(worker, *releases) as processed:
            self.assertTrue(starts[0].wait(2))
            self.assertTrue(starts[1].wait(2))
            queue.enqueue(high)
            releases[1].set()
            self.assertTrue(starts[2].wait(2))
            self.assertTrue(all(task in queue for task in pending))

        self.assertEqual(processed, current + [high])


class BoundedRetryTests(unittest.TestCase):
    def test_retry_refills_without_waiting_for_other_running_task(self):
        queue = TaskQueue()
        slow_started, fail_started, retry_started = [threading.Event() for _ in range(3)]
        slow_release, fail_release, retry_release = [threading.Event() for _ in range(3)]
        slow = blocking_task("slow", slow_started, slow_release, priority=20)
        attempts = 0

        def flaky():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                fail_started.set()
                if not fail_release.wait(5):
                    raise TimeoutError("测试未释放失败尝试")
                raise RuntimeError("first failure")
            retry_started.set()
            if not retry_release.wait(5):
                raise TimeoutError("测试未释放重试")
            return "recovered"

        retry = Task(name="retry", callable=flaky, priority=10, max_retries=1)
        pending = Task(name="low", callable=lambda: None)
        for task in [slow, retry, pending]:
            queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor(), max_workers=2, retry_policy=RetryPolicy())

        with running(worker, slow_release, fail_release, retry_release) as processed:
            self.assertTrue(slow_started.wait(2))
            self.assertTrue(fail_started.wait(2))
            fail_release.set()
            self.assertTrue(retry_started.wait(2))
            self.assertFalse(slow_release.is_set())
            self.assertIn(pending, queue)
            self.assertEqual(retry.retry_count, 1)

        self.assertEqual(processed, [slow, retry, retry])
        self.assertEqual(retry.attempt_count, 2)
        self.assertEqual(retry.result, "recovered")
        self.assertIs(retry.status, TaskStatus.SUCCESS)
        self.assertIsInstance(retry.last_error, RuntimeError)

    def test_equal_priority_retry_goes_to_fifo_tail(self):
        queue = TaskQueue()
        retry = Task(name="retry", callable=lambda: 1 / 0, max_retries=1)
        peer = Task(name="peer", callable=lambda: None)
        queue.enqueue(retry)
        queue.enqueue(peer)
        processed = ConcurrentWorker(queue, Executor(), max_workers=1,
                                     retry_policy=RetryPolicy()).run()
        self.assertEqual(processed, [retry, peer, retry])
        self.assertEqual(retry.attempt_count, 2)
        self.assertIs(retry.status, TaskStatus.FAILED)

    def test_retry_limits_and_no_policy(self):
        for policy, task_limit, expected in [(None, 3, 1), (RetryPolicy(2), None, 3),
                                              (RetryPolicy(5), 1, 2)]:
            with self.subTest(policy=policy, task_limit=task_limit):
                queue = TaskQueue()
                task = Task(name="fails", callable=lambda: 1 / 0, max_retries=task_limit)
                queue.enqueue(task)
                processed = ConcurrentWorker(queue, Executor(), max_workers=2,
                                             retry_policy=policy).run()
                self.assertEqual(processed, [task] * expected)
                self.assertEqual(task.attempt_count, expected)
                self.assertEqual(task.retry_count, expected - 1)
                self.assertIs(task.status, TaskStatus.FAILED)
                self.assertTrue(queue.is_empty())


class BoundedShutdownTests(unittest.TestCase):
    def test_stop_drains_in_flight_and_preserves_pending_tasks(self):
        queue = TaskQueue()
        starts = [threading.Event() for _ in range(2)]
        releases = [threading.Event() for _ in range(2)]
        current = [blocking_task(str(i), starts[i], releases[i]) for i in range(2)]
        pending = [Task(name=f"pending-{i}", callable=lambda: None) for i in range(4)]
        for task in current + pending:
            queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor(), max_workers=2)

        with running(worker, *releases) as processed:
            self.assertTrue(starts[0].wait(2))
            self.assertTrue(starts[1].wait(2))
            worker.stop()
            worker.stop()
            self.assertIs(worker.status, WorkerStatus.STOPPING)
            self.assertEqual(queue.size(), 4)
            self.assertTrue(all(task in queue for task in pending))

        self.assertEqual(processed, current)
        self.assertTrue(all(task.status is TaskStatus.SUCCESS for task in current))
        self.assertTrue(all(task.status is TaskStatus.PENDING for task in pending))
        self.assertIs(worker.status, WorkerStatus.STOPPED)
        self.assertEqual(ConcurrentWorker(queue, Executor(), max_workers=2).run(), pending)

    def test_failed_attempt_after_stop_leaves_retry_for_new_worker(self):
        queue = TaskQueue()
        started, release = threading.Event(), threading.Event()
        calls = 0

        def flaky():
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                if not release.wait(5):
                    raise TimeoutError("测试未释放失败尝试")
                raise RuntimeError("failure during stop")
            return "recovered"

        task = Task(name="retry-after-stop", callable=flaky, max_retries=1)
        queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor(), max_workers=2, retry_policy=RetryPolicy())
        with running(worker, release) as processed:
            self.assertTrue(started.wait(2))
            worker.stop()

        self.assertEqual(processed, [task])
        self.assertEqual(calls, 1)
        self.assertIs(task.status, TaskStatus.RETRYING)
        self.assertEqual(task.retry_count, 1)
        self.assertIn(task, queue)
        self.assertEqual(ConcurrentWorker(queue, Executor(), max_workers=2,
                                         retry_policy=RetryPolicy()).run(), [task])
        self.assertIs(task.status, TaskStatus.SUCCESS)

    def test_stop_before_run_preserves_queue_and_worker_is_single_use(self):
        queue = TaskQueue()
        task = Task(name="pending", callable=lambda: None)
        queue.enqueue(task)
        worker = ConcurrentWorker(queue, Executor())
        worker.stop()
        worker.stop()
        with self.assertRaisesRegex(RuntimeError, "STOPPED"):
            worker.run()
        self.assertIn(task, queue)
        completed = ConcurrentWorker(queue, Executor())
        self.assertEqual(completed.run(), [task])
        with self.assertRaisesRegex(RuntimeError, "STOPPED"):
            completed.run()


if __name__ == "__main__":
    unittest.main()
