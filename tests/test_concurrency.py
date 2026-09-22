"""固定线程池并发 Worker 的独立测试。"""

import threading
import time
import unittest

from workflow_engine import ConcurrentWorker, Executor, RetryPolicy, Scheduler, Task, TaskQueue, TaskStatus


class ConcurrentWorkerTests(unittest.TestCase):
    def test_processes_all_tasks_and_preserves_task_results(self) -> None:
        queue = TaskQueue()
        tasks = [Task(name=f"task-{number}", callable=lambda value=number: value * 2) for number in range(6)]
        for task in tasks:
            queue.enqueue(task)

        processed = ConcurrentWorker(queue, Executor(), max_workers=3).run()

        self.assertEqual(processed, tasks)
        self.assertTrue(queue.is_empty())
        self.assertEqual([task.result for task in tasks], [0, 2, 4, 6, 8, 10])
        self.assertTrue(all(task.status is TaskStatus.SUCCESS for task in tasks))

    def test_retries_are_safe_and_do_not_overlap_attempts(self) -> None:
        queue = TaskQueue()
        attempts = 0
        attempts_lock = threading.Lock()

        def flaky() -> str:
            nonlocal attempts
            with attempts_lock:
                attempts += 1
                current = attempts
            if current == 1:
                raise RuntimeError("first attempt fails")
            return "recovered"

        task = Task(name="flaky", callable=flaky, max_retries=1)
        queue.enqueue(task)

        processed = ConcurrentWorker(queue, Executor(), max_workers=2, retry_policy=RetryPolicy()).run()

        self.assertEqual(processed, [task, task])
        self.assertEqual(attempts, 2)
        self.assertEqual(task.retry_count, 1)
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "recovered")

    def test_scheduler_accepts_concurrent_worker(self) -> None:
        queue = TaskQueue()
        scheduler = Scheduler(queue, ConcurrentWorker(queue, Executor(), max_workers=2))
        task = Task(name="scheduled", callable=lambda: "done")

        scheduler.submit(task)

        self.assertEqual(scheduler.start(), [task])
        self.assertIs(task.status, TaskStatus.SUCCESS)

    def test_queue_rejects_concurrent_duplicate_submission(self) -> None:
        queue = TaskQueue()
        task = Task(name="only-once", callable=lambda: None)
        barrier = threading.Barrier(8)
        outcomes: list[str] = []
        outcomes_lock = threading.Lock()

        def submit() -> None:
            barrier.wait()
            try:
                queue.enqueue(task)
            except ValueError:
                value = "rejected"
            else:
                value = "accepted"
            with outcomes_lock:
                outcomes.append(value)

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertEqual(outcomes.count("rejected"), 7)
        self.assertEqual(queue.size(), 1)

    def test_same_task_cannot_execute_twice_at_the_same_time(self) -> None:
        started = threading.Event()
        release = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def work() -> None:
            nonlocal calls
            with calls_lock:
                calls += 1
            started.set()
            release.wait(timeout=1)

        task = Task(name="exclusive", callable=work)
        executor = Executor()
        first = threading.Thread(target=lambda: executor.execute(task))
        errors: list[BaseException] = []

        def second_execution() -> None:
            try:
                executor.execute(task)
            except BaseException as error:
                errors.append(error)

        second = threading.Thread(target=second_execution)
        first.start()
        self.assertTrue(started.wait(timeout=1))
        second.start()
        release.set()
        first.join(timeout=1)
        second.join(timeout=1)

        self.assertEqual(calls, 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ValueError)
        self.assertIs(task.status, TaskStatus.SUCCESS)

    def test_never_exceeds_configured_parallelism(self) -> None:
        queue = TaskQueue()
        max_workers = 3
        all_started = threading.Event()
        release = threading.Event()
        active = 0
        peak = 0
        active_lock = threading.Lock()

        def blocking_work() -> None:
            nonlocal active, peak
            with active_lock:
                active += 1
                peak = max(peak, active)
                if active == max_workers:
                    all_started.set()
            release.wait(timeout=2)
            with active_lock:
                active -= 1

        for number in range(6):
            queue.enqueue(Task(name=f"blocking-{number}", callable=blocking_work))

        runner = threading.Thread(
            target=lambda: ConcurrentWorker(queue, Executor(), max_workers=max_workers).run()
        )
        runner.start()
        self.assertTrue(all_started.wait(timeout=1), "前三个任务应当同时开始")
        # 第四个任务在某一个执行槽释放前不得开始，因此峰值正好是配置值。
        with active_lock:
            self.assertEqual(peak, max_workers)
            self.assertEqual(active, max_workers)
        release.set()
        runner.join(timeout=2)
        self.assertFalse(runner.is_alive())
        self.assertTrue(queue.is_empty())
