"""从 TaskEngine 入口验证核心任务编排场景。"""

from __future__ import annotations

import threading
import time

from workflow_engine import RetryPolicy, Task, TaskEngine, TaskStatus, TaskTimeoutError, TimeoutPolicy


class TestSystemScenarios:
    def test_normal_task_succeeds(self) -> None:
        engine = TaskEngine()
        task = engine.submit(Task(name="normal", callable=lambda: "done"))

        assert engine.start() == [task]
        assert task.status is TaskStatus.SUCCESS
        assert task.result == "done"

    def test_failed_task_retries_and_then_succeeds(self) -> None:
        calls = 0

        def flaky() -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("transient")
            return "recovered"

        engine = TaskEngine(retry_policy=RetryPolicy(max_retries=2))
        task = engine.submit(Task(name="flaky", callable=flaky))

        assert engine.start() == [task, task]
        assert (calls, task.retry_count, task.status, task.result) == (2, 1, TaskStatus.SUCCESS, "recovered")

    def test_retry_budget_is_exhausted(self) -> None:
        calls = 0

        def always_fails() -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("permanent")

        engine = TaskEngine(retry_policy=RetryPolicy(max_retries=2))
        task = engine.submit(Task(name="exhausted", callable=always_fails))

        assert engine.start() == [task, task, task]
        assert (calls, task.retry_count, task.status) == (3, 2, TaskStatus.FAILED)
        assert isinstance(task.error, RuntimeError)

    def test_timeout_is_retried_with_a_fresh_attempt_budget(self) -> None:
        calls = 0

        def slow_once() -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                time.sleep(0.08)
            return f"attempt-{calls}"

        engine = TaskEngine(
            retry_policy=RetryPolicy(max_retries=1),
            timeout_policy=TimeoutPolicy(timeout=0.03),
        )
        task = engine.submit(Task(name="timeout-then-success", callable=slow_once))

        assert engine.start() == [task, task]
        assert (calls, task.retry_count, task.status, task.result) == (2, 1, TaskStatus.SUCCESS, "attempt-2")
        assert isinstance(task.last_error, TaskTimeoutError)

    def test_concurrency_retry_and_timeout_work_together(self) -> None:
        active = 0
        peak = 0
        active_lock = threading.Lock()
        flaky_calls = 0
        timeout_calls = 0

        def bounded_work(label: str, fail_once: bool = False, timeout_once: bool = False) -> str:
            nonlocal active, peak, flaky_calls, timeout_calls
            with active_lock:
                active += 1
                peak = max(peak, active)
            try:
                if fail_once:
                    flaky_calls += 1
                    if flaky_calls == 1:
                        raise RuntimeError("transient")
                if timeout_once:
                    timeout_calls += 1
                    if timeout_calls == 1:
                        time.sleep(0.08)
                else:
                    time.sleep(0.01)
                return label
            finally:
                with active_lock:
                    active -= 1

        engine = TaskEngine(
            retry_policy=RetryPolicy(max_retries=1),
            timeout_policy=TimeoutPolicy(timeout=0.03),
            max_workers=2,
        )
        flaky = engine.submit(Task(name="flaky", callable=lambda: bounded_work("flaky", fail_once=True)))
        timed = engine.submit(Task(name="timed", callable=lambda: bounded_work("timed", timeout_once=True)))
        fast = engine.submit(Task(name="fast", callable=lambda: bounded_work("fast")))

        processed = engine.start()

        assert processed == [flaky, timed, fast, flaky, timed]
        # 超时的旧 attempt 是软超时，会在后台短暂继续运行；因此业务函数的
        # 峰值可能包含一个已超时的 attempt。调度线程池本身仍只有两个槽位。
        assert 2 <= peak <= 3
        assert flaky.status is TaskStatus.SUCCESS
        assert timed.status is TaskStatus.SUCCESS
        assert fast.status is TaskStatus.SUCCESS
        assert (flaky.retry_count, timed.retry_count) == (1, 1)
        assert isinstance(timed.last_error, TaskTimeoutError)

    def test_priority_is_preserved_when_a_high_priority_task_retries(self) -> None:
        observed: list[str] = []
        high_calls = 0

        def high() -> None:
            nonlocal high_calls
            high_calls += 1
            observed.append(f"high-{high_calls}")
            if high_calls == 1:
                raise RuntimeError("retry me")

        def low() -> None:
            observed.append("low")

        engine = TaskEngine(retry_policy=RetryPolicy(max_retries=1))
        low_task = engine.submit(Task(name="low", callable=low, priority=1))
        high_task = engine.submit(Task(name="high", callable=high, priority=10))

        assert engine.start() == [high_task, high_task, low_task]
        assert observed == ["high-1", "high-2", "low"]
        assert high_task.status is low_task.status is TaskStatus.SUCCESS
