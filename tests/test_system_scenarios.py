"""从 TaskEngine 入口验证核心任务编排场景。"""

from __future__ import annotations

import threading
import time
import tempfile
from pathlib import Path
from unittest.mock import patch

from workflow_engine import (RetryPolicy, Task, TaskEngine, TaskEventType,
                             TaskStatus, TaskTimeoutError, TimeoutPolicy)


class TestSystemScenarios:
    def test_delayed_submission_backoff_success_and_sqlite_reload(self) -> None:
        calls: list[float] = []

        def flaky() -> dict[str, int]:
            calls.append(time.time())
            if len(calls) == 1:
                raise RuntimeError("transient failure")
            return {"calls": len(calls)}

        with tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1], prefix=".scenario-tests-"
        ) as directory, patch("time.time", return_value=100) as wall, \
                patch("time.monotonic", return_value=10) as monotonic:
            options = dict(database_path=Path(directory) / "tasks.db",
                           task_registry={"flaky": flaky})
            engine = TaskEngine(**options, retry_policy=RetryPolicy(1, initial_delay=5))
            task = engine.submit(Task("delayed-flaky", flaky, scheduled_at=102))
            # 单步驱动真实 Worker，只推进时钟，不替换执行、重试或数据库模块。
            worker = engine._scheduler._worker
            submitted = engine.get_events(task.id)
            assert [event.event_type for event in submitted] == [
                TaskEventType.CREATED, TaskEventType.SUBMITTED,
            ]
            assert submitted[-1].details["scheduled_at"] == 102

            wall.return_value, monotonic.return_value = 101.999, 11.999
            assert worker.process_next() is None
            assert calls == []
            assert engine.pending_count == 1
            saved = engine.get_task(task.id)
            assert saved is not None
            assert (saved.status, saved.attempt_count, saved.retry_count) == (
                TaskStatus.PENDING, 0, 0,
            )
            assert engine.get_events(task.id) == submitted

            wall.return_value, monotonic.return_value = 102, 12
            assert worker.process_next() is task
            assert calls == [102]
            saved = engine.get_task(task.id)
            assert saved is not None
            assert (saved.status, saved.attempt_count, saved.retry_count, saved.retry_at) == (
                TaskStatus.RETRYING, 1, 1, 107,
            )
            failed = engine.get_events(task.id)
            assert [event.event_type for event in failed[2:]] == [
                TaskEventType.STARTED, TaskEventType.FAILED, TaskEventType.RETRY_SCHEDULED,
            ]
            assert failed[3].details["error_type"] == "RuntimeError"
            assert failed[3].details["error_message"] == "transient failure"
            assert failed[-1].details["retry_delay"] == 5
            assert failed[-1].details["retry_at"] == 107

            wall.return_value, monotonic.return_value = 106.999, 16.999
            assert worker.process_next() is None
            assert calls == [102]
            assert engine.pending_count == 1
            assert engine.get_events(task.id) == failed

            wall.return_value, monotonic.return_value = 107, 17
            assert engine.start() == [task]
            assert calls == [102, 107]
            assert (task.status, task.attempt_count, task.retry_count, task.result) == (
                TaskStatus.SUCCESS, 2, 1, {"calls": 2},
            )
            assert task.error is None
            assert task.retry_at is None
            assert engine.pending_count == 0
            events = engine.get_events(task.id)
            assert events[:5] == failed
            assert [event.event_type for event in events] == [
                TaskEventType.CREATED, TaskEventType.SUBMITTED, TaskEventType.STARTED,
                TaskEventType.FAILED, TaskEventType.RETRY_SCHEDULED,
                TaskEventType.STARTED, TaskEventType.SUCCEEDED,
            ]
            assert [event.sequence for event in events] == list(range(1, 8))
            assert {event.task_id for event in events} == {task.id}
            assert [event.attempt_count for event in events] == [0, 0, 1, 1, 1, 2, 2]
            assert [event.retry_count for event in events] == [0, 0, 0, 0, 1, 1, 1]
            assert events[-1].details["result"] == {"calls": 2}
            assert task.events == tuple(events)

            reloaded = TaskEngine(**options)
            restored = reloaded.get_task(task.id)
            assert restored is not None
            assert restored is not task
            assert (restored.status, restored.attempt_count, restored.retry_count,
                    restored.result, restored.scheduled_at, restored.retry_at) == (
                TaskStatus.SUCCESS, 2, 1, {"calls": 2}, 102, None,
            )
            assert restored.error is None
            assert restored.events == tuple(events)
            assert reloaded.get_events(task.id) == events
            assert reloaded.pending_count == 0
            assert reloaded.start() == []
            assert calls == [102, 107]
            assert reloaded.get_events(task.id) == events

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
