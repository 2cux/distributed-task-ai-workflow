"""工作流引擎的跨组件生命周期不变量。"""

from __future__ import annotations

import time

import pytest

from workflow_engine import Executor, RetryPolicy, Task, TaskQueue, TaskStatus, TimeoutExecutor, TimeoutPolicy, Worker


def test_non_retry_task_has_exactly_one_attempt_and_cannot_run_again() -> None:
    calls = 0

    def work() -> None:
        nonlocal calls
        calls += 1

    task = Task(name="one-shot", callable=work)
    queue = TaskQueue()
    queue.enqueue(task)

    assert Worker(queue, Executor()).run() == [task]
    assert (calls, task.attempt_count, task.status) == (1, 1, TaskStatus.SUCCESS)
    with pytest.raises(ValueError):
        Executor().execute(task)
    with pytest.raises(ValueError):
        queue.enqueue(task)
    assert calls == 1


def test_terminal_states_never_reenter_running() -> None:
    successful = Task(name="success", callable=lambda: "done")
    failed = Task(name="failure", callable=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    for task in (successful, failed):
        Executor().execute(task)
        terminal = task.status
        with pytest.raises(ValueError, match="只能执行"):
            Executor().execute(task)
        assert task.status is terminal

    with pytest.raises(ValueError, match="SUCCESS.*RUNNING"):
        successful.status = TaskStatus.RUNNING


def test_task_status_is_always_a_legal_enum_member() -> None:
    task = Task(name="typed-status", callable=lambda: None)

    with pytest.raises(TypeError, match="TaskStatus"):
        task.status = "RUNNING"  # type: ignore[assignment]

    assert task.status is TaskStatus.PENDING


def test_exhausted_retry_budget_never_returns_to_queue() -> None:
    calls = 0

    def always_fail() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent")

    task = Task(name="bounded", callable=always_fail, max_retries=2)
    queue = TaskQueue()
    queue.enqueue(task)

    processed = Worker(queue, Executor(), RetryPolicy(max_retries=99)).run()

    assert processed == [task, task, task]
    assert (calls, task.attempt_count, task.retry_count, task.status) == (3, 3, 2, TaskStatus.FAILED)
    assert queue.is_empty()

    exhausted = Task(
        name="already-exhausted",
        callable=lambda: None,
        status=TaskStatus.FAILED,
        max_retries=1,
        retry_count=1,
    )
    with pytest.raises(ValueError, match="max_retries"):
        exhausted.mark_retrying()
    assert exhausted.status is TaskStatus.FAILED


def test_one_timed_out_attempt_creates_at_most_one_retry() -> None:
    calls = 0

    def timeout_once() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            time.sleep(0.05)
        return "recovered"

    task = Task(name="timeout", callable=timeout_once, max_retries=1)
    queue = TaskQueue()
    queue.enqueue(task)

    processed = Worker(
        queue,
        TimeoutExecutor(TimeoutPolicy(timeout=0.01)),
        RetryPolicy(max_retries=8),
    ).run()

    assert processed == [task, task]
    assert (calls, task.attempt_count, task.retry_count, task.status) == (2, 2, 1, TaskStatus.SUCCESS)
    assert queue.is_empty()


def test_queue_never_contains_a_duplicate_task_attempt() -> None:
    queue = TaskQueue()
    first = Task(id="same-attempt", name="first", callable=lambda: None)
    duplicate = Task(id="same-attempt", name="duplicate", callable=lambda: None)
    queue.enqueue(first)

    with pytest.raises(ValueError, match="已经在队列中"):
        queue.enqueue(duplicate)

    assert queue.size() == 1
    assert queue.dequeue() is first


def test_completed_run_leaves_no_running_task_or_queued_work() -> None:
    succeeded = Task(name="success", callable=lambda: "ok")
    failed = Task(name="failure", callable=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    queue = TaskQueue()
    queue.enqueue(succeeded)
    queue.enqueue(failed)

    processed = Worker(queue, Executor(), RetryPolicy(max_retries=0)).run()

    assert queue.is_empty()
    assert all(task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED} for task in processed)
    assert all(task.status is not TaskStatus.RUNNING for task in processed)
