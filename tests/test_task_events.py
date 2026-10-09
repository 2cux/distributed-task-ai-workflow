"""独立验证内存事件快照、执行/重试和超时的历史语义。"""
from dataclasses import FrozenInstanceError
from datetime import datetime
import threading
import unittest

from workflow_engine import (Executor, RetryPolicy, Task, TaskEngine, TaskEventType as E,
                             TaskStatus as S, TimeoutExecutor)


class TaskEventSnapshotTests(unittest.TestCase):
    def test_creation_and_read_only_detached_details(self):
        task = Task("new", lambda: 1)
        snapshot = task.events
        self.assertEqual(len(snapshot), 1)
        event = snapshot[0]
        self.assertEqual((event.event_type, event.status, event.attempt_count), (E.CREATED, S.PENDING, 0))
        self.assertEqual(event.task_id, task.id)
        self.assertIsNone(event.previous_status)
        self.assertEqual(datetime.fromisoformat(event.timestamp).utcoffset().total_seconds(), 0)
        with self.assertRaises(FrozenInstanceError):
            event.status = S.SUCCESS
        event.details["changed"] = True
        self.assertNotIn("changed", event.details)
        Executor().execute(task)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(len(task.events), 3)

    def test_invalid_or_unchanged_status_does_not_emit_event(self):
        task = Task("new", lambda: None)
        task.status = S.PENDING
        with self.assertRaises(ValueError):
            task.status = S.SUCCESS
        with self.assertRaises(TypeError):
            task.status = "RUNNING"
        self.assertEqual([e.event_type for e in task.events], [E.CREATED])

    def test_existing_state_is_snapshot_not_fabricated_creation(self):
        task = Task("existing", lambda: None, status=S.FAILED)
        self.assertEqual(task.events[0].event_type, E.RESTORED)

    def test_unrepresentable_result_does_not_break_execution(self):
        class Result:
            def __repr__(self):
                raise RuntimeError("cannot render")
        result = Result()
        task = Task("opaque", lambda: result)
        Executor().execute(task)
        self.assertEqual(task.status, S.SUCCESS)
        self.assertIs(task.result, result)
        self.assertEqual(task.events[-1].details["result_repr"], "<unrepresentable result>")


class TaskExecutionHistoryTests(unittest.TestCase):
    def test_retry_success_keeps_first_failure_and_counters(self):
        calls = 0
        def action():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("first failure")
            return {"answer": 42}
        engine = TaskEngine(retry_policy=RetryPolicy(1))
        task = engine.submit(Task("retry", action))
        engine.start()
        events = engine.get_events(task.id)
        self.assertEqual([e.event_type for e in events],
                         [E.CREATED, E.SUBMITTED, E.STARTED, E.FAILED, E.RETRY_SCHEDULED,
                          E.STARTED, E.SUCCEEDED])
        self.assertEqual([e.attempt_count for e in events], [0, 0, 1, 1, 1, 2, 2])
        self.assertEqual([e.retry_count for e in events], [0, 0, 0, 0, 1, 1, 1])
        self.assertEqual(events[3].details["error_message"], "first failure")
        self.assertEqual(events[3].details["error_type"], "ValueError")
        self.assertEqual(events[3].previous_status, S.RUNNING)
        self.assertEqual(events[4].previous_status, S.FAILED)
        self.assertEqual(events[-1].details["result_repr"], "{'answer': 42}")
        self.assertIsNone(task.error)
        self.assertEqual(engine.get_events(task.id, after_sequence=4), events[4:])
        self.assertEqual([e.sequence for e in events], list(range(1, 8)))
        with self.assertRaises(ValueError):
            engine.submit(Task("replacement", action, id=task.id))
        self.assertEqual(engine.get_events(task.id), events)

    def test_exhausted_retries_end_with_failed_event(self):
        def fail():
            raise RuntimeError("still failing")
        engine = TaskEngine(retry_policy=RetryPolicy(2))
        task = engine.submit(Task("fail", fail))
        engine.start()
        events = engine.get_events(task.id)
        self.assertEqual(sum(e.event_type is E.FAILED for e in events), 3)
        self.assertEqual(sum(e.event_type is E.RETRY_SCHEDULED for e in events), 2)
        self.assertEqual(events[-1].event_type, E.FAILED)
        self.assertEqual(events[-1].retry_count, 2)

    def test_unknown_task_and_invalid_cursor(self):
        engine = TaskEngine()
        self.assertEqual(engine.get_events("missing"), [])
        with self.assertRaises(TypeError):
            engine.submit(None)
        for value in (-1, True, 1.5, "1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                engine.get_events("missing", after_sequence=value)


class TaskTimeoutHistoryTests(unittest.TestCase):
    def test_timeout_records_limit_and_late_completion_cannot_rewrite_history(self):
        release = threading.Event()
        completed = threading.Event()
        self.addCleanup(release.set)
        def slow():
            try:
                release.wait(2)
                return 99
            finally:
                completed.set()
        task = Task("timeout", slow, timeout=0.01)
        TimeoutExecutor().execute(task)
        events = task.events
        failure = events[-1]
        self.assertEqual(failure.event_type, E.FAILED)
        self.assertEqual(failure.details["error_type"], "TaskTimeoutError")
        self.assertEqual(failure.details["timeout"], 0.01)
        self.assertGreaterEqual(failure.details["elapsed"], 0.01)
        release.set()
        self.assertTrue(completed.wait(2))
        self.assertEqual(task.events, events)
        self.assertEqual(task.status, S.FAILED)


if __name__ == "__main__":
    unittest.main()
