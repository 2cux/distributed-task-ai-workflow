"""独立验证事件持久化、事务边界、竞争领取和旧库升级。"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from workflow_engine import (IdempotencyConflict, PersistenceError, RetryPolicy, SQLiteTaskStore,
                             Task, TaskEngine, TaskEventType as E, TaskStatus as S)


class EventStoreFixture(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix=".event-tests-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "tasks.db"

    def store(self):
        return SQLiteTaskStore(self.path, {"abs": abs})


class PersistentEventHistoryTests(EventStoreFixture):
    def test_restart_and_idempotent_resubmit_preserve_exact_history(self):
        options = dict(database_path=self.path, task_registry={"abs": abs})
        engine = TaskEngine(**options)
        task = Task("abs", abs, id="stable", args=(-7,))
        created_at = task.events[0].timestamp
        engine.submit(task)
        before = engine.get_events(task.id)
        self.assertEqual([e.event_type for e in before], [E.CREATED, E.SUBMITTED])
        self.assertEqual(before[0].timestamp, created_at)
        restarted = TaskEngine(**options)
        restarted.submit(Task("abs", abs, id="stable", args=(-7,)))
        self.assertEqual(restarted.get_events("stable"), before)
        finished = restarted.start()[0]
        events = restarted.get_events("stable")
        self.assertEqual([e.event_type for e in events], [E.CREATED, E.SUBMITTED, E.STARTED, E.SUCCEEDED])
        self.assertEqual(events[-1].details["result"], 7)
        self.assertEqual(finished.events, tuple(events))
        again = TaskEngine(**options)
        self.assertEqual(again.get_task("stable").events, tuple(events))
        self.assertEqual(again.get_events("stable", after_sequence=2), events[2:])
        self.assertEqual(again.get_events("missing"), [])
        with self.assertRaises(IdempotencyConflict):
            again.submit(Task("different", abs, id="stable", args=(-7,)))
        self.assertEqual(again.get_events("stable"), events)

    def test_retry_commit_preserves_failed_attempt_before_success(self):
        calls = 0
        def action():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("original failure")
            return {"calls": calls}
        engine = TaskEngine(database_path=self.path, task_registry={"action": action}, retry_policy=RetryPolicy(1))
        task = engine.submit(Task("retry", action))
        engine.start()
        events = engine.get_events(task.id)
        self.assertEqual([e.event_type for e in events],
                         [E.CREATED, E.SUBMITTED, E.STARTED, E.FAILED, E.RETRY_SCHEDULED, E.STARTED, E.SUCCEEDED])
        self.assertEqual([e.attempt_count for e in events], [0, 0, 1, 1, 1, 2, 2])
        self.assertEqual([e.retry_count for e in events], [0, 0, 0, 0, 1, 1, 1])
        self.assertEqual(events[3].details["error_message"], "original failure")
        self.assertEqual(events[-1].details["result"], {"calls": 2})
        self.assertEqual(task.events, tuple(events))

    def test_serialization_failure_is_recorded_without_retry(self):
        def opaque():
            return object()
        engine = TaskEngine(database_path=self.path, task_registry={"opaque": opaque}, retry_policy=RetryPolicy(3))
        task = engine.submit(Task("opaque", opaque))
        engine.start()
        events = engine.get_events(task.id)
        self.assertEqual(events[-1].event_type, E.FAILED)
        self.assertEqual(events[-1].details["error_type"], "PersistenceError")
        self.assertNotIn(E.RETRY_SCHEDULED, [e.event_type for e in events])


class EventTransactionTests(EventStoreFixture):
    def test_retry_event_failure_rolls_back_failure_event_and_retry_state(self):
        store = self.store()
        task = store.submit(Task("abs", abs, args=(-1,)), retry_limit=1)
        claimed = store.claim(task, "owner")
        started = store.get_events(task.id)
        claimed.error = ValueError("failed")
        claimed.status = S.FAILED
        RetryPolicy(1).begin_retry(claimed)
        append = store._append_event
        def fail_retry(*args, **kwargs):
            append(*args, **kwargs)
            if args[2] is E.RETRY_SCHEDULED:
                raise sqlite3.OperationalError("retry event failed")
        with patch.object(store, "_append_event", side_effect=fail_retry):
            with self.assertRaises(sqlite3.OperationalError):
                store.finish(claimed, "owner")
        self.assertEqual(store.get_events(task.id), started)
        self.assertEqual(claimed.events, tuple(started))
        self.assertEqual(store.get(task.id).status, S.RUNNING)
        self.assertEqual(store.get(task.id).retry_count, 0)
        store.finish(claimed, "owner")
        self.assertEqual([e.event_type for e in store.get_events(task.id)[-2:]], [E.FAILED, E.RETRY_SCHEDULED])

    def test_failed_engine_commit_exposes_only_committed_event_snapshot(self):
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs})
        task = engine.submit(Task("abs", abs, args=(-1,)))
        with patch.object(SQLiteTaskStore, "finish", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                engine.start()
        self.assertEqual(task.events[-1].event_type, E.STARTED)
        self.assertEqual(task.events, tuple(engine.get_events(task.id)))

    def test_event_write_failure_rolls_back_submission_claim_and_finish(self):
        store = self.store()
        task = Task("abs", abs, id="atomic", args=(-1,))
        append = store._append_event
        def insert_then_fail(*args, **kwargs):
            append(*args, **kwargs)
            raise sqlite3.OperationalError("event storage failed")
        with patch.object(store, "_append_event", side_effect=insert_then_fail):
            with self.assertRaises(sqlite3.OperationalError):
                store.submit(task)
        self.assertIsNone(store.get(task.id))
        self.assertEqual(store.get_events(task.id), [])
        store.submit(task)
        original = store.get_events(task.id)
        with patch.object(store, "_append_event", side_effect=insert_then_fail):
            with self.assertRaises(sqlite3.OperationalError):
                store.claim(task, "owner")
        self.assertEqual(store.get(task.id).status, S.PENDING)
        self.assertEqual(store.get(task.id).attempt_count, 0)
        self.assertEqual(store.get_events(task.id), original)
        claimed = store.claim(task, "owner")
        started = store.get_events(task.id)
        claimed.result = 1
        claimed.status = S.SUCCESS
        with patch.object(store, "_append_event", side_effect=insert_then_fail):
            with self.assertRaises(sqlite3.OperationalError):
                store.finish(claimed, "owner")
        self.assertEqual(store.get(task.id).status, S.RUNNING)
        self.assertEqual(store.get_events(task.id), started)
        store.finish(claimed, "owner")
        self.assertEqual(store.get_events(task.id)[-1].event_type, E.SUCCEEDED)

    def test_competing_claims_emit_one_start_and_stale_finish_emits_nothing(self):
        store = self.store()
        task = store.submit(Task("abs", abs, args=(-1,)))
        with ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(lambda owner: (owner, store.claim(task, owner)), ["a", "b"]))
        winner, claimed = next((owner, task) for owner, task in outcomes if task is not None)
        self.assertEqual(sum(task is not None for _, task in outcomes), 1)
        started = store.get_events(task.id)
        self.assertEqual(sum(e.event_type is E.STARTED for e in started), 1)
        claimed.result = 1
        claimed.status = S.SUCCESS
        with self.assertRaises(PersistenceError):
            store.finish(claimed, "stale")
        self.assertEqual(store.get_events(task.id), started)
        store.finish(claimed, winner)
        finished = store.get_events(task.id)
        with self.assertRaises(PersistenceError):
            store.finish(claimed, winner)
        self.assertEqual(store.get_events(task.id), finished)

    def test_parallel_tasks_have_isolated_contiguous_history(self):
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs}, max_workers=4)
        tasks = engine.submit_many(Task("abs", abs, args=(-index,)) for index in range(12))
        engine.start()
        for task in tasks:
            events = engine.get_events(task.id)
            self.assertEqual([e.sequence for e in events], [1, 2, 3, 4])
            self.assertEqual({e.task_id for e in events}, {task.id})
            self.assertEqual(events[-1].details["result"], abs(task.args[0]))


class EventRecoveryTests(EventStoreFixture):
    def test_interruption_and_retry_keep_previous_owner_and_attempt(self):
        store = self.store()
        task = store.submit(Task("abs", abs, args=(-5,), idempotent=True), retry_limit=1)
        store.claim(task, "crashed-owner")
        recovered = store.recover_interrupted()[0]
        events = store.get_events(task.id)
        self.assertEqual([e.event_type for e in events],
                         [E.CREATED, E.SUBMITTED, E.STARTED, E.INTERRUPTED, E.RETRY_SCHEDULED])
        self.assertEqual(events[3].details["owner"], "crashed-owner")
        self.assertEqual((events[3].status, events[3].attempt_count, events[3].retry_count), (S.FAILED, 1, 0))
        self.assertEqual(recovered.events, tuple(events))
        self.assertEqual(store.recover_interrupted(), [])
        self.assertEqual(store.get_events(task.id), events)
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs})
        engine.start()
        final = engine.get_events(task.id)
        self.assertEqual(final[:5], events)
        self.assertEqual(final[-1].event_type, E.SUCCEEDED)
        self.assertEqual(final[-1].attempt_count, 2)

    def test_non_idempotent_interruption_records_quarantine_without_retry(self):
        store = self.store()
        task = store.submit(Task("abs", abs, args=(-5,)), retry_limit=3)
        store.claim(task, "crashed")
        store.recover_interrupted()
        events = store.get_events(task.id)
        self.assertEqual(events[-1].event_type, E.INTERRUPTED)
        self.assertEqual(events[-1].status, S.FAILED)
        self.assertEqual(events[-1].details["error_type"], "UncertainExecutionError")
        self.assertNotIn(E.RETRY_SCHEDULED, [e.event_type for e in events])

    def test_legacy_database_migration_is_explicit_and_only_once(self):
        store = self.store()
        task = store.submit(Task("abs", abs, args=(-5,)))
        # Reproduce the old schema, where tasks exist but no event table exists.
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DROP TABLE task_events")
            db.commit()
        migrated = self.store()
        events = migrated.get_events(task.id)
        self.assertEqual([e.event_type for e in events], [E.RESTORED])
        self.assertFalse(events[0].details["history_available"])
        self.assertEqual(self.store().get_events(task.id), events)
        engine = TaskEngine(database_path=self.path, task_registry={"abs": abs})
        engine.start()
        self.assertEqual([e.event_type for e in engine.get_events(task.id)],
                         [E.RESTORED, E.STARTED, E.SUCCEEDED])


class EventProcessCrashTests(EventStoreFixture):
    def test_real_process_exit_keeps_only_committed_events(self):
        script = '''
import os, sys
from workflow_engine import Task, TaskEngine, RetryPolicy, SQLiteTaskStore
def action():
    if sys.argv[2] == "running":
        os._exit(23)
    raise ValueError("attempt failed")
finish = SQLiteTaskStore.finish
def crash_after_commit(self, task, owner):
    finish(self, task, owner)
    os._exit(23)
SQLiteTaskStore.finish = crash_after_commit
engine = TaskEngine(database_path=sys.argv[1], task_registry={"action": action}, retry_policy=RetryPolicy(1))
engine.submit(Task("crash", action, id="key"))
engine.start()
'''
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
        for checkpoint in ("running", "retry_committed"):
            with self.subTest(checkpoint=checkpoint):
                path = self.path.with_name(checkpoint + ".db")
                process = subprocess.run([sys.executable, "-c", script, str(path), checkpoint],
                                         env=env, capture_output=True, timeout=15)
                self.assertEqual(process.returncode, 23, process.stderr.decode(errors="replace"))
                store = SQLiteTaskStore(path, {})
                expected = [E.CREATED, E.SUBMITTED, E.STARTED]
                if checkpoint == "retry_committed":
                    expected += [E.FAILED, E.RETRY_SCHEDULED]
                self.assertEqual([e.event_type for e in store.get_events("key")], expected)
                # History remains readable even without the business handler registry.
                events = store.get_events("key")
                self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))


if __name__ == "__main__":
    unittest.main()
