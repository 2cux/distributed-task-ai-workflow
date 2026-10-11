"""Focused tests for durable Worker identity and heartbeat visibility."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest

from workflow_engine.worker_registry import (
    WorkerAlreadyActive, WorkerRegistry, WorkerSessionMismatch,
)


class WorkerRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "tasks.sqlite3"
        self.now = 100.0
        self.registry = WorkerRegistry(self.path, clock=lambda: self.now)

    def test_registration_heartbeat_and_restart(self):
        first = self.registry.register("worker-a")
        self.assertEqual((first.status, first.registered_at, first.last_heartbeat_at), ("ACTIVE", 100, 100))
        self.now = 105
        touched = self.registry.heartbeat("worker-a", first.session_id)
        self.assertEqual(touched.last_heartbeat_at, 105)
        reopened = WorkerRegistry(self.path, clock=lambda: self.now)
        self.assertEqual(reopened.get("worker-a"), touched)
        self.assertFalse(reopened.is_overdue("worker-a", 10))
        self.now = 115.1
        self.assertTrue(reopened.is_overdue("worker-a", 10))
        self.assertEqual(reopened.get("worker-a").status, "ACTIVE")  # no takeover

    def test_stopped_id_can_be_reused_but_old_session_is_fenced(self):
        a = self.registry.register("worker-a")
        self.now = 105
        stopped = self.registry.stop("worker-a", a.session_id)
        self.assertEqual(stopped.status, "STOPPED")
        self.assertFalse(self.registry.is_overdue("worker-a", 1))
        self.now = 110
        b = self.registry.register("worker-a")
        self.assertNotEqual(a.session_id, b.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.heartbeat("worker-a", a.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.stop("worker-a", a.session_id)
        self.assertEqual(self.registry.get("worker-a"), b)

    def test_duplicate_active_registration_is_rejected_even_if_overdue(self):
        self.registry.register("worker-a")
        self.now = 999
        self.assertTrue(self.registry.is_overdue("worker-a", 1))
        with self.assertRaises(WorkerAlreadyActive):
            self.registry.register("worker-a")

    def test_unknown_and_stopped_session_rejected(self):
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.heartbeat("missing", "token")
        a = self.registry.register("worker-a")
        self.registry.stop("worker-a", a.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.heartbeat("worker-a", a.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            self.registry.stop("worker-a", a.session_id)
        with self.assertRaises(KeyError):
            self.registry.is_overdue("missing", 10)

    def test_heartbeat_cannot_move_backwards(self):
        a = self.registry.register("worker-a")
        self.now = 200
        self.registry.heartbeat("worker-a", a.session_id)
        self.now = 150
        self.assertEqual(self.registry.heartbeat("worker-a", a.session_id).last_heartbeat_at, 200)

    def test_invalid_inputs(self):
        with self.assertRaises(ValueError):
            self.registry.register("  ")
        with self.assertRaises(ValueError):
            self.registry.heartbeat("a", "")
        with self.assertRaises(ValueError):
            self.registry.is_overdue("x", 0)
        with self.assertRaises(TypeError):
            self.registry.is_overdue("x", True)
        with self.assertRaises(ValueError):
            self.registry.is_overdue("x", float("nan"))
        with self.assertRaises(ValueError):
            WorkerRegistry(":memory:")
        self.now = float("inf")
        with self.assertRaises(ValueError):
            self.registry.register("worker-x")

    def test_two_registry_instances_observe_same_records(self):
        other = WorkerRegistry(self.path, clock=lambda: self.now)
        a = self.registry.register("worker-a")
        b = other.register("worker-b")
        self.assertEqual([record.worker_id for record in other.list_workers()], ["worker-a", "worker-b"])
        self.now = 120
        other.heartbeat("worker-a", a.session_id)
        self.assertFalse(self.registry.is_overdue("worker-a", 5))
        self.assertTrue(self.registry.is_overdue("worker-b", 5))
        self.assertEqual(b.status, "ACTIVE")

    def test_parallel_registration_has_single_winner(self):
        def register():
            try:
                return self.registry.register("worker-a")
            except WorkerAlreadyActive:
                return None
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda _: register(), range(4)))
        self.assertEqual(sum(o is not None for o in outcomes), 1)

    def test_coexists_with_existing_tables(self):
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE tasks_stub (id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO tasks_stub VALUES ('existing')")
        WorkerRegistry(self.path).register("worker-a")
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT id FROM tasks_stub").fetchone()[0], "existing")


if __name__ == "__main__":
    unittest.main()
