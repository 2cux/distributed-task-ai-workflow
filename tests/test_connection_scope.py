"""SQLite reuse must preserve transaction, timeout, thread and shutdown boundaries."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import tempfile
from threading import Barrier
import unittest
from unittest.mock import patch

from workflow_engine import Task, TaskEngine
from workflow_engine.remote import RemoteTaskStore


class ConnectionScopeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix='.connection-scope-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'engine.db'
        self.engine = TaskEngine(database_path=self.path, task_registry={'abs': abs})
        self.store = self.engine._store

    def test_reuses_only_within_scope_and_closes_on_exception(self):
        with self.assertRaisesRegex(RuntimeError, 'stop'):
            with self.store.connection_scope():
                with self.store._connection() as first:
                    self.assertEqual(first.execute('PRAGMA synchronous').fetchone()[0], 2)
                    self.assertEqual(first.execute('PRAGMA foreign_keys').fetchone()[0], 1)
                with self.store._connection() as second:
                    self.assertIs(first, second)
                raise RuntimeError('stop')
        with self.assertRaises(sqlite3.ProgrammingError):
            first.execute('SELECT 1')
        with self.store._connection() as fresh:
            self.assertIsNot(first, fresh)

    def test_nested_scopes_do_not_close_outer_handle(self):
        with self.store.connection_scope():
            with self.store._connection() as first:
                pass
            with self.store.connection_scope(), self.store._connection() as second:
                self.assertIs(first, second)
            with self.store._connection() as third:
                self.assertIs(first, third)

    def test_nested_leases_are_independent_connections(self):
        with self.store.connection_scope():
            with self.store._connection() as outer:
                outer.execute('BEGIN')
                with self.store._connection() as inner:
                    self.assertIsNot(outer, inner)
                    inner.execute('BEGIN')
                self.assertTrue(outer.in_transaction)
            with self.assertRaises(sqlite3.ProgrammingError):
                inner.execute('SELECT 1')
            with self.store._connection() as reused:
                self.assertIs(reused, outer)
                self.assertFalse(reused.in_transaction)

    def test_read_snapshot_is_released_between_leases(self):
        self.engine.submit(Task('sample', abs, id='sample', args=(-1,)))
        with self.store.connection_scope():
            with self.store._connection() as db:
                db.execute('BEGIN')
                self.assertEqual(db.execute('SELECT status FROM tasks').fetchone()[0], 'PENDING')
            with closing(sqlite3.connect(self.path, isolation_level=None)) as other:
                other.execute("UPDATE tasks SET status='SUCCESS'")
            with self.store._connection() as db:
                db.execute('BEGIN')
                self.assertEqual(db.execute('SELECT status FROM tasks').fetchone()[0], 'SUCCESS')

    def test_uncommitted_changes_roll_back_on_success_and_failure(self):
        self.engine.submit(Task('sample', abs, id='sample', args=(-1,)))
        with self.store.connection_scope():
            for failure in (False, True):
                try:
                    with self.store._connection() as db:
                        db.execute('BEGIN IMMEDIATE')
                        db.execute("UPDATE tasks SET status='FAILED'")
                        if failure:
                            raise RuntimeError('abort')
                except RuntimeError:
                    pass
                self.assertEqual(self.engine.get_task('sample').status, 'PENDING')

    def test_busy_timeout_is_restored_after_overrides(self):
        with self.store.connection_scope():
            with self.store.connection_timeout(.025), self.store._connection() as db:
                self.assertEqual(db.execute('PRAGMA busy_timeout').fetchone()[0], 25)
                db.execute('PRAGMA busy_timeout=1')
            with self.store._connection() as db:
                self.assertEqual(db.execute('PRAGMA busy_timeout').fetchone()[0], 30000)

    def test_parallel_consumers_never_share_connections(self):
        barrier = Barrier(2)
        def consumer(_):
            with self.store.connection_scope():
                with self.store._connection() as first:
                    barrier.wait(5)
                with self.store._connection() as second:
                    self.assertIs(first, second)
            return first
        with ThreadPoolExecutor(2) as pool:
            handles = list(pool.map(consumer, range(2)))
        self.assertIsNot(handles[0], handles[1])

    def test_remote_adapter_scope_never_opens_a_database(self):
        with patch('sqlite3.connect', side_effect=AssertionError('remote opened SQLite')):
            remote = RemoteTaskStore('http://127.0.0.1:1', {'abs': abs}, 'test-token')
            with remote.connection_scope():
                pass

    def test_scope_entry_failure_still_stops_heartbeat_and_confirms_drain(self):
        @contextmanager
        def failed_scope():
            raise RuntimeError('scope failed')
            yield
        worker = self.engine.create_worker('scope-failure')
        with patch.object(self.store, 'connection_scope', failed_scope):
            worker.start()
            self.assertTrue(worker.join(5))
        self.assertIsInstance(worker.last_error, RuntimeError)
        self.assertFalse(worker._heartbeat_thread.is_alive())
        self.assertEqual(self.engine.worker_registry.get_session(worker.session_id).state, 'STOPPED')

    def test_registry_writes_cannot_bypass_the_local_writer_queue(self):
        record = self.engine.worker_registry.register('queued-heartbeat')
        with self.store._writer_slot(), self.store.connection_timeout(.01):
            with self.assertRaises(sqlite3.OperationalError) as caught:
                self.engine.worker_registry.heartbeat('queued-heartbeat', record.session_id)
        self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_BUSY)
        # A timed out registry waiter must not poison the FIFO for later writes.
        self.engine.worker_registry.heartbeat('queued-heartbeat', record.session_id)
        self.assertEqual(self.engine.worker_registry.get_session(record.session_id).state, 'ACTIVE')


if __name__ == '__main__':
    unittest.main()
