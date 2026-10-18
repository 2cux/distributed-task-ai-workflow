"""SQLite reuse must preserve transaction, timeout, thread and shutdown boundaries."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import tempfile
from threading import Barrier, Event, get_ident
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

    def test_slow_connection_setup_or_close_does_not_block_unrelated_writer(self):
        for phase in ('setup', 'close'):
            with self.subTest(phase=phase):
                entered, release = Event(), Event()
                target_thread = [None]
                original = self.store._connection

                @contextmanager
                def slow_connection():
                    targeted = get_ident() == target_thread[0]
                    if targeted and phase == 'setup':
                        entered.set()
                        if not release.wait(5):
                            raise TimeoutError('connection setup gate')
                    with original() as db:
                        yield db
                    if targeted and phase == 'close':
                        entered.set()
                        if not release.wait(5):
                            raise TimeoutError('connection close gate')

                def slow_writer():
                    target_thread[0] = get_ident()
                    with self.store._transaction() as db:
                        db.execute('SELECT 1')

                def other_writer():
                    with self.store._transaction() as db:
                        return db.execute('SELECT 42').fetchone()[0]

                with patch.object(self.store, '_connection', slow_connection), ThreadPoolExecutor(2) as pool:
                    first = pool.submit(slow_writer)
                    try:
                        self.assertTrue(entered.wait(3))
                        self.assertEqual(pool.submit(other_writer).result(timeout=2), 42)
                    finally:
                        release.set()
                    first.result(timeout=3)

    def test_pool_never_lends_one_handle_to_two_threads_and_bounds_idle_handles(self):
        pool = self.store._connection_pool(2)
        self.addCleanup(pool.close)
        barrier = Barrier(2)

        def borrow(_):
            with self.store.connection_scope(pool=pool), self.store._connection() as db:
                barrier.wait(3)
                self.assertEqual(db.execute('SELECT 42').fetchone()[0], 42)
                return db

        with ThreadPoolExecutor(2) as workers:
            handles = list(workers.map(borrow, range(2)))
        self.assertIsNot(handles[0], handles[1])
        self.assertEqual(len(pool._idle), 2)
        pool.close()
        for handle in handles:
            with self.assertRaises(sqlite3.ProgrammingError):
                handle.execute('SELECT 1')

    def test_pool_releases_snapshot_rolls_back_errors_and_restores_settings(self):
        pool = self.store._connection_pool(1)
        self.addCleanup(pool.close)
        self.engine.submit(Task('sample', abs, id='sample', args=(-1,)))
        with self.assertRaisesRegex(RuntimeError, 'abort'):
            with self.store.connection_timeout(.025), self.store.connection_scope(pool=pool):
                with self.store._connection() as first:
                    self.assertEqual(first.execute('PRAGMA busy_timeout').fetchone()[0], 25)
                    first.execute('BEGIN IMMEDIATE')
                    first.execute("UPDATE tasks SET status='FAILED'")
                    raise RuntimeError('abort')
        self.assertFalse(first.in_transaction)
        self.assertEqual(self.engine.get_task('sample').status, 'PENDING')
        with closing(sqlite3.connect(self.path, isolation_level=None)) as other:
            other.execute("UPDATE tasks SET status='SUCCESS'")
        with self.store.connection_timeout(.2), self.store.connection_scope(pool=pool):
            with self.store._connection() as second:
                self.assertIs(first, second)
                self.assertEqual(second.execute('PRAGMA busy_timeout').fetchone()[0], 200)
                self.assertEqual(second.execute('PRAGMA synchronous').fetchone()[0], 2)
                self.assertEqual(second.execute('PRAGMA foreign_keys').fetchone()[0], 1)
                self.assertEqual(second.execute('SELECT status FROM tasks').fetchone()[0], 'SUCCESS')

    def test_pool_shutdown_drains_borrowers_and_closes_late_request_handles(self):
        pool = self.store._connection_pool(1)
        self.addCleanup(pool.close)
        with self.store.connection_scope(pool=pool), self.store._connection() as active:
            pool.close()
            self.assertEqual(active.execute('SELECT 1').fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            active.execute('SELECT 1')
        # An accepted handler may reach SQLite after a bounded shutdown returns.
        with self.store.connection_scope(pool=pool), self.store._connection() as late:
            self.assertEqual(late.execute('SELECT 1').fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            late.execute('SELECT 1')
        self.assertEqual(pool._idle, [])

    def test_invalid_pooled_handle_is_discarded_after_lease_failure(self):
        pool = self.store._connection_pool(1)
        self.addCleanup(pool.close)
        with self.assertRaises(sqlite3.ProgrammingError):
            with self.store.connection_scope(pool=pool), self.store._connection() as broken:
                broken.close()
        with self.store.connection_scope(pool=pool), self.store._connection() as healthy:
            self.assertIsNot(broken, healthy)
            self.assertEqual(healthy.execute('SELECT 1').fetchone()[0], 1)

    def test_pool_admission_timeout_does_not_poison_capacity_or_waiting_requests(self):
        pool = self.store._connection_pool(1)
        self.addCleanup(pool.close)

        def borrower():
            with self.store.connection_timeout(.025), self.store.connection_scope(pool=pool):
                with self.store._connection() as db:
                    return db.execute('SELECT 42').fetchone()[0]

        with ThreadPoolExecutor(1) as workers:
            with self.store.connection_scope(pool=pool), self.store._connection():
                with self.assertRaises(sqlite3.OperationalError) as caught:
                    workers.submit(borrower).result(timeout=2)
                self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_BUSY)
            self.assertEqual(workers.submit(borrower).result(timeout=2), 42)
        self.assertEqual(pool._active, 0)
        self.assertEqual(len(pool._waiters), 0)

    def test_pool_connection_creation_failure_releases_admission_slot(self):
        pool = self.store._connection_pool(1)
        self.addCleanup(pool.close)
        with patch.object(self.store, '_open_connection', side_effect=OSError('open failed')):
            with self.assertRaises(OSError):
                with self.store.connection_scope(pool=pool), self.store._connection():
                    self.fail('unreachable')
        with self.store.connection_scope(pool=pool), self.store._connection() as db:
            self.assertEqual(db.execute('SELECT 1').fetchone()[0], 1)
        self.assertEqual(pool._active, 0)


if __name__ == '__main__':
    unittest.main()
