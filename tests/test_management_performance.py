"""Small, repeatable capacity regressions; benchmark tool supplies larger runs."""
from concurrent.futures import ThreadPoolExecutor
import time

from management_test_support import ManagementTestCase


class ManagementPerformanceTests(ManagementTestCase):
    def test_concurrent_mixed_load_completes_once_without_exhausting_service(self):
        self.start_service(max_concurrency=16)
        workers = [self.worker('load-' + str(index)) for index in range(3)]
        started = time.monotonic()

        def request(index):
            before = time.monotonic()
            if index % 3 == 0:
                reply = self.call('/api/v1/workflows', self.workflow('load-' + str(index // 3), -index - 1))
                expected = 202
            else:
                reply = self.call('/api/v1/overview' if index % 3 == 1 else '/api/v1/workflows?limit=10')
                expected = 200
            self.assertEqual(reply[0], expected, reply)
            return time.monotonic() - before

        with ThreadPoolExecutor(8) as pool:
            latencies = list(pool.map(request, range(120)))
        elapsed = time.monotonic() - started
        self.wait_for(lambda: self.call('/api/v1/overview')[1]['workflows']['SUCCESS'] == 40, timeout=15)
        for worker in workers:
            self.assertTrue(worker.shutdown(5))
        totals = self.call('/api/v1/overview')[1]
        self.assertEqual((totals['attempts'], totals['tasks']['SUCCESS'], totals['retries']), (40, 40, 0))
        # Generous regression guard, not a machine-independent throughput claim.
        self.assertLess(sorted(latencies)[113], 5)
        self.assertGreater(120 / elapsed, 1)
        with self.engine._store._connection() as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')

    def test_page_limit_remains_bounded_while_new_runs_are_inserted(self):
        self.start_service()
        with ThreadPoolExecutor(6) as pool:
            list(pool.map(lambda index: self.submit('page-' + str(index)), range(75)))
        cursor, found = 0, []
        while True:
            page = self.call('/api/v1/workflows?limit=7&cursor=' + str(cursor))[1]
            self.assertLessEqual(len(page['items']), 7)
            found.extend(item['id'] for item in page['items'])
            if page['next_cursor'] is None:
                break
            cursor = page['next_cursor']
        self.assertEqual(len(found), 75)
        self.assertEqual(len(set(found)), 75)


if __name__ == '__main__':
    import unittest
    unittest.main()
