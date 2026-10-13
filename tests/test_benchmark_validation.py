"""Independent input, measurement and persisted-result checks; no HTTP needed."""
import unittest
from unittest.mock import patch

from tools import benchmark_management as benchmark


class BenchmarkValidationTests(unittest.TestCase):
    def test_invalid_configuration_fails_before_creating_resources(self):
        invalid = {
            'workflows': (0, -1, True, 1.5, '2'),
            'reads': (-1, False, 1.5, '2'),
            'concurrency': (0, -1, True, 129),
            'workers': (0, -1, False, 65),
            'deadline': (0, -1, True, '2', float('nan'), float('inf')),
        }
        with patch.object(benchmark.tempfile, 'TemporaryDirectory') as directory:
            for name, values in invalid.items():
                for value in values:
                    with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                        benchmark.run_benchmark(**{name: value})
            directory.assert_not_called()

    def test_configuration_boundaries_allow_write_only_load(self):
        benchmark._validate_configuration(workflows=1, reads=0, concurrency=128, workers=64, deadline=.001)

    def test_invalid_rounds_and_suite_inputs_allocate_no_run(self):
        with patch.object(benchmark, 'run_benchmark') as run:
            for options in ({'repeat': 0}, {'repeat': True}, {'repeat': 1.5},
                            {'warmup': -1}, {'warmup': False}, {'warmup': '1'}, {'reads': -1}):
                with self.subTest(options=options), self.assertRaises(ValueError):
                    benchmark.run_suite(**options)
            run.assert_not_called()

    def test_nearest_rank_latency_uses_seconds_and_keeps_the_tail(self):
        self.assertEqual(benchmark._latency_summary([.004, .001, .1, .003, .002]),
                         dict(p50=3.0, p95=100.0, p99=100.0, maximum=100.0))
        self.assertEqual(benchmark._latency_summary([0]), dict(p50=0, p95=0, p99=0, maximum=0))
        for samples in ([], [-.1], [float('nan')], [float('inf')]):
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                benchmark._latency_summary(samples)

    @staticmethod
    def rows():
        return [dict(workflow_id='benchmark-0', node_id=node, result='0', status='SUCCESS',
                     attempt_count=1, retry_count=0) for node in ('root', 'join')]

    def test_result_verification_rejects_missing_duplicate_and_unexpected_nodes(self):
        rows = self.rows()
        self.assertTrue(benchmark._verify_tasks(rows, 1)['passed'])
        for bad in ([], rows[:1], [rows[0], rows[0]], rows + [rows[1]],
                    [rows[0], {**rows[1], 'workflow_id': 'benchmark-1'}]):
            with self.subTest(rows=bad):
                self.assertFalse(benchmark._verify_tasks(bad, 1)['passed'])

    def test_result_verification_checks_root_and_join_and_json_types(self):
        for index in (0, 1):
            for change in ({'result': '1'}, {'result': 'false'}, {'result': '0.0'},
                           {'result': None}, {'result': 'invalid-json'}, {'status': 'FAILED'}):
                rows = self.rows()
                rows[index].update(change)
                with self.subTest(index=index, change=change):
                    report = benchmark._verify_tasks(rows, 1)
                    self.assertFalse(report['passed'])
                    self.assertEqual(report['invalid_results'], 1)

    def test_total_attempt_count_cannot_hide_duplicate_and_unexecuted_tasks(self):
        rows = self.rows()
        rows[0]['attempt_count'], rows[1]['attempt_count'] = 2, 0
        self.assertEqual(sum(row['attempt_count'] for row in rows), 2)
        verification = benchmark._verify_tasks(rows, 1)
        self.assertFalse(verification['passed'])
        self.assertEqual(verification['invalid_attempts'], 2)
        rows = self.rows()
        rows[1]['retry_count'] = 1
        self.assertFalse(benchmark._verify_tasks(rows, 1)['passed'])


if __name__ == '__main__':
    unittest.main()
