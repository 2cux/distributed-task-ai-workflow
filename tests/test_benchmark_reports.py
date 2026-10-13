"""Independent aggregation, warmup accounting and CLI artifact contract tests."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools import benchmark_management as benchmark


def sample(*, seconds=2, rate=50, passed=True):
    return dict(schema_version=2, workload='management-http-two-node-dag',
                timestamp='2026-10-13T00:00:00+00:00', python='3.12', platform='test', sqlite='test',
                passed=passed, configuration=dict(workflows=5, reads=95, concurrency=2, workers=2, deadline=30),
                checks=dict(http_requests=passed), failure_reasons=[] if passed else ['http_requests'],
                lifecycle_errors=[], integrity='ok', verification=dict(passed=True),
                http=dict(requests=100, errors=0 if passed else 1, error_types=[] if passed else ['http_503'],
                          elapsed_seconds=seconds, requests_per_second=rate,
                          latency_ms=dict(p50=10, p95=20, p99=30, maximum=40)),
                execution=dict(completed_workflows=5, attempts=10, retries=0,
                               total_elapsed_seconds=seconds, workflows_per_second=5 / seconds))


class BenchmarkReportTests(unittest.TestCase):
    def test_aggregate_throughput_weights_elapsed_time_and_keeps_failed_rounds(self):
        result = benchmark.summarize_runs([sample(), sample(seconds=8, rate=12.5, passed=False)])
        self.assertEqual((result['measured_runs'], result['passed_runs']), (2, 1))
        self.assertEqual(result['aggregate']['requests_per_second'], 20)
        self.assertEqual(result['aggregate']['workflows_per_second'], 1)
        self.assertEqual(result['aggregate']['errors'], 1)
        self.assertEqual(result['round_statistics']['http_requests_per_second']['mean'], 31.25)
        self.assertEqual(result['round_statistics']['http_requests_per_second']['sample_stdev'], 26.517)

    def test_single_round_statistics_and_empty_run_guard(self):
        self.assertEqual(benchmark.summarize_runs([sample()])['round_statistics']['http_p95_latency_ms'],
                         dict(minimum=20, median=20, maximum=20, mean=20, sample_stdev=0.0))
        with self.assertRaises(ValueError):
            benchmark.summarize_runs([])

    def test_warmup_is_retained_and_excluded_but_failure_fails_suite(self):
        warmup = sample(seconds=100, rate=1, passed=False)
        with patch.object(benchmark, 'run_benchmark', side_effect=[warmup, sample(), sample()]) as run:
            result = benchmark.run_suite(repeat=2, warmup=1, workflows=5, reads=95)
        self.assertEqual(run.call_count, 3)
        self.assertFalse(result['passed'])
        self.assertEqual(result['warmup_runs'], [warmup])
        self.assertEqual(result['summary']['passed_runs'], 2)
        self.assertEqual(result['summary']['aggregate']['requests'], 200)
        self.assertEqual(result['summary']['aggregate']['http_elapsed_seconds'], 4)
        self.assertEqual(result['summary']['aggregate']['requests_per_second'], 50)

    def test_markdown_discloses_failure_and_percentile_semantics(self):
        with patch.object(benchmark, 'run_benchmark', side_effect=[sample(), sample(passed=False)]):
            result = benchmark.run_suite(repeat=2, warmup=0)
        markdown = benchmark.render_markdown(result)
        self.assertIn('Result: **FAIL**', markdown)
        self.assertIn('Measured 2: http_requests', markdown)
        self.assertIn('http_503', markdown)
        self.assertIn('not pooled request percentiles', markdown)
        self.assertIn('Passed measured rounds: 1/2', markdown)
        self.assertIn('Result: **PASS**', benchmark.render_markdown(sample()))

    def test_cli_writes_json_and_markdown_and_returns_failure_status(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd(), prefix='.benchmark-report-test-') as directory:
            output, markdown = Path(directory) / 'nested/result.json', Path(directory) / 'nested/result.md'
            for passed, expected_status in ((True, 0), (False, 1)):
                result = sample(passed=passed)
                with self.subTest(passed=passed), patch.object(benchmark, 'run_benchmark', return_value=result), \
                        redirect_stdout(io.StringIO()) as stdout:
                    status = benchmark.main(['--output', str(output), '--markdown', str(markdown)])
                self.assertEqual(status, expected_status)
                self.assertEqual(json.loads(output.read_text(encoding='utf-8')), result)
                self.assertEqual(json.loads(stdout.getvalue()), result)
                self.assertIn('**PASS**' if passed else '**FAIL**', markdown.read_text(encoding='utf-8'))

    def test_cli_repeat_and_warmup_select_suite(self):
        with patch.object(benchmark, 'run_suite', return_value=sample()) as suite, \
                patch.object(benchmark, 'run_benchmark') as single, redirect_stdout(io.StringIO()):
            self.assertEqual(benchmark.main(['--repeat', '2', '--warmup', '1']), 0)
        self.assertEqual(suite.call_args.kwargs['repeat'], 2)
        self.assertEqual(suite.call_args.kwargs['warmup'], 1)
        single.assert_not_called()

    def test_cli_rejects_invalid_rounds_and_output_collision_before_running(self):
        with patch.object(benchmark, 'run_benchmark') as single, patch.object(benchmark, 'run_suite') as suite:
            for options in (['--repeat', '0'], ['--warmup', '-1'],
                            ['--output', 'result.json', '--markdown', './result.json']):
                with self.subTest(options=options), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    benchmark.main(options)
                self.assertEqual(error.exception.code, 2)
        single.assert_not_called()
        suite.assert_not_called()


if __name__ == '__main__':
    unittest.main()
