"""Independently runnable real HTTP/SQLite/worker/CLI benchmark regressions."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools import benchmark_management as benchmark


ROOT = Path(__file__).resolve().parents[1]


class BenchmarkIntegrationTests(unittest.TestCase):
    def assert_complete(self, result, workflows, reads):
        self.assertTrue(result['passed'], result)
        self.assertTrue(all(result['checks'].values()))
        self.assertEqual(result['failure_reasons'], [])
        self.assertEqual(result['http']['requests'], workflows + reads)
        self.assertEqual(result['http']['errors'], 0)
        self.assertEqual(result['execution']['completed_workflows'], workflows)
        self.assertEqual(result['execution']['attempts'], workflows * 2)
        self.assertEqual(result['execution']['retries'], 0)
        self.assertEqual(result['verification']['observed_tasks'], workflows * 2)
        self.assertEqual(result['integrity'], 'ok')
        latencies = result['http']['latency_ms']
        self.assertLessEqual(latencies['p50'], latencies['p95'])
        self.assertLessEqual(latencies['p95'], latencies['p99'])
        self.assertLessEqual(latencies['p99'], latencies['maximum'])

    def test_repeated_rounds_have_fresh_databases_and_cleanup(self):
        original = tempfile.TemporaryDirectory
        paths = []

        def directory(*args, **kwargs):
            self.assertTrue(all(not path.exists() for path in paths))
            temporary = original(*args, **kwargs)
            paths.append(Path(temporary.name))
            return temporary

        with patch.object(benchmark.tempfile, 'TemporaryDirectory', side_effect=directory):
            result = benchmark.run_suite(repeat=2, warmup=1, workflows=4, reads=8,
                                         concurrency=2, workers=2, deadline=30)
        self.assertEqual(len(paths), 3)
        self.assertEqual(len(set(paths)), 3)
        self.assertTrue(all(not path.exists() for path in paths))
        self.assertTrue(result['passed'])
        for run in result['warmup_runs'] + result['runs']:
            self.assert_complete(run, 4, 8)
        self.assertEqual(result['summary']['aggregate']['requests'], 24)
        self.assertEqual(result['summary']['aggregate']['completed_workflows'], 8)
        self.assertEqual(result['summary']['aggregate']['attempts'], 16)

    def test_zero_reads_still_measures_and_verifies_the_dag(self):
        self.assert_complete(benchmark.run_benchmark(workflows=3, reads=0, concurrency=1,
                                                    workers=1, deadline=30), 3, 0)

    def test_expired_completion_cutoff_produces_failed_evidence(self):
        # The HTTP batch takes longer than this cutoff on any supported clock.
        result = benchmark.run_benchmark(workflows=1, reads=0, concurrency=1, workers=1, deadline=1e-9)
        self.assertFalse(result['passed'])
        self.assertIn('completion_before_deadline', result['failure_reasons'])
        self.assertEqual(result['integrity'], 'ok')
        self.assertTrue(result['checks']['clean_shutdown'], result)
        json.dumps(result, allow_nan=False)

    def test_cli_real_rounds_emit_matching_json_and_markdown(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix='.benchmark-cli-test-') as directory:
            output, markdown = Path(directory) / 'report.json', Path(directory) / 'report.md'
            environment = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
            completed = subprocess.run([sys.executable, str(ROOT / 'tools/benchmark_management.py'),
                                       '--workflows', '3', '--reads', '6', '--workers', '2', '--concurrency', '2',
                                       '--repeat', '2', '--deadline', '30', '--output', str(output),
                                       '--markdown', str(markdown)], cwd=ROOT, env=environment,
                                       capture_output=True, text=True, encoding='utf-8', timeout=90)
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            report = json.loads(output.read_text(encoding='utf-8'))
            self.assertEqual(json.loads(completed.stdout), report)
            self.assertTrue(report['passed'])
            self.assertEqual(len(report['runs']), 2)
            for run in report['runs']:
                self.assert_complete(run, 3, 6)
            text = markdown.read_text(encoding='utf-8')
            self.assertIn('Result: **PASS**', text)
            self.assertIn('Passed measured rounds: 2/2', text)


if __name__ == '__main__':
    unittest.main()
