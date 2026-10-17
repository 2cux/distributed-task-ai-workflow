"""Independent correctness, contention instrumentation and regression gate tests."""
from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tools import benchmark_ai_workflow as bench
from workflow_engine import DAGWorkflow, LLMConfig, TaskEngine, WorkflowOrchestrator, create_llm_registry


class ConfigurationTests(unittest.TestCase):
    def test_committed_profiles(self):
        for path in Path('benchmarks').glob('ai-*.json'):
            profile = json.loads(path.read_text())
            bench.validate(profile['configuration'], profile['limits'])

    def test_invalid_configuration_precedes_side_effects(self):
        for change in (dict(duration=float('nan')), dict(duration=0), dict(workers=True),
                       dict(concurrencies=[1, 1]), dict(concurrencies=[129]), dict(repeat=0)):
            with self.subTest(change=change), patch.object(bench, 'run_round') as run:
                with self.assertRaises(ValueError):
                    bench.run_suite({**bench.DEFAULT, **change}, bench.LIMITS)
                run.assert_not_called()

    def test_invalid_limits(self):
        for change in (dict(http_failure_rate=2), dict(max_latency_ratio=.9), dict(busy_errors=-1)):
            with self.assertRaises(ValueError):
                bench.validate(bench.DEFAULT, {**bench.LIMITS, **change})

    def test_histogram_includes_failures_and_bounds_quantiles(self):
        samples = bench.Samples()
        for index in range(1, 101):
            samples.add(index / 1000, 'timeout' if index > 95 else None)
        report = samples.report()
        self.assertEqual(report['errors'], 5)
        self.assertEqual(report['failure_rate'], .05)
        self.assertGreaterEqual(report['latency_ms']['p95'], 95)
        self.assertLessEqual(report['latency_ms']['p95'], 95 * 1.01)
        self.assertEqual(report['latency_ms']['maximum'], 100)
        self.assertEqual(bench.Samples().report()['latency_ms']['p95'], 0)


def passing_run():
    sample = bench.Samples()
    sample.add(.01)
    return dict(concurrency=1, round=1, passed=True, failure_reasons=[], admission_seconds=1,
        elapsed_seconds=1, http=sample.report(), workflows={**sample.report(), 'workflows_per_second': 5},
        database=dict(writer_wait=sample.report(), sqlite_begin=sample.report(), busy_errors=0),
        verification=dict(passed=True, checks=dict(outputs=True)), lifecycle_errors=[])


class GateTests(unittest.TestCase):
    def report(self):
        return dict(schema_version=1, workload=bench.WORKLOAD, complete=True, passed=True,
            configuration={**bench.DEFAULT, 'concurrencies': [1], 'repeat': 1},
            environment=dict(python='3.12', sqlite='3.45', platform='test'), limits=deepcopy(bench.LIMITS),
            runs=[passing_run()], warmup_runs=[], failure_reasons=[], timestamp='test')

    def test_each_absolute_gate_and_correctness(self):
        run = passing_run()
        self.assertEqual(bench.gate(run, bench.LIMITS), [])
        for field in ('http', 'workflows'):
            changed = deepcopy(run)
            changed[field]['failure_rate'] = .01
            self.assertIn('http_failure_rate' if field == 'http' else 'workflow_failure_rate',
                          bench.gate(changed, bench.LIMITS))
        changed = deepcopy(run)
        changed['database']['busy_errors'] = 1
        changed['verification'] = dict(passed=False, checks=dict(outputs=False))
        changed['lifecycle_errors'] = ['worker_error']
        self.assertEqual(set(bench.gate(changed, bench.LIMITS)),
                         {'busy_errors', 'verification.outputs', 'lifecycle_errors'})
        changed = deepcopy(run)
        changed['http']['latency_ms']['p95'] = 2001
        changed['workflows']['workflows_per_second'] = 0
        self.assertEqual(set(bench.gate(changed, bench.LIMITS)),
                         {'http_p95_ms', 'min_workflows_per_second'})

    def test_relative_regressions(self):
        baseline = self.report()
        current = deepcopy(baseline)
        current['runs'][0]['workflows']['workflows_per_second'] = 3
        current['runs'][0]['http']['latency_ms']['p95'] = 20
        self.assertEqual(set(bench.compare(current, baseline)),
                         {'regression.c1.min_workflows_per_second', 'regression.c1.http_p95_ms'})

    def test_baseline_mismatch_failed_partial_and_missing_rounds_rejected(self):
        current = self.report()
        for field, value in [('passed', False), ('complete', False), ('schema_version', 9),
                             ('environment', {}), ('configuration', {}), ('limits', {})]:
            baseline = deepcopy(current)
            baseline[field] = value
            self.assertEqual(bench.compare(current, baseline), ['baseline_incompatible_or_failed'])
        baseline = deepcopy(current)
        baseline['runs'] = []
        self.assertEqual(bench.compare(current, baseline), ['baseline_invalid_rounds'])

    def test_warmup_failure_not_hidden_and_rotation(self):
        config = {**bench.DEFAULT, 'concurrencies': [1, 4], 'repeat': 2}
        def fake(config, concurrency, duration):
            run = passing_run()
            run['concurrency'] = concurrency
            if duration == config['warmup']:
                run['http']['failure_rate'] = .01
            return run
        with patch.object(bench, 'run_round', side_effect=fake):
            report = bench.run_suite(config, bench.LIMITS)
        self.assertFalse(report['passed'])
        self.assertEqual(len(report['failure_reasons']), 4)
        self.assertEqual([r['concurrency'] for r in report['runs']], [1, 4, 4, 1])
        self.assertTrue(all(run['passed'] for run in report['runs']))

    def test_markdown_reports_failed_rounds(self):
        report = self.report()
        report['passed'] = False
        report['runs'][0]['passed'] = False
        self.assertIn('**FAIL**', bench.render_markdown(report))

    def test_cli_failure_writes_reviewable_evidence_and_returns_one(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            profile = root / 'profile.json'
            profile.write_text(json.dumps(dict(configuration=bench.DEFAULT, limits=bench.LIMITS)))
            report = self.report()
            report['passed'] = False
            with patch.object(bench, 'run_suite', return_value=report), redirect_stdout(io.StringIO()):
                code = bench.main(['--profile', str(profile), '--output', str(root / 'result.json'),
                                   '--markdown', str(root / 'result.md')])
            self.assertEqual(code, 1)
            self.assertFalse(json.loads((root / 'result.json').read_text())['passed'])
            self.assertIn('FAIL', (root / 'result.md').read_text())


class DatabaseProbeTests(unittest.TestCase):
    def test_real_sqlite_contention_is_measured_and_not_swallowed(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / 'test.db'
            engine = TaskEngine(database_path=path, task_registry={'abs': abs})
            probe = bench.DatabaseProbe(engine._store)
            blocker = sqlite3.connect(path, isolation_level=None)
            try:
                blocker.execute('BEGIN IMMEDIATE')
                with engine._store.connection_timeout(.02), self.assertRaises(sqlite3.OperationalError):
                    with engine._store._transaction():
                        pass
                self.assertEqual(probe.report()['busy_errors'], 1)
                self.assertGreaterEqual(probe.report()['sqlite_begin']['latency_ms']['p95'], 10)
            finally:
                blocker.close()
                probe.close()
            with engine._store._transaction() as db:
                self.assertIsInstance(db, sqlite3.Connection)

    def test_empty_verification_cannot_pass(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            engine = TaskEngine(database_path=Path(directory) / 'test.db', task_registry={'abs': abs})
            self.assertFalse(bench.verify(engine._store, set(), {})['passed'])


class IntegrationTests(unittest.TestCase):
    def test_persisted_corruption_and_duplicate_provider_calls_fail_verification(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory, bench.provider(0) as (url, calls):
            engine = TaskEngine(database_path=Path(directory) / 'test.db',
                task_registry=create_llm_registry(LLMConfig('local-key', 'model', url)))
            engine.submit_workflow(DAGWorkflow.from_dict(bench.workflow('verify')))
            WorkflowOrchestrator(engine).run('verify', timeout=10)
            self.assertTrue(bench.verify(engine._store, {'verify'}, calls)['passed'])
            with engine._store._transaction() as db:
                db.execute("UPDATE tasks SET result='{}',attempt_count=2")
            calls['verify'] += 1
            report = bench.verify(engine._store, {'verify'}, calls)
            self.assertFalse(report['checks']['outputs_attempts_events'])
            self.assertFalse(report['checks']['provider_exactly_once'])

    def test_continuous_real_http_llm_sqlite_and_cleanup(self):
        before = set(Path.cwd().glob('.ai-benchmark-*'))
        config = {**bench.DEFAULT, 'duration': .4, 'workers': 2, 'window': .1, 'warmup': 0}
        run = bench.run_round(config, 2)
        self.assertEqual(bench.gate(run, bench.LIMITS), [], run)
        self.assertEqual(run['verification']['tasks'], run['workflows']['count'] * 2)
        self.assertEqual(run['verification']['provider_calls'], run['workflows']['count'])
        self.assertGreater(len(run['windows']), 0)
        self.assertGreater(len(run['database']['windows']), 0)
        self.assertEqual(set(Path.cwd().glob('.ai-benchmark-*')), before)


if __name__ == '__main__':
    unittest.main()
