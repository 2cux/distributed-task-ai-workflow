"""Standalone local HTTP/SQLite capacity run; emits machine-readable evidence.

Uses its own temporary database and workers; never sends load to a live deployment.
Exit status is nonzero if requests, results, attempt counts or DB integrity fail.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import argparse
import json
import math
from pathlib import Path
import platform
import sqlite3
import statistics
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from workflow_engine import RetryPolicy, TaskEngine


def _identity(value):
    return value


def _validate_configuration(workflows, reads, concurrency, workers, deadline):
    for name, value in (('workflows', workflows), ('reads', reads), ('concurrency', concurrency), ('workers', workers)):
        if type(value) is not int or value < (0 if name == 'reads' else 1):
            raise ValueError(name + (' must be a nonnegative integer' if name == 'reads' else ' must be a positive integer'))
    if concurrency > 128 or workers > 64:
        raise ValueError('concurrency must be <=128 and workers <=64')
    if isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError('deadline must be finite positive seconds')


def _latency_summary(latencies):
    """Nearest-rank percentiles of seconds, reported in milliseconds."""
    ordered = sorted(latencies)
    if not ordered:
        raise ValueError('at least one latency sample is required')
    if any(not math.isfinite(value) or value < 0 for value in ordered):
        raise ValueError('latencies must be finite nonnegative seconds')
    def percentile(fraction):
        return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] * 1000, 3)
    return dict(p50=percentile(.5), p95=percentile(.95), p99=percentile(.99), maximum=percentile(1))


def _verify_tasks(rows, workflows):
    """Check every node, including cardinality, without vacuous all([]) success."""
    expected = {('benchmark-' + str(index), node): index
                for index in range(workflows) for node in ('root', 'join')}
    observed, invalid_results, invalid_attempts = [], 0, 0
    for row in rows:
        key = (row['workflow_id'], row['node_id'])
        observed.append(key)
        try:
            result = json.loads(row['result'])
        except (TypeError, ValueError):
            result = None
        if (key not in expected or row['status'] != 'SUCCESS' or
                type(result) is not int or result != expected[key]):
            invalid_results += 1
        if row['attempt_count'] != 1 or row['retry_count'] != 0:
            invalid_attempts += 1
    node_set_matches = len(observed) == len(expected) and set(observed) == set(expected)
    return dict(expected_tasks=len(expected), observed_tasks=len(observed),
                node_set_matches=node_set_matches, invalid_results=invalid_results,
                invalid_attempts=invalid_attempts,
                passed=node_set_matches and invalid_results == 0 and invalid_attempts == 0)


def run_benchmark(*, workflows=100, reads=300, concurrency=8, workers=4, deadline=60):
    _validate_configuration(workflows, reads, concurrency, workers, deadline)
    token = 'temporary-local-benchmark-token'
    # The caller's directory is writable in workspace sandboxes where OS temp
    # may not be usable by SQLite. Every run owns and removes a distinct DB.
    with tempfile.TemporaryDirectory(prefix='.management-benchmark-', dir=Path.cwd()) as directory:
        engine = TaskEngine(database_path=Path(directory) / 'engine.db',
                            task_registry={'abs': abs, 'identity': _identity}, retry_policy=RetryPolicy(0))
        service = engine.create_management_service(token, max_concurrency=min(256, concurrency * 2)).start()
        consumers = []
        latencies, errors, lifecycle_errors = [], [], []
        started = time.perf_counter()

        def request(index):
            before = time.perf_counter()
            if index < workflows:
                body = dict(id='benchmark-' + str(index), nodes=[
                    dict(id='root', handler='abs', args=[-index], idempotent=True),
                    dict(id='join', handler='identity', inputs={'value': {'node_id': 'root'}}, idempotent=True)])
                url, expected = '/api/v1/workflows', 202
                raw = json.dumps(body).encode('utf-8')
            else:
                url = ('/api/v1/overview', '/api/v1/workflows?limit=10', '/metrics')[index % 3]
                raw, expected = None, 200
            req = Request(service.url + url, data=raw,
                          headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
            try:
                with build_opener(ProxyHandler({})).open(req, timeout=min(deadline, 10)) as response:
                    response.read()
                    error = None if response.code == expected else 'http_' + str(response.code)
            except HTTPError as exception:
                exception.close()
                error = 'http_' + str(exception.code)
            except Exception as exception:
                error = type(exception).__name__
            return time.perf_counter() - before, error

        try:
            for slot in range(workers):
                worker = engine.create_worker('benchmark-worker-' + str(slot))
                consumers.append(worker)
                worker.start()
            # Interleave reads with writes so the read phase exercises live execution.
            jobs = []
            for index in range(max(workflows, reads)):
                if index < workflows:
                    jobs.append(index)
                if index < reads:
                    jobs.append(workflows + index)
            with ThreadPoolExecutor(concurrency) as pool:
                for elapsed, error in pool.map(request, jobs):
                    latencies.append(elapsed)
                    if error:
                        errors.append(error)
            http_elapsed = time.perf_counter() - started
            completed = False
            while time.perf_counter() - started < deadline:
                snapshot = service.queries.overview()
                if snapshot['workflows']['SUCCESS'] == workflows:
                    completed = True
                    break
                time.sleep(.02)
            total_elapsed = time.perf_counter() - started
            for worker in consumers:
                if not worker.shutdown(5):
                    lifecycle_errors.append('worker_drain_timeout')
                if worker.last_error is not None:
                    lifecycle_errors.append('worker_' + type(worker.last_error).__name__)
            if not service.shutdown(5):
                lifecycle_errors.append('service_drain_timeout')
            snapshot = service.queries.overview()
            with engine._store._connection() as db:
                integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
                verification = _verify_tasks(db.execute('''SELECT n.workflow_id,n.node_id,
                    t.result,t.status,t.attempt_count,t.retry_count FROM workflow_nodes n
                    JOIN tasks t ON t.id=n.task_id'''), workflows)
            checks = dict(http_requests=not errors, completion_before_deadline=completed,
                          workflow_count=snapshot['workflows']['SUCCESS'] == workflows,
                          task_count=snapshot['tasks']['SUCCESS'] == workflows * 2,
                          single_attempts=snapshot['attempts'] == workflows * 2 and snapshot['retries'] == 0,
                          task_results=verification['passed'], database_integrity=integrity == 'ok',
                          clean_shutdown=not lifecycle_errors)
            return dict(timestamp=datetime.now(timezone.utc).isoformat(), python=platform.python_version(),
                        platform=platform.platform(), sqlite=sqlite3.sqlite_version, schema_version=2,
                        workload='management-http-two-node-dag', passed=all(checks.values()), checks=checks,
                        failure_reasons=[name for name, passed in checks.items() if not passed],
                        lifecycle_errors=lifecycle_errors, verification=verification,
                        configuration=dict(workflows=workflows, reads=reads, concurrency=concurrency, workers=workers,
                                           deadline=deadline, coordinator_poll_seconds=service.poll_interval,
                                           worker_poll_seconds=.05, worker_heartbeat_seconds=1),
                        http=dict(requests=len(latencies), errors=len(errors), error_types=sorted(set(errors)),
                                  elapsed_seconds=round(http_elapsed, 3), requests_per_second=round(len(latencies) / http_elapsed, 2),
                                  latency_ms=_latency_summary(latencies)),
                        execution=dict(completed_workflows=snapshot['workflows']['SUCCESS'], attempts=snapshot['attempts'],
                                       retries=snapshot['retries'], total_elapsed_seconds=round(total_elapsed, 3),
                                       workflows_per_second=round(snapshot['workflows']['SUCCESS'] / total_elapsed, 2)),
                        integrity=integrity)
        finally:
            for worker in consumers:
                worker.shutdown(5)
            service.shutdown(5)


def _validate_rounds(repeat, warmup):
    for name, value, minimum in (('repeat', repeat, 1), ('warmup', warmup, 0)):
        if type(value) is not int or value < minimum:
            raise ValueError(name + ' must be an integer >= ' + str(minimum))


def _round_statistics(values):
    return dict(minimum=round(min(values), 3), median=round(statistics.median(values), 3),
                maximum=round(max(values), 3), mean=round(statistics.mean(values), 3),
                sample_stdev=round(statistics.stdev(values), 3) if len(values) > 1 else 0.0)


def summarize_runs(runs):
    """Summarize measured rounds; never pool or average percentiles as a percentile."""
    if not runs:
        raise ValueError('at least one measured run is required')
    requests = sum(run['http']['requests'] for run in runs)
    workflows = sum(run['execution']['completed_workflows'] for run in runs)
    http_seconds = sum(run['http']['elapsed_seconds'] for run in runs)
    execution_seconds = sum(run['execution']['total_elapsed_seconds'] for run in runs)
    metrics = {
        'http_requests_per_second': [run['http']['requests_per_second'] for run in runs],
        'workflows_per_second': [run['execution']['workflows_per_second'] for run in runs],
        'execution_seconds': [run['execution']['total_elapsed_seconds'] for run in runs],
    }
    for percentile in ('p50', 'p95', 'p99', 'maximum'):
        metrics['http_' + percentile + '_latency_ms'] = [run['http']['latency_ms'][percentile] for run in runs]
    return dict(measured_runs=len(runs), passed_runs=sum(run['passed'] for run in runs),
                aggregate=dict(requests=requests, errors=sum(run['http']['errors'] for run in runs),
                               completed_workflows=workflows,
                               attempts=sum(run['execution']['attempts'] for run in runs),
                               retries=sum(run['execution']['retries'] for run in runs),
                               http_elapsed_seconds=round(http_seconds, 3),
                               execution_elapsed_seconds=round(execution_seconds, 3),
                               requests_per_second=round(requests / http_seconds, 2) if http_seconds else 0.0,
                               workflows_per_second=round(workflows / execution_seconds, 2) if execution_seconds else 0.0),
                round_statistics={name: _round_statistics(values) for name, values in metrics.items()})


def run_suite(*, repeat=3, warmup=1, **configuration):
    """Sequential isolated rounds. Warmups are retained but excluded from metrics."""
    _validate_rounds(repeat, warmup)
    # Validate before allocating even the first warmup's resources.
    _validate_configuration(**{**dict(workflows=100, reads=300, concurrency=8, workers=4, deadline=60),
                               **configuration})
    warmup_runs = [run_benchmark(**configuration) for _ in range(warmup)]
    runs = [run_benchmark(**configuration) for _ in range(repeat)]
    return dict(schema_version=2, workload=runs[0]['workload'], timestamp=runs[-1]['timestamp'],
                python=runs[0]['python'], platform=runs[0]['platform'], sqlite=runs[0]['sqlite'],
                passed=all(run['passed'] for run in warmup_runs + runs),
                configuration={**runs[0]['configuration'], 'repeat': repeat, 'warmup': warmup},
                warmup_runs=warmup_runs, runs=runs, summary=summarize_runs(runs))


def render_markdown(result):
    runs = result.get('runs', [result])
    warmups = result.get('warmup_runs', [])
    summary = result.get('summary') or summarize_runs(runs)
    status = 'PASS' if result['passed'] else 'FAIL'
    lines = ['# Management Benchmark report', '', f"Result: **{status}**; UTC timestamp: {result['timestamp']}.", '',
             f"Environment: Python {result['python']}; SQLite {result['sqlite']}; {result['platform']}.", '',
             '## Configuration', '', '```json', json.dumps(result['configuration'], indent=2), '```', '',
             'Each round uses a new temporary SQLite database, a loopback HTTP service and dedicated workers.',
             'The workload submits root(abs) → join(identity) DAGs and interleaves overview, list and metrics reads.',
             'Measured time includes worker startup, HTTP load and completion polling; service/DB setup and shutdown are excluded.',
             'The deadline is the completion polling cutoff, not a hard timeout for the HTTP batch.', '',
             '## Individual rounds', '',
             '| Round | Result | Requests | HTTP errors | HTTP req/s | P95 ms | P99 ms | Completed workflows | Attempts | Results | DB |',
             '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |']
    for phase, items in (('Warmup', warmups), ('Measured', runs)):
        for index, run in enumerate(items, 1):
            http, execution = run['http'], run['execution']
            lines.append(f"| {phase} {index} | {'PASS' if run['passed'] else 'FAIL'} | {http['requests']} | "
                         f"{http['errors']} | {http['requests_per_second']} | {http['latency_ms']['p95']} | "
                         f"{http['latency_ms']['p99']} | {execution['completed_workflows']} | {execution['attempts']} | "
                         f"{'PASS' if run['verification']['passed'] else 'FAIL'} | {run['integrity']} |")
    lines.extend(['', '## Statistics across measured rounds', '',
                  'Warmups are excluded. Failed measured rounds remain in the statistics and make the report FAIL.',
                  'Latency statistics below describe per-round percentiles; they are not pooled request percentiles.', '',
                  '| Metric | Min | Median | Max | Mean | Sample standard deviation |',
                  '| --- | ---: | ---: | ---: | ---: | ---: |'])
    for name, values in summary['round_statistics'].items():
        lines.append('| ' + name + ' | ' + ' | '.join(str(values[key]) for key in
                     ('minimum', 'median', 'maximum', 'mean', 'sample_stdev')) + ' |')
    aggregate = summary['aggregate']
    lines.extend(['', f"Passed measured rounds: {summary['passed_runs']}/{summary['measured_runs']}. "
                  f"Aggregate HTTP throughput: {aggregate['requests_per_second']} req/s; "
                  f"workflow throughput: {aggregate['workflows_per_second']} workflows/s.", '',
                  '## Correctness checks', '',
                  'Every round checks expected HTTP statuses, completion before the cutoff, exact workflow/task counts,',
                  'both node results, exactly one attempt per task, zero retries, SQLite integrity and clean shutdown.'])
    for phase, items in (('Warmup', warmups), ('Measured', runs)):
        for index, run in enumerate(items, 1):
            if not run['passed']:
                reasons = ', '.join(run['failure_reasons'])
                lines.extend(['', f"- {phase} {index}: {reasons}; HTTP errors: {run['http']['error_types']}; "
                              f"lifecycle errors: {run['lifecycle_errors']}."])
    lines.extend(['', 'This is a local capacity sample, not a production SLO. It does not benchmark model inference,',
                  'cross-host networking, long-running soak, or fault recovery. Timings vary with machine load and disk.', ''])
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflows', type=int, default=100)
    parser.add_argument('--reads', type=int, default=300)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--deadline', type=float, default=60)
    parser.add_argument('--repeat', type=int, default=1, help='number of measured rounds (default: 1)')
    parser.add_argument('--warmup', type=int, default=0, help='isolated warmup rounds excluded from statistics')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--markdown', type=Path, help='also write a human-readable report')
    options = parser.parse_args(argv)
    try:
        _validate_rounds(options.repeat, options.warmup)
        if options.output and options.markdown and options.output.resolve() == options.markdown.resolve():
            raise ValueError('--output and --markdown must be different files')
        configuration = dict(workflows=options.workflows, reads=options.reads, concurrency=options.concurrency,
                             workers=options.workers, deadline=options.deadline)
        if options.repeat == 1 and options.warmup == 0:
            result = run_benchmark(**configuration)
        else:
            result = run_suite(repeat=options.repeat, warmup=options.warmup, **configuration)
    except ValueError as error:
        parser.error(str(error))
    raw = json.dumps(result, ensure_ascii=False, indent=2)
    if options.output:
        options.output.parent.mkdir(parents=True, exist_ok=True)
        options.output.write_text(raw + '\n', encoding='utf-8')
    if options.markdown:
        options.markdown.parent.mkdir(parents=True, exist_ok=True)
        options.markdown.write_text(render_markdown(result), encoding='utf-8')
    print(raw)
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
