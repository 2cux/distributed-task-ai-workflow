"""Continuous, isolated AI workflow benchmark and fail-closed regression gate.

Only a loopback deterministic provider is used. No environment credentials are read.
Run from the repository root with PYTHONPATH=src. See docs/ai-workflow-benchmarks.md.
"""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import sqlite3
import statistics
import sys
import tempfile
from threading import Lock, Thread
import time
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from workflow_engine import LLMConfig, RetryPolicy, TaskEngine, create_llm_registry

WORKLOAD = 'ai-http-generate-validate-v1'
DEFAULT = dict(concurrencies=[1, 4, 8], workers=4, duration=180, repeat=2,
               warmup=10, window=30, drain_timeout=60, request_timeout=10,
               provider_delay_ms=25)
LIMITS = dict(http_failure_rate=0, workflow_failure_rate=0, http_p95_ms=2000,
              workflow_p95_ms=10000, writer_wait_p95_ms=1000, sqlite_begin_p95_ms=500,
              busy_errors=0, min_workflows_per_second=.1,
              min_throughput_ratio=.75, max_latency_ratio=1.3)


def validate(config, limits):
    if set(config) != set(DEFAULT) or set(limits) != set(LIMITS):
        raise ValueError('configuration/limits must contain exactly the documented keys')
    levels = config['concurrencies']
    if (not isinstance(levels, list) or not levels or len(levels) != len(set(levels)) or
            any(type(x) is not int or not 1 <= x <= 128 for x in levels)):
        raise ValueError('concurrencies must be distinct integers in 1..128')
    for name in ('workers', 'repeat'):
        if type(config[name]) is not int or not 1 <= config[name] <= (64 if name == 'workers' else 100):
            raise ValueError(name + ' is outside the supported integer range')
    for name in set(config) - {'concurrencies', 'workers', 'repeat'}:
        value = config[name]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (
                value == 0 and name not in ('warmup', 'provider_delay_ms')):
            raise ValueError(name + ' must be finite and positive (warmup/delay may be zero)')
    for name, value in limits.items():
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(name + ' must be finite and nonnegative')
    for name in ('http_failure_rate', 'workflow_failure_rate', 'min_throughput_ratio'):
        if limits[name] > 1:
            raise ValueError(name + ' must be <= 1')
    if limits['max_latency_ratio'] < 1:
        raise ValueError('max_latency_ratio must be >= 1')


class Samples:
    """Fixed-memory, 1%-resolution logarithmic histograms (upper-bound quantiles)."""
    def __init__(self):
        self.lock = Lock()
        self.count = self.errors = 0
        self.total = self.maximum = 0.0
        self.bins, self.error_types = Counter(), Counter()

    def add(self, seconds, error=None):
        with self.lock:
            self.count += 1
            self.total += seconds
            self.maximum = max(self.maximum, seconds)
            # Zero bucket <= 1 microsecond; overflow bucket >= 1 day.
            bucket = min(2544, max(0, math.ceil(math.log(max(seconds, 1e-6) / 1e-6, 1.01))))
            self.bins[bucket] += 1
            if error:
                self.errors += 1
                self.error_types[str(error)] += 1

    def report(self):
        with self.lock:
            def percentile(fraction):
                target, seen = math.ceil(self.count * fraction), 0
                for bucket, count in sorted(self.bins.items()):
                    seen += count
                    if seen >= target:
                        return round(min(self.maximum, 1e-6 * 1.01 ** bucket) * 1000, 3)
                return 0.0
            return dict(count=self.count, errors=self.errors,
                        failure_rate=self.errors / self.count if self.count else 0,
                        error_types=dict(self.error_types), total_seconds=round(self.total, 6),
                        latency_ms=dict(p50=percentile(.5), p95=percentile(.95), p99=percentile(.99),
                                        maximum=round(self.maximum * 1000, 3)))


def is_busy(error):
    code = getattr(error, 'sqlite_errorcode', 0)
    return isinstance(error, sqlite3.OperationalError) and (
        code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or
        'locked' in str(error).lower() or 'busy' in str(error).lower())


class DatabaseProbe:
    """Instrument only this benchmark's instances; retain real SQLite behavior."""
    def __init__(self, store):
        self.writer_wait, self.begin, self.sql = Samples(), Samples(), Samples()
        self.windows, self.lock = defaultdict(lambda: defaultdict(Samples)), Lock()
        self.origin, self.window_seconds = None, 30
        self.originals = []
        self._wrap_slot(store)
        for owner in (store, store.workers):
            self._wrap_connection(owner)

    def record(self, name, elapsed, error=None):
        getattr(self, name).add(elapsed, error)
        if self.origin is not None:
            with self.lock:
                sample = self.windows[int((time.perf_counter() - self.origin) // self.window_seconds)][name]
            sample.add(elapsed, error)

    def _wrap_slot(self, store):
        original = store._writer_slot
        self.originals.append((store, '_writer_slot', original))
        @contextmanager
        def slot():
            before = time.perf_counter()
            entered = False
            try:
                with original():
                    entered = True
                    self.record('writer_wait', time.perf_counter() - before)
                    yield
            except BaseException as error:
                if not entered:
                    self.record('writer_wait', time.perf_counter() - before,
                                'busy_locked' if is_busy(error) else type(error).__name__)
                raise
        store._writer_slot = slot

    def _wrap_connection(self, owner):
        original, probe = owner._connection, self
        self.originals.append((owner, '_connection', original))
        class Connection:
            def __init__(self, db):
                self.db = db

            def __getattr__(self, name):
                return getattr(self.db, name)

            def execute(self, sql, *args, **kwargs):
                before, error_type = time.perf_counter(), None
                try:
                    return self.db.execute(sql, *args, **kwargs)
                except BaseException as error:
                    error_type = 'busy_locked' if is_busy(error) else type(error).__name__
                    raise
                finally:
                    elapsed = time.perf_counter() - before
                    probe.record('sql', elapsed, error_type)
                    if sql.strip().upper().startswith('BEGIN IMMEDIATE'):
                        probe.record('begin', elapsed, error_type)
        @contextmanager
        def connection():
            with original() as db:
                yield Connection(db)
        owner._connection = connection

    def report(self):
        sql = self.sql.report()
        writer = self.writer_wait.report()
        return dict(writer_wait=writer, sqlite_begin=self.begin.report(),
                    sql=sql, busy_errors=sql['error_types'].get('busy_locked', 0) +
                    writer['error_types'].get('busy_locked', 0),
                    windows=[dict(start_seconds=index * self.window_seconds,
                                  **{name: sample.report() for name, sample in items.items()})
                             for index, items in sorted(self.windows.items())])

    def close(self):
        for owner, name, original in reversed(self.originals):
            setattr(owner, name, original)


@contextmanager
def provider(delay_ms):
    calls, mutex = Counter(), Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            self.connection.settimeout(10)
            value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            prompt = value['messages'][-1]['content']
            with mutex:
                calls[prompt] += 1
            time.sleep(delay_ms / 1000)
            raw = json.dumps(dict(id='mock-' + prompt, model='benchmark-model',
                choices=[dict(message=dict(role='assistant', content=prompt), finish_reason='stop')],
                usage=dict(prompt_tokens=11, completion_tokens=7, total_tokens=18))).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    class Server(ThreadingHTTPServer):
        request_queue_size = 256
        daemon_threads = False
    server = Server(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/v1', calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def workflow(identifier):
    return dict(id=identifier, llm_budget=dict(max_calls=1, max_total_tokens=18), nodes=[
        dict(id='generate', handler='llm.chat.v1', kwargs=dict(prompt=identifier)),
        dict(id='validate', handler='llm.validate.v1', kwargs=dict(expected_text=identifier),
             inputs=dict(result=dict(node_id='generate')))])


def verify(store, submitted, calls):
    """Check submitted identities, exact nodes, persisted outputs, attempts and events."""
    invalid = []
    with store._connection() as db:
        integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
        foreign_keys = list(db.execute('PRAGMA foreign_key_check'))
        saved = {row['id']: row['status'] for row in db.execute('SELECT id,status FROM workflow_runs')}
        rows = list(db.execute('''SELECT n.workflow_id,n.node_id,t.* FROM workflow_nodes n
            JOIN tasks t ON n.task_id=t.id'''))
        events = {row['task_id']: row['n'] for row in db.execute(
            "SELECT task_id,count(*) AS n FROM task_events WHERE event_type='SUCCEEDED' GROUP BY task_id")}
    observed = Counter()
    for row in rows:
        observed[(row['workflow_id'], row['node_id'])] += 1
        try:
            result = json.loads(row['result'])
            correct = (result['text'] == row['workflow_id'] and result['usage']['total_tokens'] == 18)
        except (ValueError, TypeError, KeyError):
            correct = False
        if not (correct and row['status'] == 'SUCCESS' and row['attempt_count'] == 1 and
                row['retry_count'] == 0 and events.get(row['id']) == 1):
            invalid.append(row['id'])
    expected = Counter({(identifier, node): 1 for identifier in submitted for node in ('generate', 'validate')})
    checks = dict(nonempty=bool(submitted), workflow_ids=set(saved) == set(submitted),
                  workflow_success=all(status == 'SUCCESS' for status in saved.values()),
                  node_set=observed == expected, outputs_attempts_events=not invalid,
                  provider_exactly_once=dict(calls) == {identifier: 1 for identifier in submitted},
                  integrity=integrity == 'ok', foreign_keys=not foreign_keys)
    return dict(passed=all(checks.values()), checks=checks, invalid_tasks=invalid[:20],
                workflows=len(saved), tasks=len(rows), provider_calls=sum(calls.values()), integrity=integrity)


def run_round(config, concurrency, duration=None):
    duration = config['duration'] if duration is None else duration
    http, flows, endpoints = Samples(), Samples(), defaultdict(Samples)
    windows, submitted, failures, lock = defaultdict(lambda: [Samples(), Samples()]), set(), [], Lock()
    consumers, lifecycle = [], []
    with tempfile.TemporaryDirectory(prefix='.ai-benchmark-', dir=Path.cwd()) as directory:
        with provider(config['provider_delay_ms']) as (base_url, calls):
            engine = TaskEngine(database_path=Path(directory) / 'engine.db', retry_policy=RetryPolicy(0),
                task_registry=create_llm_registry(LLMConfig('local-benchmark-only', 'benchmark-model',
                                                          base_url, timeout=config['request_timeout'])))
            probe = DatabaseProbe(engine._store)
            service = engine.create_management_service('benchmark-token', max_concurrency=min(256, concurrency * 2),
                                                       request_timeout=config['request_timeout']).start()
            try:
                for index in range(config['workers']):
                    worker = engine.create_worker('benchmark-' + str(index))
                    consumers.append(worker)
                    worker.start()
                started = time.perf_counter()
                probe.origin, probe.window_seconds = started, config['window']
                admission_end = started + duration
                drain_end = admission_end + config['drain_timeout']

                def request(opener, path, body=None):
                    before, error, value = time.perf_counter(), None, None
                    req = Request(service.url + path, data=json.dumps(body).encode() if body else None,
                        headers={'Authorization': 'Bearer benchmark-token', 'Content-Type': 'application/json'})
                    try:
                        with opener.open(req, timeout=config['request_timeout']) as response:
                            raw = response.read()
                            value = json.loads(raw)
                            if response.code != (202 if body else 200):
                                error = 'http_' + str(response.code)
                    except HTTPError as exception:
                        error = 'http_' + str(exception.code)
                        exception.close()
                    except Exception as exception:
                        error = type(exception).__name__
                    elapsed = time.perf_counter() - before
                    http.add(elapsed, error)
                    endpoints['submit' if body else 'read'].add(elapsed, error)
                    with lock:
                        window = windows[int((before - started) // config['window'])]
                    window[0].add(elapsed, error)
                    return value, error

                def client(slot):
                    opener, index = build_opener(ProxyHandler({})), 0
                    while time.perf_counter() < admission_end:
                        identifier = f'ai-{slot}-{index}'
                        index += 1
                        before = time.perf_counter()
                        # Include ambiguous submission failures in expected persisted identities.
                        with lock:
                            submitted.add(identifier)
                        _, error = request(opener, '/api/v1/workflows', workflow(identifier))
                        terminal = False
                        while not error and time.perf_counter() < drain_end:
                            value, error = request(opener, '/api/v1/workflows/' + identifier)
                            if value and value.get('status') in ('SUCCESS', 'FAILED', 'PAUSED'):
                                terminal = True
                                error = None if value['status'] == 'SUCCESS' else value['status']
                                usage = value.get('llm_usage', {})
                                if not error and (usage.get('call_count') != 1 or usage.get('total_tokens') != 18 or
                                                  usage.get('successful_calls') != 1):
                                    error = 'usage_mismatch'
                                break
                            if not error:
                                time.sleep(.05)
                        error = error or (None if terminal else 'drain_deadline')
                        elapsed = time.perf_counter() - before
                        flows.add(elapsed, error)
                        with lock:
                            window = windows[int((before - started) // config['window'])]
                            if error and len(failures) < 20:
                                failures.append(dict(id=identifier, error=error))
                        window[1].add(elapsed, error)
                        if error:
                            time.sleep(.05)

                with ThreadPoolExecutor(concurrency) as pool:
                    pending = {pool.submit(client, slot) for slot in range(concurrency)}
                    while pending:
                        completed, pending = wait(pending, timeout=30)
                        for future in completed:
                            future.result()
                        if pending:
                            print(f"active c={concurrency} seconds={time.perf_counter() - started:.1f} "
                                  f"workflows={flows.report()['count']} http_errors={http.report()['errors']} "
                                  f"writer_p95_ms={probe.writer_wait.report()['latency_ms']['p95']}",
                                  file=sys.stderr, flush=True)
                elapsed = time.perf_counter() - started
            finally:
                for worker in consumers:
                    try:
                        if not worker.shutdown(5):
                            lifecycle.append('worker_shutdown_timeout')
                        if worker.last_error:
                            lifecycle.append('worker_' + type(worker.last_error).__name__)
                    except Exception as error:
                        lifecycle.append('worker_shutdown_' + type(error).__name__)
                if not service.shutdown(5):
                    lifecycle.append('service_shutdown_timeout')
                if service.metrics.coordinator_errors:
                    lifecycle.append('coordinator_errors_' + str(service.metrics.coordinator_errors))
                database = probe.report()
                probe.close()
            verification = verify(engine._store, submitted, calls)
            flow_report = flows.report()
            flow_report['workflows_per_second'] = round((flow_report['count'] - flow_report['errors']) / elapsed, 6)
            http_report = http.report()
            http_report['requests_per_second'] = round(http_report['count'] / elapsed, 6)
            return dict(concurrency=concurrency, elapsed_seconds=round(elapsed, 6),
                admission_seconds=duration, http=http_report, workflows=flow_report,
                endpoints={key: value.report() for key, value in endpoints.items()}, database=database,
                verification=verification, lifecycle_errors=lifecycle, failures=failures,
                windows=[dict(start_seconds=index * config['window'], http=items[0].report(),
                              workflows=items[1].report()) for index, items in sorted(windows.items())])


def metrics(run):
    return dict(http_failure_rate=run['http']['failure_rate'],
                workflow_failure_rate=run['workflows']['failure_rate'],
                http_p95_ms=run['http']['latency_ms']['p95'],
                workflow_p95_ms=run['workflows']['latency_ms']['p95'],
                writer_wait_p95_ms=run['database']['writer_wait']['latency_ms']['p95'],
                sqlite_begin_p95_ms=run['database']['sqlite_begin']['latency_ms']['p95'],
                busy_errors=run['database']['busy_errors'],
                min_workflows_per_second=run['workflows']['workflows_per_second'])


def gate(run, limits):
    reasons = []
    if not run['verification']['passed']:
        reasons.extend('verification.' + name for name, passed in run['verification']['checks'].items() if not passed)
    if run['lifecycle_errors']:
        reasons.append('lifecycle_errors')
    for name, value in metrics(run).items():
        if (value < limits[name] if name.startswith('min_') else value > limits[name]):
            reasons.append(name)
    return reasons


def compare(report, baseline):
    reasons = []
    if (baseline.get('schema_version') != report['schema_version'] or
            baseline.get('workload') != report['workload'] or baseline.get('configuration') != report['configuration'] or
            baseline.get('environment') != report['environment'] or not baseline.get('passed') or
            baseline.get('limits') != report['limits'] or baseline.get('complete') is not True):
        return ['baseline_incompatible_or_failed']
    all_runs = baseline.get('runs', [])
    if (len(all_runs) != report['configuration']['repeat'] * len(report['configuration']['concurrencies']) or
            any(not run.get('passed') or gate(run, report['limits']) for run in all_runs)):
        return ['baseline_invalid_rounds']
    for concurrency in report['configuration']['concurrencies']:
        current = [metrics(run) for run in report['runs'] if run['concurrency'] == concurrency]
        previous = [metrics(run) for run in baseline['runs'] if run['concurrency'] == concurrency]
        if len(previous) != len(current) or not current:
            reasons.append(f'baseline_missing_rounds.c{concurrency}')
            continue
        for name in ('min_workflows_per_second', 'http_p95_ms', 'workflow_p95_ms',
                     'writer_wait_p95_ms', 'sqlite_begin_p95_ms'):
            value, before = statistics.median(x[name] for x in current), statistics.median(x[name] for x in previous)
            # 1 ms noise floor avoids ratios of almost-zero lock acquisition timings.
            failed = (value < before * report['limits']['min_throughput_ratio'] if name.startswith('min_') else
                      value > max(before, 1) * report['limits']['max_latency_ratio'])
            if failed:
                reasons.append(f'regression.c{concurrency}.{name}')
    return reasons


def run_suite(config, limits, baseline=None, progress=None):
    validate(config, limits)
    report = dict(schema_version=1, workload=WORKLOAD, timestamp=datetime.now(timezone.utc).isoformat(),
        configuration=config, limits=limits, environment=dict(python=platform.python_version(),
        sqlite=sqlite3.sqlite_version, platform=platform.platform(), machine=platform.node()),
        warmup_runs=[], runs=[], failure_reasons=[])
    for round_index in range(config['repeat']):
        # Rotate ordering to reduce systematic first/last-level bias.
        levels = config['concurrencies'][round_index % len(config['concurrencies']):] + config['concurrencies'][:round_index % len(config['concurrencies'])]
        for concurrency in levels:
            for warmup in ([True, False] if config['warmup'] else [False]):
                if progress:
                    progress(report, f"round={round_index + 1} concurrency={concurrency} {'warmup' if warmup else 'measured'} started")
                run = run_round(config, concurrency, config['warmup'] if warmup else config['duration'])
                run['round'] = round_index + 1
                run['failure_reasons'] = gate(run, limits)
                run['passed'] = not run['failure_reasons']
                report['warmup_runs' if warmup else 'runs'].append(run)
                if not run['passed']:
                    report['failure_reasons'].append(f"{'warmup' if warmup else 'measured'}.r{round_index + 1}.c{concurrency}:" + ','.join(run['failure_reasons']))
                if progress:
                    progress(report, f"completed: {'PASS' if run['passed'] else 'FAIL'}; {metrics(run)}")
    report['passed'] = not report['failure_reasons']
    if baseline is not None:
        report['failure_reasons'].extend(compare(report, baseline))
        report['passed'] = not report['failure_reasons']
        report['baseline_sha256'] = hashlib.sha256(json.dumps(baseline, sort_keys=True).encode()).hexdigest()
    return report


def render_markdown(report):
    lines = ['# AI workflow load regression', '', f"Result: **{'PASS' if report['passed'] else 'FAIL'}**", '',
        f"UTC: {report['timestamp']}; environment: `{json.dumps(report['environment'])}`", '',
        'Closed-loop clients: one workflow in flight per client; duration controls admission, then bounded drain.',
        'Latency quantiles are upper bounds from fixed-memory 1% logarithmic histograms; failures are included.', '',
        '| Phase | Round | Clients | Workflows | WF/s | HTTP failure % | WF failure % | HTTP P95/P99 ms | WF P95/P99 ms | Writer wait P95 ms | SQLite BEGIN P95 ms | Busy | Result |',
        '| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |']
    for phase, key in [('Warmup', 'warmup_runs'), ('Measured', 'runs')]:
        for run in report[key]:
            h, w, d = run['http'], run['workflows'], run['database']
            lines.append(f"| {phase} | {run['round']} | {run['concurrency']} | {w['count']} | {w['workflows_per_second']} | "
                f"{h['failure_rate'] * 100:.3f} | {w['failure_rate'] * 100:.3f} | {h['latency_ms']['p95']}/{h['latency_ms']['p99']} | "
                f"{w['latency_ms']['p95']}/{w['latency_ms']['p99']} | {d['writer_wait']['latency_ms']['p95']} | "
                f"{d['sqlite_begin']['latency_ms']['p95']} | {d['busy_errors']} | {'PASS' if run['passed'] else 'FAIL'} |")
    lines.extend(['', 'Configuration:', '', '```json', json.dumps(report['configuration'], indent=2), '```', '',
                  'Gate limits:', '', '```json', json.dumps(report['limits'], indent=2), '```', '',
                  'Failure reasons: ' + (', '.join(report['failure_reasons']) or 'none'), '',
                  'Per-window evidence, endpoint distributions, exact persisted node/attempt/event checks and provider call counts are in JSON.',
                  'SQLite BEGIN time includes lock acquisition and statement overhead; writer wait includes the local FIFO queue.',
                  'A zero busy-error count does not imply zero contention. Setup PRAGMAs, cursor fetching and other processes are not instrumented.',
                  'This measures the local AI orchestration path with a deterministic HTTP provider; it does not measure production model inference.', ''])
    return '\n'.join(lines)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')
    temporary.replace(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--markdown', type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        paths = [options.output.resolve(), options.markdown.resolve(), options.profile.resolve(),
                 options.output.with_suffix('.partial.json').resolve(),
                 options.output.with_suffix(options.output.suffix + '.tmp').resolve(),
                 options.output.with_suffix('.partial.json.tmp').resolve()]
        if options.baseline:
            paths.append(options.baseline.resolve())
        if len(paths) != len(set(paths)):
            raise ValueError('profile, baseline, JSON and Markdown must be distinct files')
        profile = json.loads(options.profile.read_text(encoding='utf-8'))
        if set(profile) != {'configuration', 'limits'}:
            raise ValueError('profile requires configuration and limits')
        baseline = json.loads(options.baseline.read_text(encoding='utf-8')) if options.baseline else None
        validate(profile['configuration'], profile['limits'])
    except (OSError, ValueError, TypeError) as error:
        parser.error(str(error))
    def progress(report, message):
        write_json(options.output.with_suffix('.partial.json'), {**report, 'complete': False})
        print(datetime.now(timezone.utc).isoformat() + ' ' + message, file=sys.stderr, flush=True)
    report = run_suite(profile['configuration'], profile['limits'], baseline, progress)
    report['complete'] = True
    write_json(options.output, report)
    options.markdown.parent.mkdir(parents=True, exist_ok=True)
    options.markdown.write_text(render_markdown(report), encoding='utf-8')
    print(json.dumps(dict(passed=report['passed'], output=str(options.output),
                          failure_reasons=report['failure_reasons'])), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
