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
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from workflow_engine import RetryPolicy, TaskEngine


def _identity(value):
    return value


def run_benchmark(*, workflows=100, reads=300, concurrency=8, workers=4, deadline=60):
    for name, value in (('workflows', workflows), ('reads', reads), ('concurrency', concurrency), ('workers', workers)):
        if type(value) is not int or value < (0 if name == 'reads' else 1):
            raise ValueError(name + ' must be a valid nonnegative count')
    if concurrency > 128 or workers > 64:
        raise ValueError('concurrency must be <=128 and workers <=64')
    if isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError('deadline must be finite positive seconds')
    token = 'temporary-local-benchmark-token'
    with tempfile.TemporaryDirectory(prefix='workflow-api-benchmark-') as directory:
        engine = TaskEngine(database_path=Path(directory) / 'engine.db',
                            task_registry={'abs': abs, 'identity': _identity}, retry_policy=RetryPolicy(0))
        service = engine.create_management_service(token, max_concurrency=min(256, concurrency * 2)).start()
        consumers = []
        latencies, errors = [], []
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
                    errors.append('worker_drain_timeout')
            snapshot = service.queries.overview()
            with engine._store._connection() as db:
                integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
                results_ok = all(json.loads(row['result']) == int(row['workflow_id'].split('-')[1])
                    for row in db.execute('''SELECT n.workflow_id,t.result FROM workflow_nodes n
                        JOIN tasks t ON t.id=n.task_id WHERE n.node_id='join' AND t.status='SUCCESS' '''))
            passed = (not errors and completed and integrity == 'ok' and results_ok and
                      snapshot['attempts'] == workflows * 2 and snapshot['tasks']['SUCCESS'] == workflows * 2)
            ordered = sorted(latencies)
            percentile = lambda fraction: round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] * 1000, 3)
            return dict(timestamp=datetime.now(timezone.utc).isoformat(), python=platform.python_version(),
                        platform=platform.platform(), passed=passed,
                        configuration=dict(workflows=workflows, reads=reads, concurrency=concurrency, workers=workers,
                                           deadline=deadline, coordinator_poll_seconds=service.poll_interval,
                                           worker_poll_seconds=.05, worker_heartbeat_seconds=1),
                        http=dict(requests=len(latencies), errors=len(errors), error_types=sorted(set(errors)),
                                  elapsed_seconds=round(http_elapsed, 3), requests_per_second=round(len(latencies) / http_elapsed, 2),
                                  latency_ms=dict(p50=percentile(.5), p95=percentile(.95), p99=percentile(.99), maximum=percentile(1))),
                        execution=dict(completed_workflows=snapshot['workflows']['SUCCESS'], attempts=snapshot['attempts'],
                                       retries=snapshot['retries'], total_elapsed_seconds=round(total_elapsed, 3),
                                       workflows_per_second=round(snapshot['workflows']['SUCCESS'] / total_elapsed, 2)),
                        integrity=integrity)
        finally:
            for worker in consumers:
                worker.shutdown(5)
            service.shutdown(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflows', type=int, default=100)
    parser.add_argument('--reads', type=int, default=300)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--deadline', type=float, default=60)
    parser.add_argument('--output', type=Path)
    options = parser.parse_args()
    try:
        result = run_benchmark(workflows=options.workflows, reads=options.reads, concurrency=options.concurrency,
                               workers=options.workers, deadline=options.deadline)
    except ValueError as error:
        parser.error(str(error))
    raw = json.dumps(result, ensure_ascii=False, indent=2)
    if options.output:
        options.output.write_text(raw + '\n', encoding='utf-8')
    print(raw)
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
