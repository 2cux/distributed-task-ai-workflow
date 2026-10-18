"""Offline verification and summary; never generates load or changes the database."""
from collections import Counter, defaultdict
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics

ROOT = Path(__file__).resolve().parent


def quantiles(values):
    values = sorted(values)
    return {name: round(values[max(0, math.ceil(len(values) * q) - 1)] * 1000, 3)
            for name, q in [('p50', .5), ('p95', .95), ('p99', .99), ('maximum', 1)]}


def inspect(path):
    raw = json.loads(path.read_text(encoding='utf-8'))
    result = raw['result']
    with closing(sqlite3.connect(str(path).removesuffix('.raw.json') + '.sqlite3')) as db:
        db.row_factory = sqlite3.Row
        rows = list(db.execute('SELECT n.workflow_id,n.node_id,t.* FROM workflow_nodes n JOIN tasks t ON t.id=n.task_id'))
        success_counts = dict(db.execute("SELECT task_id,count(*) FROM task_events WHERE event_type='SUCCEEDED' GROUP BY task_id"))
        workflows = dict(db.execute('SELECT id,status FROM workflow_runs'))
        expected = {(f'benchmark-{i}', node): i for i in range(100) for node in ('root', 'join')}
        observed = Counter((row['workflow_id'], row['node_id']) for row in rows)
        invalid = [row['id'] for row in rows if row['status'] != 'SUCCESS' or
                   json.loads(row['result'] or 'null') != expected.get((row['workflow_id'], row['node_id'])) or
                   type(json.loads(row['result'] or 'null')) is not int or
                   row['attempt_count'] != 1 or row['retry_count'] != 0 or success_counts.get(row['id']) != 1]
        checks = dict(exact_workflow_ids=set(workflows) == {f'benchmark-{i}' for i in range(100)},
                      all_workflows_success=bool(workflows) and all(s == 'SUCCESS' for s in workflows.values()),
                      exact_nodes=observed == Counter({key: 1 for key in expected}),
                      outputs_attempts_success_events=len(rows) == 200 and not invalid,
                      integrity=db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok',
                      foreign_keys=not list(db.execute('PRAGMA foreign_key_check')))
        accepted_expected = Counter({(identifier, node): 1 for identifier in workflows for node in ('root', 'join')})
        accepted_correct = bool(workflows) and observed == accepted_expected and not invalid and all(s == 'SUCCESS' for s in workflows.values())
        evidence = dict(persisted_workflows=len(workflows), persisted_tasks=len(rows),
                        missing_workflows=sorted({f'benchmark-{i}' for i in range(100)} - set(workflows)),
                        accepted_workflows_correct=accepted_correct,
                        attempts=sum(row['attempt_count'] for row in rows),
                        retries=sum(row['retry_count'] for row in rows),
                        task_status_counts=dict(Counter(row['status'] for row in rows)))
    latency = raw['latency_seconds_submission_order']
    assert raw['native']['logged_requests'] + raw['native']['overloads'] == 400, path
    calculated = quantiles(latency)
    assert calculated == result['http']['latency_ms'], (path, calculated)
    assert len(latency) == result['http']['requests'] == 400
    endpoint_samples = defaultdict(list)
    jobs = []
    for i in range(300):
        if i < 100:
            jobs.append('POST /api/v1/workflows')
        jobs.append(('GET /api/v1/overview', 'GET /api/v1/workflows?limit=10', 'GET /metrics')[(100 + i) % 3])
    for endpoint, elapsed in zip(jobs, latency):
        endpoint_samples[endpoint].append(elapsed)
    endpoints = {key: dict(requests=len(values), latency_ms=quantiles(values)) for key, values in endpoint_samples.items()}
    return dict(round=path.name.removesuffix('.raw.json'), result=result, native=raw['native'],
                latency_seconds=latency, database_profile=raw['database_profile'],
                offline_correctness=dict(passed=all(checks.values()), checks=checks, invalid_tasks=invalid, **evidence),
                endpoints=endpoints)


def main():
    summary = {}
    for kind in ('baseline', 'diagnostic'):
        all_rounds = [inspect(path) for path in sorted((ROOT / kind).glob('*.raw.json'))]
        if not all_rounds:
            continue
        runs = [run for run in all_rounds if run['round'].startswith('measured')]
        latencies = [value for run in runs for value in run['latency_seconds']]
        requests = sum(run['result']['http']['requests'] for run in runs)
        errors = sum(run['result']['http']['errors'] for run in runs)
        seconds = sum(run['result']['http']['elapsed_seconds'] for run in runs)
        statuses = Counter()
        failure_kinds = Counter()
        for run in runs:
            statuses.update(run['native']['http_status_counts'])
            failure_kinds.update(run['native']['failure_kinds'])
        summary[kind] = dict(rounds=len(runs), warmups=len(all_rounds)-len(runs), requests=requests,
            errors=errors, error_rate=errors/requests, weighted_http_requests_per_second=round(requests/seconds, 3),
            pooled_client_latency_ms=quantiles(latencies), server_status_counts=dict(statuses),
            server_failure_kinds=dict(failure_kinds),
            http_503=int(statuses.get('503', 0)) + sum(run['native']['overloads'] for run in runs),
            native_database_busy=sum(run['native']['database_busy_errors'] for run in runs),
            native_overloads=sum(run['native']['overloads'] for run in runs),
            coordinator_errors=sum(run['native']['coordinator_errors'] for run in runs),
            all_offline_correctness=all(run['offline_correctness']['passed'] for run in all_rounds),
            all_accepted_workflows_correct=all(run['offline_correctness']['accepted_workflows_correct'] for run in all_rounds),
            completed_workflows=sum(run['offline_correctness']['persisted_workflows'] for run in runs),
            task_attempts=sum(run['offline_correctness']['attempts'] for run in runs),
            task_retries=sum(run['offline_correctness']['retries'] for run in runs),
            per_round=[{k:v for k,v in run.items() if k != 'latency_seconds'} for run in all_rounds])
    (ROOT / 'analysis.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({key:{k:v for k,v in value.items() if k != 'per_round'} for key,value in summary.items()}, indent=2))
    manifest = {str(path.relative_to(ROOT)).replace('\\', '/'): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(ROOT.rglob('*')) if path.is_file() and path.name != 'SHA256SUMS.json'
                and '__pycache__' not in path.parts}
    (ROOT / 'SHA256SUMS.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')


if __name__ == '__main__':
    main()
