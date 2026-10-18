"""Collect existing management benchmark evidence from an immutable Git export.

No workload or timeout overrides. Native logs stay in memory during load; files,
task export and SQLite backup are written after each timed benchmark completes.
Use --probe only for separate diagnostic runs (existing DatabaseProbe overhead).
"""
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import sqlite3
import sys
import time
import traceback


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--repeat', type=int, default=5)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--probe', action='store_true')
    args = parser.parse_args()
    snapshot, output = args.snapshot.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.chdir(snapshot)
    sys.path[:0] = [str(snapshot), str(snapshot / 'src')]
    from tools import benchmark_management as bench
    from tools.benchmark_ai_workflow import DatabaseProbe
    import workflow_engine.management as management

    hashes = {str(path.relative_to(snapshot)).replace('\\', '/'): hashlib.sha256(path.read_bytes()).hexdigest()
              for folder in ('src', 'tools') for path in sorted((snapshot / folder).rglob('*.py'))}
    save(output / 'environment.json', dict(commit=args.commit, snapshot=str(snapshot),
         started_utc=datetime.now(timezone.utc).isoformat(), python=sys.version,
         executable=sys.executable, platform=platform.platform(), machine=platform.machine(),
         logical_cpus=os.cpu_count(), sqlite=sqlite3.sqlite_version, mode='diagnostic' if args.probe else 'baseline',
         repeat=args.repeat, warmup=args.warmup, imported_benchmark=str(Path(bench.__file__).resolve()),
         imported_management=str(Path(management.__file__).resolve()), source_sha256=hashes,
         command=sys.argv, note='Default run_benchmark() arguments; no timeout overrides.'))

    state = {}
    create = bench.TaskEngine.create_management_service
    verify = bench._verify_tasks
    latency = bench._latency_summary

    def capture_service(engine, *positional, **keywords):
        service = create(engine, *positional, **keywords)
        state['service'] = service
        if args.probe:
            state['probe'] = DatabaseProbe(engine._store)
        return service

    def capture_tasks(rows, workflows):
        rows = list(rows)
        state['tasks'] = [dict(row) for row in rows]
        service = state['service']
        probe = state.get('probe')
        state['database_profile'] = probe.report() if probe else None
        if probe:
            probe.close()
        # Benchmark has stopped workers/service and closed its timing intervals,
        # but still owns its temporary directory at this verification callback.
        with closing(sqlite3.connect(service.store.path)) as source:
            with closing(sqlite3.connect(str(state['prefix']) + '.sqlite3')) as destination:
                source.backup(destination)
            state['pragmas'] = {name: source.execute('PRAGMA ' + name).fetchone()[0]
                                for name in ('journal_mode', 'synchronous', 'page_size')}
            state['foreign_keys'] = list(source.execute('PRAGMA foreign_key_check'))
            state['event_counts'] = dict(source.execute('SELECT event_type,count(*) FROM task_events GROUP BY event_type'))
            state['success_events'] = list(source.execute("SELECT task_id,count(*) FROM task_events WHERE event_type='SUCCEEDED' GROUP BY task_id"))
        state['overview'] = service.queries.overview()
        state['metrics'] = service.metrics.render(state['overview'])
        return verify(rows, workflows)

    def capture_latency(values):
        state['latency_seconds'] = list(values)
        return latency(values)

    class Capture(logging.Handler):
        def emit(self, record):
            state.setdefault('logs', []).append(dict(created=record.created, level=record.levelname,
                                                    thread=record.threadName, message=record.getMessage()))

    handler = Capture()
    logger = logging.getLogger('workflow_engine.management')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    bench.TaskEngine.create_management_service = capture_service
    bench._verify_tasks = capture_tasks
    bench._latency_summary = capture_latency
    warmups, runs = [], []
    for ordinal in range(args.warmup + args.repeat):
        phase = 'warmup' if ordinal < args.warmup else 'measured'
        index = ordinal + 1 if phase == 'warmup' else ordinal - args.warmup + 1
        prefix = output / f'{phase}-{index:02d}'
        state.clear()
        state['prefix'] = prefix
        print(f'{datetime.now(timezone.utc).isoformat()} START {prefix.name}', flush=True)
        before_cpu, before_wall = time.process_time(), time.perf_counter()
        try:
            result = bench.run_benchmark()
        except BaseException:
            prefix.with_suffix('.exception.txt').write_text(traceback.format_exc(), encoding='utf-8')
            raise
        cpu, wall = time.process_time() - before_cpu, time.perf_counter() - before_wall
        service = state['service']
        database_profile = state['database_profile']
        prefix.with_suffix('.metrics.prom').write_text(state['metrics'], encoding='utf-8')
        events = []
        with prefix.with_suffix('.server.jsonl').open('w', encoding='utf-8') as stream:
            for record in state.get('logs', []):
                try:
                    record['event'] = json.loads(record.pop('message'))
                except ValueError:
                    pass
                events.append(record.get('event', {}))
                stream.write(json.dumps(record, ensure_ascii=False) + '\n')
        requests = [event for event in events if event.get('event') == 'http_request']
        native = dict(http_status_counts=dict(Counter(str(event['status']) for event in requests)),
                      failure_kinds=dict(Counter(event['failure_kind'] for event in requests if event.get('failure_kind'))),
                      logged_requests=len(requests), overloads=service.metrics.overloads,
                      database_busy_errors=service.metrics.database_busy_errors,
                      coordinator_ticks=service.metrics.coordinator_ticks,
                      coordinator_errors=service.metrics.coordinator_errors)
        save(prefix.with_suffix('.raw.json'), dict(result=result, native=native,
             latency_seconds_submission_order=state['latency_seconds'], task_rows=state['tasks'],
             overview_after_shutdown=state['overview'], database_connection_pragmas=state['pragmas'],
             foreign_key_violations=state['foreign_keys'], task_event_counts=state['event_counts'],
             succeeded_event_counts=state['success_events'], database_profile=database_profile,
             process_cpu_seconds=cpu, collector_round_wall_seconds=wall,
             service_configuration=dict(request_timeout=service.request_timeout,
                 query_busy_timeout=service.queries.busy_timeout,
                 max_concurrency=service.max_concurrency,
                 request_queue_size=service._server.request_queue_size)))
        (warmups if phase == 'warmup' else runs).append(result)
        save(output / 'progress.json', dict(warmup_runs=warmups, runs=runs))
        print(json.dumps(dict(round=prefix.name, passed=result['passed'], http=result['http'],
                              native=native, database_profile=database_profile), ensure_ascii=False), flush=True)
    suite = dict(schema_version=2, workload=runs[0]['workload'], timestamp=runs[-1]['timestamp'],
                 python=runs[0]['python'], platform=runs[0]['platform'], sqlite=runs[0]['sqlite'],
                 passed=all(run['passed'] for run in warmups + runs),
                 configuration={**runs[0]['configuration'], 'repeat': args.repeat, 'warmup': args.warmup},
                 warmup_runs=warmups, runs=runs, summary=bench.summarize_runs(runs))
    save(output / 'suite.json', suite)
    (output / 'suite.md').write_text(bench.render_markdown(suite), encoding='utf-8')
    print(f'COMPLETE {output} benchmark_passed={suite["passed"]}', flush=True)
    return 0 if suite['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
