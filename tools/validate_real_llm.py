"""Opt-in live provider validation; never substitutes a simulated LLM.

Run from the repository root with PYTHONPATH=src. Credentials are inherited
through the environment only. Each run uses a new, retained artifact directory.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from workflow_engine import (LLMConfig, LLM_HANDLER, LLM_OUTPUT_HANDLER, OpenAIChatHandler,
                             RemoteWorker, TaskEngine, validate_llm_output)


ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def configured_env():
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'), PYTHONIOENCODING='utf-8')
    # Configuration compatibility is exercised in product code, not translated here.
    LLMConfig.from_env(env)  # Validate before starting services or submitting work.
    return env


def worker_main(args):
    directory = args.output.resolve()
    config = LLMConfig.from_env(configured_env())
    handler = OpenAIChatHandler(config)
    nonce = args.nonce
    journal = directory / f'worker-{os.getpid()}.jsonl'

    def log(**entry):
        with journal.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(dict(pid=os.getpid(), time=time.time(), **entry)) + '\n')

    def retrieve():
        return {'a': f'A:{nonce}: 请计算 19+23，只输出整数结果。',
                'b': f'B:{nonce}: 请计算 7*8，只输出整数结果。'}

    def model(**kwargs):
        prompt = kwargs['prompt']
        branch = prompt[0].lower() if prompt.startswith(('A:', 'B:')) else 'judge'
        if branch in ('a', 'b'):
            (directory / ('entered-' + branch)).write_text(str(os.getpid()))
            deadline = time.monotonic() + 20
            while not all((directory / ('entered-' + peer)).exists() for peer in ('a', 'b')):
                require(time.monotonic() < deadline, 'parallel peer did not enter')
                time.sleep(.02)
        log(event='llm_started', branch=branch, prompt=prompt)
        try:
            result = handler(**kwargs)
        except Exception as error:
            # Only built-in sanitized adapter errors reach this journal.
            log(event='llm_failed', branch=branch, error_type=type(error).__name__)
            raise
        log(event='llm_finished', branch=branch, result=result)
        return result

    def join(left, right):
        require(left.strip() == '42', 'branch A answer mismatch')
        require(right.strip() == '56', 'branch B answer mismatch')
        return {'prompt': f'请求 {nonce}：上游结果为 {left} 和 {right}。计算两个结果之和，只输出整数结果。'}

    registry = {LLM_HANDLER: model, 'retrieve': retrieve, 'join': join,
                LLM_OUTPUT_HANDLER: validate_llm_output}
    worker = RemoteWorker(args.rpc_url, registry, os.environ['LIVE_VALIDATION_RPC_TOKEN'],
                          args.worker_id, host_id=args.worker_id,
                          heartbeat_interval=.2, heartbeat_timeout=15, poll_interval=.02)
    worker.start()
    try:
        deadline = time.monotonic() + 240
        while not (directory / 'done').exists():
            require(worker.last_error is None, 'remote worker control failure')
            require(time.monotonic() < deadline, 'supervisor deadline exceeded')
            time.sleep(.05)
    finally:
        stopped = worker.shutdown(config.timeout + 10)
        log(event='worker_stopped', stopped=stopped, processed_count=worker.processed_count,
            error_type=type(worker.last_error).__name__ if worker.last_error else None)
    require(stopped and worker.last_error is None, 'worker failed to stop cleanly')


def cli_validation(directory, env, nonce):
    flow = dict(id='live-cli-' + nonce, nodes=[
        dict(id='draft', handler=LLM_HANDLER,
             kwargs={'prompt': f'Reply only CLI:{nonce}:42, with no extra text.'}),
        dict(id='contract', handler=LLM_OUTPUT_HANDLER, kwargs={'expected_text': f'CLI:{nonce}:42'},
             inputs={'result': {'node_id': 'draft'}}),
        dict(id='review', handler=LLM_HANDLER,
             kwargs={'system_prompt': 'Echo the user input exactly, with no extra text.'},
             inputs={'prompt': {'node_id': 'contract', 'path': ['text']}}),
    ])
    definition = directory / 'cli-workflow.json'
    save(definition, flow)
    database = directory / 'cli.sqlite3'
    command = [sys.executable, '-m', 'workflow_engine.llm_cli', '--database', str(database),
               '--workflow', str(definition)]
    results = []
    for index in range(2):
        completed = subprocess.run(command, env=env, capture_output=True, text=True,
                                   encoding='utf-8', timeout=180)
        # Successful stdout contains answers/usage only, never configuration.
        save(directory / f'cli-process-{index + 1}.json', dict(returncode=completed.returncode))
        value = json.loads(completed.stdout)
        save(directory / f'cli-result-{index + 1}.json', value)
        require(completed.returncode == 0, f'CLI run {index + 1} exited {completed.returncode}')
        require(value['status'] == 'SUCCESS', 'CLI workflow failed')
        for node in value['nodes'].values():
            require(node['result']['text'].strip() == f'CLI:{nonce}:42', 'CLI answer mismatch')
            require(node['attempt_count'] == 1 and node['retry_count'] == 0, 'CLI duplicate attempt')
        results.append(value)
    require(results[0] == results[1], 'CLI replay changed saved results')
    return dict(passed=True, nodes=3, successful_provider_calls=2, restart_reused_result=True,
                output_contract_passed=True,
                results=[results[0]['nodes'][key]['result'] for key in ('draft', 'review')])


def remote_validation(directory, env, nonce):
    def central_only(*args, **kwargs):
        raise AssertionError('central process must never execute business handlers')

    # Distinct callable identities preserve the registry's reverse mapping.
    registry = {name: (lambda *a, **kw: central_only(*a, **kw))
                for name in (LLM_HANDLER, 'retrieve', 'join', LLM_OUTPUT_HANDLER)}
    database = directory / 'remote.sqlite3'
    engine = TaskEngine(database_path=database, task_registry=registry)
    admin, reader, rpc_token = (secrets.token_urlsafe(24) for _ in range(3))
    env = dict(env, LIVE_VALIDATION_RPC_TOKEN=rpc_token)
    service = engine.create_management_service(admin, readonly_token=reader, poll_interval=.02).start()
    rpc = engine.create_recovery_service(rpc_token).start()
    processes = []
    opener = build_opener(ProxyHandler({}))

    def call(path, body=None, token=admin):
        headers = {'Content-Type': 'application/json'}
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
        request = Request(service.url + path,
                          data=json.dumps(body, ensure_ascii=False).encode() if body is not None else None,
                          headers=headers)
        try:
            response = opener.open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            raw = response.read().decode('utf-8')
            return response.status, (json.loads(raw) if response.headers.get_content_type() == 'application/json'
                                     else raw)

    def ok(path, body=None, token=admin, status=200):
        code, value = call(path, body, token)
        require(code == status, f'{path} returned HTTP {code}')
        return value

    def until(predicate, timeout=180):
        deadline = time.monotonic() + timeout
        while not predicate():
            if not (directory / 'done').exists():
                require(all(p.poll() is None for p in processes), 'a worker process exited early')
            require(time.monotonic() < deadline, 'workflow condition deadline exceeded')
            time.sleep(.05)

    flow = dict(id='live-remote-' + nonce, name='real-provider-parallel-review', nodes=[
        dict(id='retrieve', handler='retrieve'),
        dict(id='a', handler=LLM_HANDLER, inputs={'prompt': {'node_id': 'retrieve', 'path': ['a']}}),
        dict(id='b', handler=LLM_HANDLER, inputs={'prompt': {'node_id': 'retrieve', 'path': ['b']}}),
        dict(id='join', handler='join', inputs={'left': {'node_id': 'a', 'path': ['text']},
                                              'right': {'node_id': 'b', 'path': ['text']}}),
        dict(id='judge', handler=LLM_HANDLER, inputs={'prompt': {'node_id': 'join', 'path': ['prompt']}}),
        dict(id='validate', handler=LLM_OUTPUT_HANDLER, kwargs={'expected_text': '98'},
             inputs={'result': {'node_id': 'judge'}}),
    ])
    endpoint = '/api/v1/workflows/' + flow['id']
    operator = dict(operator='live-llm-validation', reason='end-to-end verification')
    try:
        until(lambda: call('/health/ready', token=None)[0] == 200, 10)
        require(call('/api/v1/workflows', flow, reader)[0] == 403, 'reader submitted work')
        require(call('/api/v1/workflows', flow, 'invalid')[0] == 401, 'invalid token accepted')
        require(ok('/api/v1/overview')['attempts'] == 0, 'unauthorized request executed')
        accepted = ok('/api/v1/workflows', flow, status=202)
        require(ok('/api/v1/workflows', flow, status=202) == accepted, 'duplicate submission differs')
        require(ok(endpoint + '/pause', operator)['status'] == 'PAUSED', 'pause failed')
        save(directory / 'remote-workflow.json', flow)
        for index in range(2):
            processes.append(subprocess.Popen([
                sys.executable, str(Path(__file__).resolve()), '--worker', '--output', str(directory),
                '--rpc-url', rpc.url, '--worker-id', f'live-llm-{index}', '--nonce', nonce,
            ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        until(lambda: len(ok('/api/v1/sessions')['items']) == 2, 15)
        time.sleep(.25)
        require(ok('/api/v1/overview')['attempts'] == 0, 'paused DAG executed')
        ok(endpoint + '/resume', operator)

        def terminal():
            value = ok(endpoint, token=reader)
            if value['status'] in ('FAILED', 'PAUSED'):
                save(directory / 'remote-failed.json', value)
                save(directory / 'remote-failure-diagnostics.json', dict(
                    overview=ok('/api/v1/overview'), incidents=ok('/api/v1/incidents?status=OPEN'),
                    events=ok(endpoint + '/events?limit=100')))
                raise AssertionError('live remote DAG failed; see remote-failed.json')
            return value['status'] == 'SUCCESS'

        until(terminal)
        result = ok(endpoint, token=reader)
        save(directory / 'remote-result.json', result)
        nodes = {node['id']: node for node in result['nodes']}
        require(nodes['validate']['result'] == nodes['judge']['result'] and
                nodes['validate']['result']['text'] == '98', 'final answer validation failed')
        require(len(nodes) == 6, 'unexpected node count')
        events = ok(endpoint + '/events?limit=100', token=reader)['items']
        save(directory / 'remote-events.json', events)
        for node in nodes.values():
            require(node['status'] == 'SUCCESS' and node['attempt_count'] == 1
                    and node['retry_count'] == 0, 'node execution count mismatch')
            task = ok('/api/v1/tasks/' + node['task_id'], token=reader)
            require(task['result'] == node['result'], 'task result differs from node result')
            history = [event for event in events if event['task_id'] == node['task_id']]
            require([event['event_type'] for event in history] ==
                    ['CREATED', 'SUBMITTED', 'STARTED', 'SUCCEEDED'], 'unexpected event history')
            require(history[-1]['details']['result'] == node['result'], 'event result differs')
        require(len({event['cursor'] for event in events}) == len(events), 'duplicate event cursor')
        overview = ok('/api/v1/overview', token=reader)
        require((overview['attempts'], overview['retries'], overview['open_incidents']) == (6, 0, 0),
                'overview mismatch')
        metrics = ok('/metrics', token=reader)
        require('workflow_engine_attempts 6\n' in metrics, 'metrics mismatch')
        (directory / 'remote-metrics.txt').write_text(metrics, encoding='utf-8')
        # Ask the real provider for a nonexistent model; this is a genuine HTTP failure.
        failed_flow = dict(id='live-rejected-' + nonce, nodes=[
            dict(id='bad_model', handler=LLM_HANDLER,
                 kwargs={'model': '__missing_validation_model_' + nonce, 'prompt': 'Reply OK.'}),
            dict(id='downstream', handler=LLM_OUTPUT_HANDLER,
                 inputs={'result': {'node_id': 'bad_model'}}),
        ])
        ok('/api/v1/workflows', failed_flow, status=202)
        failed_endpoint = '/api/v1/workflows/' + failed_flow['id']
        until(lambda: ok(failed_endpoint)['status'] in ('PAUSED', 'FAILED', 'SUCCESS'))
        failure = ok(failed_endpoint)
        failures = {node['id']: node for node in failure['nodes']}
        require(failure['status'] in ('FAILED', 'PAUSED'), 'provider accepted nonexistent model')
        require(failures['bad_model']['status'] == 'FAILED' and
                failures['bad_model']['attempt_count'] == 1 and
                failures['bad_model']['retry_count'] == 0, 'unexpected failed attempt count')
        require('LLMHTTPError' in str(failures['bad_model']['error']), 'failure was not provider HTTP')
        until(lambda: len(ok('/api/v1/incidents?status=OPEN')['items']) == 1)
        incidents = ok('/api/v1/incidents?status=OPEN')['items']
        require(incidents[0]['task_id'] == failures['bad_model']['task_id'], 'wrong task incident')
        require(ok(failed_endpoint)['status'] == 'PAUSED', 'failed workflow was not held for review')
        save(directory / 'provider-failure-paused.json', ok(failed_endpoint))
        save(directory / 'provider-failure-incident.json', incidents)
        ok('/api/v1/incidents/' + incidents[0]['id'] + '/resolve', dict(
            action='fail', operator='live-llm-validation', reason='accept expected nonexistent-model rejection'))
        until(lambda: ok(failed_endpoint)['status'] == 'FAILED')
        failure = ok(failed_endpoint)
        save(directory / 'provider-failure-result.json', failure)
        failures = {node['id']: node for node in failure['nodes']}
        require(failures['downstream']['status'] == 'SKIPPED' and
                failures['downstream']['attempt_count'] == 0, 'failed descendants executed')
        require(ok('/api/v1/incidents?status=OPEN')['items'] == [], 'failure incident remained open')

        for session in ok('/api/v1/sessions')['items']:
            ok('/api/v1/sessions/' + session['session_id'] + '/stop', operator, status=202)
        until(lambda: all(s['state'] == 'STOPPED' for s in ok('/api/v1/sessions')['items']), 15)
        (directory / 'done').touch()
        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            # Worker error messages may contain arbitrary data; preserve no raw stderr.
            require(process.returncode == 0, f'worker {process.pid} exited {process.returncode}')
        audit = ok('/api/v1/audit?limit=100')['items']
        actions = [entry['action'] for entry in audit]
        require(actions == ['WORKFLOW_PAUSED', 'WORKFLOW_RESUMED', 'INCIDENT_OPENED',
                            'INCIDENT_RESOLVED', 'WORKER_STOP_REQUESTED', 'WORKER_STOP_REQUESTED'],
                'audit sequence mismatch')
        save(directory / 'remote-audit.json', audit)
        sessions = ok('/api/v1/sessions')['items']
        save(directory / 'remote-sessions.json', sessions)
        require(rpc.shutdown(5), 'RPC shutdown failed')
        require(service.shutdown(5), 'management shutdown failed')
        engine = TaskEngine(database_path=database, task_registry=registry)
        service = engine.create_management_service(admin, readonly_token=reader, poll_interval=.02).start()
        until(lambda: call('/health/ready', token=None)[0] == 200, 10)
        require(ok(endpoint, token=reader) == result, 'restart changed results')
        require(ok(endpoint + '/events?limit=100')['items'] == events, 'restart changed events')
        require(ok('/api/v1/audit?limit=100')['items'] == audit, 'restart changed audit')
        require(ok('/api/v1/workflows', flow, status=202) == result, 'restart replay changed result')
        require(ok('/api/v1/workflows', failed_flow, status=202) == failure, 'failed workflow replay differs')
        require(ok('/api/v1/overview')['attempts'] == 7, 'restart reran completed tasks')
        require(engine.start() == [], 'restart left executable tasks')

        journal = [json.loads(line) for path in directory.glob('worker-*.jsonl')
                   for line in path.read_text(encoding='utf-8').splitlines()]
        save(directory / 'provider-journal.json', journal)
        starts = {entry['branch']: entry for entry in journal if entry['event'] == 'llm_started'
                  and entry['branch'] in ('a', 'b')}
        ends = {entry['branch']: entry for entry in journal if entry['event'] == 'llm_finished'
                and entry['branch'] in ('a', 'b')}
        require(len(starts) == 2 and len(ends) == 2, 'missing parallel call evidence')
        require({entry['pid'] for entry in starts.values()} == {p.pid for p in processes},
                'parallel branches did not use distinct worker processes')
        require(max(entry['time'] for entry in starts.values()) < min(entry['time'] for entry in ends.values()),
                'provider request intervals did not overlap')
        require(sum(entry['event'] == 'llm_started' for entry in journal) == 4, 'unexpected provider call count')
        require(sum(entry['event'] == 'llm_finished' for entry in journal) == 3, 'unexpected success count')
        require(sum(entry['event'] == 'llm_failed' for entry in journal) == 1, 'missing rejected call')
        return dict(passed=True, successful_nodes=6, failed_nodes=1, skipped_nodes=1,
                    successful_provider_calls=3, rejected_provider_calls=1,
                    worker_pids=[p.pid for p in processes], parallel_requests_overlap=True,
                    provider_error=failures['bad_model']['error'],
                    restart_reused_results_events_audit=True, permission_and_pause_checks=True,
                    provider_failure_incident_resolved=True,
                    metrics_consistent=True, worker_stop_clean=True,
                    output_contract_passed=True,
                    results=[nodes[key]['result'] for key in ('a', 'b', 'judge')])
    finally:
        (directory / 'done').touch()
        for process in processes:
            if process.poll() is None:
                try:
                    process.communicate(timeout=75)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)
        rpc.shutdown(5)
        service.shutdown(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--rpc-url', help=argparse.SUPPRESS)
    parser.add_argument('--worker-id', help=argparse.SUPPRESS)
    parser.add_argument('--nonce', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker_main(args)
        return 0
    env = configured_env()
    config = LLMConfig.from_env(env)
    nonce = uuid4().hex[:12]
    directory = (args.output or ROOT / '.artifacts' / ('real-llm-' + nonce)).resolve()
    # Existing artifacts must not turn a fresh validation into a cache hit.
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    summary = dict(status='RUNNING', started_at=datetime.now(timezone(timedelta(hours=8))).isoformat(),
                   commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                   python=sys.version.split()[0], provider_host=urlsplit(config.base_url).hostname,
                   requested_model=config.model, artifact_directory=str(directory),
                   credentials_source='LLM_* or fallback APP_LLM_*; environment only')
    save(directory / 'summary.json', summary)
    try:
        print('Running live CLI chain and restart replay...', flush=True)
        summary['cli'] = cli_validation(directory, env, nonce)
        print('Running management HTTP / two remote processes / live provider...', flush=True)
        summary['remote'] = remote_validation(directory, env, nonce)
        summary['database_checks'] = {}
        for name in ('cli', 'remote'):
            with closing(sqlite3.connect(directory / (name + '.sqlite3'))) as db:
                integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
                foreign_keys = db.execute('PRAGMA foreign_key_check').fetchall()
                dump = '\n'.join(db.iterdump())
            require(integrity == 'ok' and not foreign_keys, 'database consistency check failed')
            require(config.api_key not in dump, 'credential leaked into database')
            summary['database_checks'][name] = dict(integrity=integrity, foreign_key_violations=0,
                                                  api_key_absent=True)
        results = summary['cli']['results'] + summary['remote']['results']
        for result in results:
            require(result['response_id'] and result['model'] and result['text'].strip(),
                    'missing provider response metadata')
            require(result['finish_reason'] == 'stop', 'provider answer truncated or interrupted')
            require(all(type(result['usage'].get(key)) is int and result['usage'][key] > 0
                        for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')),
                    'provider did not supply positive token counts')
        require(len({r['response_id'] for r in results}) == 5, 'duplicate provider response IDs')
        summary['confirmed_successful_provider_calls'] = 5
        summary['confirmed_success_usage'] = {key: sum(r['usage'][key] for r in results)
                                              for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
        summary['rejected_provider_calls'] = 1
        summary['status'] = 'PASSED'
    except Exception as error:
        summary['status'] = 'FAILED'
        # Avoid arbitrary provider/transport exception text in the public summary.
        summary['failure_type'] = type(error).__name__
        if isinstance(error, AssertionError):
            summary['failure'] = str(error)
        raise
    finally:
        summary['elapsed_seconds'] = round(time.monotonic() - started, 3)
        save(directory / 'summary.json', summary)
        print(json.dumps({k: v for k, v in summary.items() if k not in ('cli', 'remote')},
                         ensure_ascii=True, indent=2), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
