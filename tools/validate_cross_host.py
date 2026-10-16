"""Real container/network faults; each invocation owns a fresh Compose project.

No Docker socket is mounted in application containers. The host supervisor alone
injects faults and attests an exact stopped container/session. Reports contain no
tokens. This is a deterministic workflow probe, not a live model quality test.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import Request, build_opener, ProxyHandler
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from workflow_engine.remote import _Transport
from workflow_engine import StaleExecutionError, WorkerSessionMismatch

SCENARIOS = ('network', 'crash', 'stale', 'non-idempotent')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def docker_executable():
    candidates = [shutil.which('docker')]
    if os.name == 'nt':
        candidates.append(str(Path(os.environ.get('ProgramFiles', 'C:/Program Files')) /
                              'Docker/Docker/resources/bin/docker.exe'))
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and Path(candidate).stat().st_size:
            return candidate
    raise RuntimeError('Docker CLI is unavailable; install/start Docker Engine or Docker Desktop')


class ContainerLab:
    def __init__(self, docker=None):
        self.docker = docker or docker_executable()
        self.project = 'workflow-fault-' + uuid4().hex[:12]
        self.env = dict(os.environ, WORKFLOW_RECOVERY_TOKEN=secrets.token_hex(32),
                        WORKFLOW_API_TOKEN=secrets.token_hex(32))
        self.prefix = ['compose', '-p', self.project, '-f', str(ROOT / 'deploy/compose.yaml'),
                       '--profile', 'workers']
        self.opener = build_opener(ProxyHandler({}))
        self.api_url = None

    def command(self, *args, timeout=90):
        result = subprocess.run([self.docker, *args], cwd=ROOT, env=self.env,
                                capture_output=True, text=True, timeout=timeout)
        require(result.returncode == 0, 'Docker operation failed: ' + result.stderr[-2000:])
        return result.stdout.strip()

    def compose(self, *args, **kwargs):
        return self.command(*self.prefix, *args, **kwargs)

    def container(self, service):
        value = self.compose('ps', '-aq', service)
        require(bool(value) and '\n' not in value, 'expected exactly one ' + service + ' container')
        return value

    def inspect(self, container):
        value = json.loads(self.command('inspect', container))[0]
        require(value['Config']['Labels'].get('com.docker.compose.project') == self.project,
                'refusing to operate on a container outside this lab')
        return value

    def request(self, path, body=None):
        raw = json.dumps(body).encode() if body is not None else None
        request = Request(self.api_url + path, raw, headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.env['WORKFLOW_API_TOKEN']})
        with self.opener.open(request, timeout=3) as response:
            return json.load(response)

    @staticmethod
    def wait(predicate, description, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.1)
        raise RuntimeError('deadline waiting for ' + description)

    def start(self):
        try:
            self.engine_version = self.command('version', '--format', '{{.Server.Version}}', timeout=20)
        except Exception as error:
            raise RuntimeError('Docker Engine unavailable: ' + str(error)) from error
        self.provisioned = True
        self.compose('build', 'central', 'victim', 'replacement', timeout=300)
        self.compose('up', '-d', '--no-build', '--wait', 'central', timeout=90)
        self.central = self.container('central')
        self.refresh_endpoints()
        self.request('/health/ready')

    def refresh_endpoints(self):
        # Docker may allocate different ephemeral host ports on container restart.
        api = self.compose('port', 'central', '8080')
        rpc = self.compose('port', 'central', '8090')
        for address in (api, rpc):
            host, _, port = address.rpartition(':')
            require(host == '127.0.0.1' and port.isdigit() and 0 < int(port) <= 65535,
                    'center ports must publish on host loopback; check the access network')
        self.api_url = 'http://' + api
        self.transport = _Transport('http://' + rpc, self.env['WORKFLOW_RECOVERY_TOKEN'])

    def workflow(self, scenario):
        # A completed upstream branch must survive recovery without replay.
        return dict(id=scenario, name='isolated-fault-probe', nodes=[
            dict(id='prepare', handler='probe.echo.v1', kwargs={'value': 'context'}, idempotent=True),
            dict(id='generate', handler='probe.echo.v1', depends_on=['prepare'],
                 kwargs={'value': 'answer'},
                 idempotent=scenario != 'non-idempotent'),
            dict(id='validate', handler='probe.summarize.v1',
                 inputs={'item': {'node_id': 'generate', 'path': []}}, idempotent=True)])

    def node(self, scenario, name='generate'):
        flow = self.request('/api/v1/workflows/' + scenario)
        return next(node for node in flow['nodes'] if node['id'] == name)

    def session(self, key):
        return self.request('/api/v1/sessions/' + key)

    def capture_attempt(self, task_id):
        # Trusted test supervisor captures the old RPC tuple before the fault.
        code = ('import sqlite3,json,sys; c=sqlite3.connect("/data/workflow.db"); '
                'c.row_factory=sqlite3.Row; '
                'r=c.execute("SELECT id,owner,worker_session,attempt_count FROM tasks WHERE id=?",'
                '(sys.argv[1],)).fetchone(); print(json.dumps(dict(r)))')
        row = json.loads(self.compose('exec', '-T', 'central', 'python', '-c', code, task_id))
        return dict(task_id=row['id'], owner=row['owner'], worker_session=row['worker_session'],
                    attempt_count=row['attempt_count'], outcome='SUCCESS', result='stale-overwrite', error=None)

    def reject_stale(self, old, worker_id):
        try:
            self.transport.call('finish', **old)
        except StaleExecutionError:
            pass
        else:
            raise RuntimeError('old result was accepted')
        try:
            self.transport.call('heartbeat', worker_id=worker_id, session_id=old['worker_session'])
        except WorkerSessionMismatch:
            pass
        else:
            raise RuntimeError('old heartbeat resurrected a revoked session')
        try:
            self.transport.call('claim_next', worker_session=old['worker_session'],
                                owner='revoked-claim-probe',
                                handlers=['probe.echo.v1', 'probe.summarize.v1'])
        except WorkerSessionMismatch:
            pass
        else:
            raise RuntimeError('old session was allowed to claim work')

    def attest_container_exit(self, container, session):
        value = self.inspect(container)
        state = value['State']
        require(not state['Running'] and not state.get('Restarting') and state['Pid'] == 0
                and state['Status'] == 'exited', 'container execution is not proven stopped')
        require(value['HostConfig']['RestartPolicy']['Name'] == 'no', 'automatic restart invalidates exit proof')
        require(value['Config']['Hostname'] == session['host_id'], 'session is not bound to this container')
        require(value['RestartCount'] == 0, 'container restarted since session capture')
        evidence = json.dumps(dict(container_id=value['Id'], session_id=session['session_id'],
                                   finished_at=state['FinishedAt'], exit_code=state['ExitCode']))
        self.compose('exec', '-T', 'central', 'python', '-m', 'workflow_engine.deployment',
            'confirm-stopped', '--database', '/data/workflow.db', '--session-id', session['session_id'],
            '--host-id', session['host_id'], '--operator', 'docker-lab-supervisor', '--evidence', evidence)
        return json.loads(evidence)

    def run(self, scenario):
        self.start()
        self.compose('up', '-d', '--no-build', 'victim')
        victim = self.container('victim')
        central_info, victim_info = self.inspect(self.central), self.inspect(victim)
        namespace_code = "import os; print(os.readlink('/proc/self/ns/net'))"
        central_namespace = self.compose('exec', '-T', 'central', 'python', '-c', namespace_code)
        victim_namespace = self.compose('exec', '-T', 'victim', 'python', '-c', namespace_code)
        require(central_namespace != victim_namespace, 'center and worker share a kernel network namespace')
        networks = victim_info['NetworkSettings']['Networks']
        require(len(networks) == 1, 'fault target must have exactly one network')
        network = next(iter(networks))
        require(central_info['NetworkSettings']['SandboxID'] != victim_info['NetworkSettings']['SandboxID'],
                'central and worker must use different network namespaces')
        require(not any(mount['Type'] in ('bind', 'volume') for mount in victim_info['Mounts']),
                'worker must not mount the central database or shared control files')
        self.request('/api/v1/workflows', self.workflow(scenario))
        node = self.wait(lambda: (n if (n := self.node(scenario))['task_status'] == 'RUNNING' else None),
                         'victim to enter generate node')
        task = self.request('/api/v1/tasks/' + node['task_id'])
        session = self.session(task['worker_session'])
        require(session['worker_id'] == 'victim' and session['host_id'] == victim_info['Config']['Hostname'],
                'attempt is not owned by captured victim container')
        old = self.capture_attempt(node['task_id'])
        self.wait(lambda: self.compose('exec', '-T', 'victim', 'python', '-c',
                  "from pathlib import Path; print(Path('/tmp/workflow-probe-entered').exists())") == 'True',
                  'actual business invocation to enter private runtime gate')
        partitioned = scenario in ('network', 'stale')
        if partitioned:
            self.command('network', 'disconnect', network, victim)
        else:
            self.command('kill', '--signal', 'KILL', victim)
        self.wait(lambda: self.session(session['session_id'])['state'] == 'FENCED', 'old session fencing')
        self.compose('up', '-d', '--no-build', 'replacement')
        self.wait(lambda: any(s['worker_id'] == 'replacement' and s['state'] == 'ACTIVE'
                             for s in self.request('/api/v1/sessions')['items']), 'replacement registration')
        # Center stays healthy, replacement is connected, but no replay is allowed.
        self.request('/health/ready')
        before = self.node(scenario)
        require((before['task_status'], before['attempt_count'], before['retry_count']) == ('RUNNING', 1, 0),
                'attempt replayed without trusted stop proof')
        incidents = self.request('/api/v1/incidents')['items']
        require(any(i['session_id'] == session['session_id'] and i['kind'] == 'WAIT_STOP' for i in incidents),
                'missing WAIT_STOP incident')
        self.reject_stale(old, 'victim')
        if partitioned:
            # Restore the actual link. Fencing remains permanent after reconnection.
            require(self.inspect(victim)['State']['Running'],
                    'victim exited before the supervisor released its invocation')
            self.command('network', 'connect', network, victim)
            self.reject_stale(old, 'victim')
            self.compose('exec', '-T', 'victim', 'python', '-c',
                         "from pathlib import Path; Path('/tmp/workflow-probe-release').touch()")
            self.wait(lambda: not self.inspect(victim)['State']['Running'], 'old worker to drain and exit')
        proof = self.attest_container_exit(victim, session)
        if scenario == 'non-idempotent':
            self.wait(lambda: self.node(scenario)['task_status'] == 'FAILED', 'uncertain failure')
            require(self.request('/api/v1/workflows/' + scenario)['status'] == 'PAUSED',
                    'uncertain non-idempotent workflow must await operator review')
            incident = next(i for i in self.request('/api/v1/incidents?status=OPEN')['items']
                            if i['task_id'] == node['task_id'])
            self.request('/api/v1/incidents/' + incident['id'] + '/resolve',
                         dict(action='fail', operator='docker-lab-supervisor',
                              reason='accept uncertain probe failure; never replay non-idempotent work'))
        expected = 'FAILED' if scenario == 'non-idempotent' else 'SUCCESS'
        self.wait(lambda: self.request('/api/v1/workflows/' + scenario)['status'] == expected,
                  'workflow ' + expected)
        final = self.request('/api/v1/workflows/' + scenario)
        generated = next(n for n in final['nodes'] if n['id'] == 'generate')
        prepared = next(n for n in final['nodes'] if n['id'] == 'prepare')
        require(prepared['attempt_count'] == 1, 'completed upstream branch was replayed')
        require((generated['attempt_count'], generated['retry_count']) ==
                ((1, 0) if scenario == 'non-idempotent' else (2, 1)), 'incorrect attempt/retry accounting')
        if expected == 'SUCCESS':
            validated = next(n for n in final['nodes'] if n['id'] == 'validate')
            require(validated['result']['answer'] == 'verified:answer', 'downstream binding/result is incorrect')
            require(generated['result']['host'] != session['host_id'], 'replacement did not execute recovered node')
        self.reject_stale(old, 'victim')
        new_session = None
        if scenario == 'stale':
            # Reuse the stable worker ID in a new container incarnation. Neither
            # its new heartbeat nor late old messages may revive the old session.
            self.compose('up', '-d', '--no-build', '--force-recreate', 'victim')
            def incarnation():
                return next((s for s in self.request('/api/v1/sessions')['items']
                             if s['worker_id'] == 'victim' and s['session_id'] != session['session_id']
                             and s['state'] == 'ACTIVE'), None)
            new_session = self.wait(incarnation, 'new incarnation of the same worker ID')
            self.reject_stale(old, 'victim')
            require(self.session(new_session['session_id'])['state'] == 'ACTIVE',
                    'stale messages changed the new worker session')
        history = self.request('/api/v1/tasks/' + node['task_id'] + '/events')['items']
        session_history = self.request('/api/v1/sessions/' + session['session_id'] + '/events')['items']
        # Restart the center on its original volume and check persisted results/events.
        self.compose('stop', 'replacement', 'victim')
        self.compose('restart', 'central')
        self.refresh_endpoints()
        def restarted():
            try:
                return self.request('/health/ready')
            except (URLError, OSError):
                return None
        self.wait(restarted, 'central restart readiness')
        require(self.request('/api/v1/workflows/' + scenario) == final, 'workflow changed across center restart')
        require(self.request('/api/v1/tasks/' + node['task_id'] + '/events')['items'] == history,
                'task history changed across center restart')
        return dict(scenario=scenario, status='passed', project=self.project,
                    isolation='docker-network-namespaces', center_container=self.central,
                    docker_engine_version=self.engine_version, image_id=central_info['Image'],
                    worker_container=victim, network=network, stop_proof=proof,
                    isolation_evidence=dict(center_hostname=central_info['Config']['Hostname'],
                        worker_hostname=victim_info['Config']['Hostname'],
                        center_network_namespace=central_info['NetworkSettings']['SandboxID'],
                        worker_network_namespace=victim_info['NetworkSettings']['SandboxID'],
                        center_kernel_network_namespace=central_namespace,
                        worker_kernel_network_namespace=victim_namespace,
                        worker_mounts=victim_info['Mounts']),
                    workflow=final, task_events=history, session_events=session_history,
                    new_incarnation=new_session)

    def close(self):
        # This project name is generated here; never operates on an existing stack.
        if getattr(self, 'provisioned', False):
            self.compose('down', '--volumes', '--remove-orphans', '--rmi', 'local', timeout=90)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=(*SCENARIOS, 'all'), default='all')
    parser.add_argument('--docker', help='Docker executable path')
    parser.add_argument('--output', type=Path, default=ROOT / '.artifacts/cross-host-report.json')
    options = parser.parse_args(argv)
    report = dict(started_at=datetime.now(timezone.utc).isoformat(), results=[])
    try:
        for scenario in SCENARIOS if options.scenario == 'all' else (options.scenario,):
            lab = ContainerLab(options.docker)
            failure = None
            try:
                result = lab.run(scenario)
                report['results'].append(result)
                print(json.dumps(dict(scenario=scenario, status='passed')), flush=True)
            except Exception as error:
                failure = error
                raise
            finally:
                try:
                    lab.close()
                except Exception as error:
                    report.setdefault('cleanup_errors', []).append(str(error))
                    if failure is None:
                        raise
        report['status'] = 'passed'
    except Exception as error:
        report.update(status='blocked' if 'Docker Engine unavailable' in str(error) else 'failed', error=str(error))
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(dict(status=report['status'], report=str(options.output))), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
