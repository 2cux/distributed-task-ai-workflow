"""Stale result/heartbeat/claim RPCs and central restart use actual HTTP."""
from urllib.error import HTTPError

from deployment_test_support import DeploymentTestCase
from workflow_engine import StaleExecutionError, WorkerSessionMismatch
from workflow_engine.deployment_demo import create_registry
from workflow_engine.remote import RemoteAuthenticationError, RemoteTaskStore


class SessionDeploymentTests(DeploymentTestCase):
    def test_fenced_session_cannot_write_claim_or_resurrect_after_same_id_restarts(self):
        self.submit(delay=0)
        remote = RemoteTaskStore(self.rpc_url, create_registry(), self.env['WORKFLOW_RECOVERY_TOKEN'])
        first = remote.workers.register('protocol-worker', heartbeat_timeout=.1, host_id='protocol-host')
        task = remote.claim_next(first.session_id, 'old-owner')
        self.wait(lambda: self.request('/api/v1/sessions/' + first.session_id)['state'] == 'FENCED')
        # An actual old completion tuple is rejected even before any replacement.
        args = dict(task_id=task.id, owner='old-owner', worker_session=first.session_id,
                    attempt_count=1, outcome='SUCCESS', result='bad-old-result', error=None)
        with self.assertRaises(StaleExecutionError):
            remote._transport.call('finish', **args)
        with self.assertRaises(WorkerSessionMismatch):
            remote.workers.heartbeat('protocol-worker', first.session_id)
        with self.assertRaises(WorkerSessionMismatch):
            remote.claim_next(first.session_id, 'stale-new-claim')
        # This protocol client did not invoke business code; runtime is already drained.
        remote.workers.confirm_stopped('protocol-worker', first.session_id,
                                      evidence='protocol-only client; no business invocation started')
        self.wait(lambda: self.node('prepare')['task_status'] == 'FAILED')
        # Prepare was non-idempotent. Resolve its known result to repair the DAG.
        incident = next(i for i in self.request('/api/v1/incidents?status=OPEN')['items']
                        if i['task_id'] == task.id)
        self.request('/api/v1/incidents/' + incident['id'] + '/resolve',
            dict(action='succeed', result={'value': 'context', 'host': 'protocol-host'},
                 operator='test-supervisor', reason='known probe result'))
        replacement = self.worker('protocol-worker')
        self.assertNotEqual(replacement.session_id, first.session_id)
        self.wait(lambda: self.request('/api/v1/workflows/flow')['status'] == 'SUCCESS')
        saved = self.request('/api/v1/tasks/' + task.id)
        for _ in range(2):
            with self.assertRaises(StaleExecutionError):
                remote._transport.call('finish', **args)
        self.assertEqual(self.request('/api/v1/tasks/' + task.id), saved)
        self.assertEqual(self.session(replacement)['state'], 'ACTIVE')

    def test_completed_remote_workflow_and_event_history_survive_central_restart(self):
        self.worker()
        self.submit(delay=0)
        self.wait(lambda: self.request('/api/v1/workflows/flow')['status'] == 'SUCCESS')
        snapshot = self.request('/api/v1/workflows/flow')
        task_id = self.node()['task_id']
        history = self.request('/api/v1/tasks/' + task_id + '/events')
        self.kill(self.central)
        self.start_central()
        self.worker('after-restart')
        self.assertEqual(self.request('/api/v1/workflows/flow'), snapshot)
        self.assertEqual(self.request('/api/v1/tasks/' + task_id + '/events'), history)
        self.assertEqual(self.node()['attempt_count'], 1)

    def test_bad_worker_and_management_credentials_cannot_mutate_state(self):
        self.submit(delay=0)
        remote = RemoteTaskStore(self.rpc_url, create_registry(), 'wrong-token')
        with self.assertRaises(RemoteAuthenticationError):
            remote.workers.register('intruder')
        with self.assertRaises(HTTPError) as raised:
            self.request('/api/v1/workflows', dict(id='forbidden'), token='wrong-token')
        self.assertEqual(raised.exception.code, 401)
        self.assertEqual(self.request('/api/v1/sessions')['items'], [])
        self.assertEqual(self.node('prepare')['attempt_count'], 0)
