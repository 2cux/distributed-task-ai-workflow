"""Restart, overload, malformed-client and recovery-boundary tests over real HTTP."""
import socket
from unittest.mock import patch
import sqlite3
import os
from pathlib import Path
import subprocess
import sys

from management_test_support import ADMIN, READER, ManagementTestCase


class ManagementStabilityTests(ManagementTestCase):
    def test_rejected_uploads_deliver_errors_without_connection_reset(self):
        self.start_service(max_request_bytes=1024)
        cases = [
            (401, 'wrong-token', {}, b' ' * 65536),
            (403, READER, {}, b' ' * 65536),
            (413, ADMIN, {}, b' ' * 65536),
            (415, ADMIN, {'Content-Type': 'text/plain'}, b' ' * 1024),
            (400, ADMIN, {'Transfer-Encoding': 'chunked'}, b' ' * 1024),
        ]
        for _ in range(8):
            for expected, token, headers, body in cases:
                with self.subTest(expected=expected):
                    status, payload, _ = self.call('/api/v1/workflows', raw=body,
                                                   token=token, headers=headers)
                    self.assertEqual(status, expected, payload)
                    self.assertIn('error', payload)
        self.assertEqual(self.call('/api/v1/workflows')[1]['items'], [])
        self.assertEqual(self.call('/api/v1/overview')[1]['attempts'], 0)

    def test_restart_keeps_pause_events_results_and_idempotency(self):
        self.start_service()
        definition = self.workflow('restart')
        self.call('/api/v1/workflows', definition)
        self.call('/api/v1/workflows/restart/pause', dict(operator='ops', reason='deploy'))
        old_events = self.call('/api/v1/workflows/restart/events')[1]
        self.assertTrue(self.service.shutdown(5))
        self.start_service()
        self.assertEqual(self.call('/api/v1/workflows/restart')[1]['status'], 'PAUSED')
        self.assertEqual(self.call('/api/v1/workflows/restart/events')[1], old_events)
        self.assertEqual(self.call('/api/v1/workflows', definition)[0], 202)
        self.worker()
        self.call('/api/v1/workflows/restart/resume', dict(operator='ops', reason='deploy finished'))
        self.wait_for(lambda: self.call('/api/v1/workflows/restart')[1]['status'] == 'SUCCESS')
        for worker in self.engine.worker_registry.list_workers():
            self.call('/api/v1/sessions/' + worker.session_id + '/stop', dict(operator='ops', reason='restart'))
        self.wait_for(lambda: all(row.state == 'STOPPED' for row in self.engine.worker_registry.list_sessions()))
        events = self.call('/api/v1/workflows/restart/events')[1]
        self.assertTrue(self.service.shutdown(5))
        self.start_service({'removed-handler-replacement': abs})
        self.assertEqual(self.call('/api/v1/workflows/restart')[1]['nodes'][0]['result'], 7)
        self.assertEqual(self.call('/api/v1/workflows/restart/events')[1], events)
        self.assertEqual(self.call('/api/v1/overview')[1]['attempts'], 1)

    def test_slow_clients_are_bounded_rejected_and_release_slots_after_timeout(self):
        self.start_service(max_concurrency=2, request_timeout=.4)
        clients = []
        try:
            for _ in range(2):
                client = socket.create_connection(self.service.address, timeout=2)
                clients.append(client)
                client.sendall(b'POST /api/v1/workflows HTTP/1.0\r\nContent-Length: 100\r\n'
                               b'Authorization: Bearer management-tests-admin\r\nContent-Type: application/json\r\n\r\n{')
            self.wait_for(lambda: self.service._server._active == 2, timeout=2)
            self.assertEqual(self.call('/api/v1/workflows')[0], 503)
            self.assertLessEqual(self.service._server._active, 2)
            self.wait_for(lambda: self.service._server._active == 0, timeout=3)
            self.assertEqual(self.call('/api/v1/workflows')[0], 200)
            metrics = self.call('/metrics')[1]
            self.assertIn('workflow_api_overloads_total 1', metrics)
        finally:
            for client in clients:
                client.close()

    def test_shutdown_reports_undrained_handlers_and_can_finish_later(self):
        self.start_service(request_timeout=.8)
        client = socket.create_connection(self.service.address, timeout=2)
        self.addCleanup(client.close)
        client.sendall(b'GET /health/live HTTP/1.0\r\n')  # Leave header parsing blocked.
        self.wait_for(lambda: self.service._server._active == 1)
        self.assertFalse(self.service.shutdown(.05))
        client.close()
        self.assertTrue(self.service.shutdown(3))

    def test_failure_incident_resolution_enforces_idempotency_budget_and_stale_decisions(self):
        calls = []

        def action():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('first invocation failed')
            return 23

        self.start_service({'action': action})
        workflow = dict(id='failed', nodes=[dict(id='n', handler='action', idempotent=True)])
        self.assertEqual(self.call('/api/v1/workflows', workflow)[0], 202)
        self.worker()
        self.wait_for(lambda: bool(self.call('/api/v1/incidents?status=OPEN')[1]['items']))
        incident = self.call('/api/v1/incidents?status=OPEN')[1]['items'][0]
        decision = dict(operator='ops', reason='safe replay', action='retry')
        endpoint = '/api/v1/incidents/' + incident['id'] + '/resolve'
        self.assertEqual(self.call(endpoint, decision)[0], 409)
        self.assertEqual(self.call(endpoint, {**decision, 'extra_retries': 1})[0], 200)
        self.assertEqual(self.call(endpoint, {**decision, 'extra_retries': 1})[0], 409)
        self.wait_for(lambda: self.call('/api/v1/workflows/failed')[1]['status'] == 'SUCCESS')
        run = self.call('/api/v1/workflows/failed')[1]
        self.assertEqual((run['nodes'][0]['result'], run['nodes'][0]['attempt_count'], len(calls)), (23, 2, 2))
        self.assertEqual(run['nodes'][0]['retry_count'], 1)

    def test_network_suspicion_does_not_authorize_replay_or_stop_confirmation(self):
        self.start_service()
        workflow = self.workflow('uncertain')
        workflow['nodes'][0]['max_retries'] = 1
        run = self.call('/api/v1/workflows', workflow)[1]
        now = self.engine._store.workers._now()
        self.engine._store.workers._clock = lambda: now
        # WorkerRegistry actually stores its injected clock in _clock.
        session = self.engine.worker_registry.register('lost', heartbeat_timeout=.1)
        claimed = self.engine._store.claim_next(session.session_id, 'old-owner')
        self.assertIsNotNone(claimed)
        self.engine._store.workers._clock = lambda: now + 10
        self.wait_for(lambda: bool(self.call('/api/v1/incidents?status=OPEN')[1]['items']))
        incident = self.call('/api/v1/incidents?status=OPEN')[1]['items'][0]
        self.assertEqual(self.call('/api/v1/incidents/' + incident['id'] + '/resolve',
                                  dict(action='retry', extra_retries=1, operator='ops', reason='only timeout observed'))[0], 409)
        task = self.call('/api/v1/tasks/' + run['nodes'][0]['task_id'])[1]
        self.assertEqual((task['status'], task['attempt_count'], task['retry_count']), ('RUNNING', 1, 0))
        operator = dict(operator='ops', reason='manual hold released; incident still pending')
        self.call('/api/v1/workflows/uncertain/pause', operator)
        self.assertEqual(self.call('/api/v1/workflows/uncertain/resume', operator)[1]['status'], 'PAUSED')
        self.assertEqual(self.call('/api/v1/sessions/' + session.session_id + '/confirm-stopped',
                                  dict(operator='ops', evidence='timeout'))[0], 405)

    def test_non_idempotent_failure_requires_reconciliation_instead_of_retry(self):
        def failure():
            raise RuntimeError('external effect may already exist')

        self.start_service({'failure': failure})
        self.call('/api/v1/workflows', dict(id='non-idempotent', nodes=[dict(id='n', handler='failure')]))
        self.worker()
        self.wait_for(lambda: bool(self.call('/api/v1/incidents?status=OPEN')[1]['items']))
        incident = self.call('/api/v1/incidents?status=OPEN')[1]['items'][0]
        endpoint = '/api/v1/incidents/' + incident['id'] + '/resolve'
        self.assertEqual(self.call(endpoint, dict(action='retry', extra_retries=10, operator='ops', reason='budget only'))[0], 409)
        self.assertEqual(self.call(endpoint, dict(action='succeed', result={'verified': True},
                         operator='ops', reason='external result reconciled'))[0], 200)
        run = self.call('/api/v1/workflows/non-idempotent')[1]
        self.assertEqual((run['status'], run['nodes'][0]['result'], run['nodes'][0]['attempt_count']),
                         ('SUCCESS', {'verified': True}, 1))

    def test_killed_control_plane_process_preserves_accepted_submission(self):
        marker = self.path.parent / 'service-url.txt'
        script = '''
from pathlib import Path
import sys, time
from workflow_engine import TaskEngine, RetryPolicy
engine = TaskEngine(database_path=sys.argv[1], task_registry={'abs': abs}, retry_policy=RetryPolicy(0))
service = engine.create_management_service('management-tests-admin', poll_interval=.02).start()
Path(sys.argv[2]).write_text(service.url, encoding='utf-8')
while True:
    time.sleep(.1)
'''
        process = subprocess.Popen([sys.executable, '-c', script, str(self.path), str(marker)],
            env={**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def stop_process():
            if process.poll() is None:
                process.kill()
            process.wait(5)

        self.addCleanup(stop_process)
        self.wait_for(lambda: marker.exists() and marker.stat().st_size > 0, timeout=5)
        # Only a URL is needed for the fixture's external HTTP client.
        from types import SimpleNamespace
        self.service = SimpleNamespace(url=marker.read_text(encoding='utf-8'))
        accepted = self.submit('before-crash')
        stop_process()
        self.start_service()
        restored = self.call('/api/v1/workflows/before-crash')[1]
        self.assertEqual(restored['nodes'][0]['task_id'], accepted['nodes'][0]['task_id'])
        self.submit('before-crash')
        self.worker()
        self.wait_for(lambda: self.call('/api/v1/workflows/before-crash')[1]['status'] == 'SUCCESS')
        self.assertEqual(self.call('/api/v1/overview')[1]['attempts'], 1)

    def test_storage_failure_returns_unavailable_without_leaking_sql_or_disrupting_next_request(self):
        self.start_service()
        with patch.object(self.service.queries, 'get', side_effect=sqlite3.OperationalError('private db path')):
            status, body, _ = self.call('/api/v1/workflows/anything')
        self.assertEqual(status, 503)
        self.assertNotIn('private db path', str(body))
        self.submit('healthy')
        self.assertEqual(self.call('/api/v1/workflows/healthy')[0], 200)


if __name__ == '__main__':
    import unittest
    unittest.main()
