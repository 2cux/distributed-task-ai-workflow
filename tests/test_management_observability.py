"""Independent monitoring tests, including labels, redaction and restart totals."""
import json
from threading import Event
from unittest.mock import patch

from management_test_support import ADMIN, READER, ManagementTestCase


class ManagementObservabilityTests(ManagementTestCase):
    def test_metrics_track_http_latency_and_persisted_execution(self):
        self.start_service()
        self.submit('sensitive-business-id')
        worker = self.worker()
        self.wait_for(lambda: self.call('/api/v1/workflows/sensitive-business-id')[1]['status'] == 'SUCCESS')
        self.assertTrue(worker.shutdown(5))
        overview = self.call('/api/v1/overview', token=READER)[1]
        self.assertEqual(overview['tasks']['SUCCESS'], 1)
        self.assertEqual((overview['attempts'], overview['retries'], overview['dispatchable_tasks']), (1, 0, 0))
        self.call('/api/v1/workflows', token='wrong-token')
        metrics = self.call('/metrics', token=READER)[1]
        self.assertIn('workflow_engine_tasks{status="SUCCESS"} 1', metrics)
        self.assertIn('workflow_engine_attempts 1', metrics)
        self.assertIn('# TYPE workflow_api_request_duration_seconds histogram', metrics)
        self.assertIn('route="/api/v1/workflows/{id}"', metrics)
        self.assertIn('status="401"', metrics)
        self.assertNotIn('sensitive-business-id', metrics)
        self.assertNotIn(ADMIN, metrics)
        self.assertNotIn(READER, metrics)
        counts = [float(line.rsplit(' ', 1)[1]) for line in metrics.splitlines()
                  if line.startswith('workflow_api_request_duration_seconds_bucket{method="GET",route="/api/v1/overview"')]
        self.assertTrue(counts)
        self.assertEqual(counts, sorted(counts))
        self.assertEqual(counts[-1], 1)

    def test_structured_logs_correlate_errors_without_body_token_or_raw_path(self):
        self.start_service()
        with self.assertLogs('workflow_engine.management', level='INFO') as captured:
            self.call('/api/v1/workflows/private-customer?secret=private-value',
                      token=ADMIN, headers={'X-Request-ID': 'correlation-1'})
            self.call('/api/v1/workflows', dict(id='sensitive-body', nodes=[]))
        logs = '\n'.join(captured.output)
        record = json.loads(captured.records[0].getMessage())
        self.assertEqual(record['request_id'], 'correlation-1')
        self.assertEqual(record['route'], '/api/v1/workflows/{id}')
        self.assertEqual(record['status'], 400)
        for private in (ADMIN, READER, 'private-customer', 'private-value', 'sensitive-body'):
            self.assertNotIn(private, logs)

    def test_readiness_recovers_after_coordinator_failure_liveness_stays_available(self):
        self.start_service()
        with patch.object(self.service.recovery, 'tick', side_effect=RuntimeError('private-error')):
            self.wait_for(lambda: self.call('/health/ready', token=None)[0] == 503)
            self.assertEqual(self.call('/health/live', token=None)[0], 200)
        self.wait_for(lambda: self.call('/health/ready', token=None)[0] == 200)
        metrics = self.call('/metrics')[1]
        count = next(line for line in metrics.splitlines() if line.startswith('workflow_coordinator_errors_total '))
        self.assertGreaterEqual(int(count.split()[-1]), 1)
        self.assertNotIn('private-error', metrics)

    def test_paused_and_delayed_work_are_not_reported_dispatchable(self):
        self.start_service()
        import time
        delayed = self.workflow('delayed')
        delayed['nodes'][0]['scheduled_at'] = time.time() + 60
        self.assertEqual(self.call('/api/v1/workflows', delayed)[0], 202)
        self.submit('paused')
        self.call('/api/v1/workflows/paused/pause', dict(operator='ops', reason='hold'))
        overview = self.call('/api/v1/overview')[1]
        self.assertEqual(overview['tasks']['PENDING'], 2)
        self.assertEqual(overview['workflows']['PAUSED'], 1)
        self.assertEqual(overview['dispatchable_tasks'], 0)

    def test_readiness_detects_a_stalled_coordinator(self):
        self.start_service(coordinator_stale_after=.15)
        entered, release = Event(), Event()
        original = self.service.recovery.tick

        def blocked():
            entered.set()
            release.wait(3)
            return original()

        try:
            with patch.object(self.service.recovery, 'tick', side_effect=blocked):
                self.assertTrue(entered.wait(2))
                self.wait_for(lambda: self.call('/health/ready', token=None)[0] == 503)
                self.assertEqual(self.call('/health/live', token=None)[0], 200)
                release.set()
            self.wait_for(lambda: self.call('/health/ready', token=None)[0] == 200)
        finally:
            release.set()


if __name__ == '__main__':
    import unittest
    unittest.main()
