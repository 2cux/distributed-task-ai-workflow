"""Real hard exits, exact host/session attestation and bounded DAG recovery."""
from deployment_test_support import DeploymentTestCase


class WorkerCrashDeploymentTests(DeploymentTestCase):
    def crash(self, idempotent):
        victim = self.worker()
        self.submit(idempotent=idempotent, delay=.8)
        self.running()
        victim.kill()
        victim.wait(timeout=5)
        self.assertNotEqual(victim.returncode, 0)
        self.fenced(victim)
        self.worker('replacement')
        self.unchanged_attempt()
        return victim

    def test_killed_worker_requires_exact_host_proof_before_replacement_runs(self):
        victim = self.crash(True)
        rejected = self.attest(victim, host='wrong-host')
        self.assertNotEqual(rejected.returncode, 0)
        self.unchanged_attempt()
        accepted = self.attest(victim)
        self.assertEqual(accepted.returncode, 0, accepted.stderr)
        self.recovered()
        # Repeating confirmation cannot consume another retry.
        self.assertEqual(self.attest(victim).returncode, 0)
        self.assertEqual((self.node()['attempt_count'], self.node()['retry_count']), (2, 1))

    def test_non_idempotent_crash_stays_uncertain_without_replay(self):
        victim = self.crash(False)
        self.assertEqual(self.attest(victim).returncode, 0)
        self.wait(lambda: self.node()['task_status'] == 'FAILED')
        node = self.node()
        self.assertEqual((node['attempt_count'], node['retry_count']), (1, 0))
        self.assertEqual(self.request('/api/v1/workflows/flow')['status'], 'PAUSED')
        self.assertIn('UncertainExecutionError', str(node['error']))
        incidents = self.request('/api/v1/incidents?status=OPEN')['items']
        incident = next(i for i in incidents if i['task_id'] == node['task_id'])
        self.request('/api/v1/incidents/' + incident['id'] + '/resolve',
                     dict(action='fail', operator='test-operator', reason='accept uncertain failure'))
        self.wait(lambda: self.request('/api/v1/workflows/flow')['status'] == 'FAILED')
        self.assertEqual(self.node('validate')['attempt_count'], 0)
