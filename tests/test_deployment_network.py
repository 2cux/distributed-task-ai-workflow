"""Run independently: unittest discover -s tests -p test_deployment_network.py -v."""
import os

from deployment_test_support import DeploymentTestCase, ROOT


class NetworkPartitionTests(DeploymentTestCase):
    def test_partition_keeps_center_alive_and_reconnect_cannot_revive_old_session(self):
        link = self.link()
        release = self.directory / 'old-invocation-release'
        env = dict(self.env, WORKFLOW_TEST_RELEASE=str(release),
                   PYTHONPATH=os.pathsep.join((str(ROOT / 'src'), str(ROOT / 'tests'))))
        victim = self.worker(server=link.url, env=env,
                             registry='deployment_test_handlers:create_registry')
        self.submit(delay=0)
        self.running()
        self.wait(release.with_suffix('.entered').exists)
        link.blocked.set()
        self.fenced(victim)
        self.worker('replacement')
        self.assertIsNone(victim.poll(), 'old business invocation must still be alive')
        self.request('/health/ready')  # service was not shut down to simulate the fault
        for _ in range(3):
            self.unchanged_attempt()
        incidents = self.request('/api/v1/incidents?status=OPEN')['items']
        self.assertTrue(any(i['kind'] == 'WAIT_STOP' and i['session_id'] == victim.session_id for i in incidents))
        link.blocked.clear()
        release.touch()  # Explicitly release the exact invocation after reconnect.
        self.wait(lambda: victim.poll() is not None)
        self.assertEqual(victim.returncode, 1)
        self.recovered()
        self.assertEqual(self.session(victim)['state'], 'STOPPED')
        history = self.request('/api/v1/sessions/' + victim.session_id + '/events')['items']
        states = [row['state'] for row in history]
        self.assertIn('FENCED', states)
        self.assertNotIn('ACTIVE', states[states.index('FENCED') + 1:])

    def test_link_stays_down_after_business_drain_until_supervisor_attests_exit(self):
        link = self.link()
        victim = self.worker(server=link.url)
        self.submit(delay=1.5)
        self.running()
        link.blocked.set()
        self.fenced(victim)
        self.worker('replacement')
        self.wait(lambda: victim.poll() is not None)
        self.unchanged_attempt()
        result = self.attest(victim)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.recovered()
