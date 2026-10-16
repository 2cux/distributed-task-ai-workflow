"""Opt-in isolated environments. Each test builds a separate temporary stack."""
import os
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.validate_cross_host import ContainerLab


@unittest.skipUnless(os.environ.get('WORKFLOW_CONTAINER_TESTS') == '1',
                     'set WORKFLOW_CONTAINER_TESTS=1 with a running Docker Engine')
class IsolatedContainerDeploymentTests(unittest.TestCase):
    def scenario(self, name):
        lab = ContainerLab(os.environ.get('WORKFLOW_DOCKER') or None)
        self.addCleanup(lab.close)
        result = lab.run(name)
        self.assertEqual(result['status'], 'passed')

    def test_real_network_partition_and_reconnection(self):
        self.scenario('network')

    def test_sigkill_and_supervised_recovery(self):
        self.scenario('crash')

    def test_old_session_cannot_commit_after_replacement(self):
        self.scenario('stale')

    def test_non_idempotent_crash_never_replays(self):
        self.scenario('non-idempotent')
