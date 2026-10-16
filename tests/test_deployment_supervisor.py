"""Supervisor proof guards run without Docker; they do not certify a real engine."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.validate_cross_host import ContainerLab


class StopProofTests(unittest.TestCase):
    def setUp(self):
        self.runtime = dict(Id='exact-container-id', RestartCount=0,
            Config={'Hostname': 'captured-host'}, HostConfig={'RestartPolicy': {'Name': 'no'}},
            State=dict(Running=False, Restarting=False, Pid=0, Status='exited',
                       FinishedAt='2026-10-16T00:00:00Z', ExitCode=137))
        self.session = dict(session_id='exact-session', host_id='captured-host')
        self.lab = ContainerLab.__new__(ContainerLab)
        self.lab.inspect = lambda container: deepcopy(self.runtime)
        self.calls = []
        self.lab.compose = lambda *args: self.calls.append(args)

    def test_live_restarting_or_reused_runtime_cannot_grant_replay(self):
        changes = [dict(Running=True), dict(Restarting=True), dict(Pid=12), dict(Status='paused')]
        for change in changes:
            with self.subTest(change=change):
                original = deepcopy(self.runtime)
                self.runtime['State'].update(change)
                with self.assertRaises(RuntimeError):
                    self.lab.attest_container_exit('exact-container-id', self.session)
                self.assertEqual(self.calls, [])
                self.runtime = original
        self.runtime['RestartCount'] = 1
        with self.assertRaises(RuntimeError):
            self.lab.attest_container_exit('exact-container-id', self.session)
        self.assertEqual(self.calls, [])

    def test_wrong_host_or_automatic_restart_policy_cannot_confirm_session(self):
        self.runtime['Config']['Hostname'] = 'other-host'
        with self.assertRaises(RuntimeError):
            self.lab.attest_container_exit('exact-container-id', self.session)
        self.runtime['Config']['Hostname'] = 'captured-host'
        self.runtime['HostConfig']['RestartPolicy']['Name'] = 'always'
        with self.assertRaises(RuntimeError):
            self.lab.attest_container_exit('exact-container-id', self.session)
        self.assertEqual(self.calls, [])

    def test_stopped_runtime_attestation_binds_exact_container_and_session(self):
        proof = self.lab.attest_container_exit('exact-container-id', self.session)
        self.assertEqual(proof['container_id'], 'exact-container-id')
        self.assertEqual(proof['session_id'], 'exact-session')
        self.assertEqual(proof['exit_code'], 137)
        self.assertEqual(len(self.calls), 1)
        args = self.calls[0]
        self.assertEqual(args[args.index('--session-id') + 1], 'exact-session')
        self.assertEqual(json.loads(args[args.index('--evidence') + 1]), proof)
