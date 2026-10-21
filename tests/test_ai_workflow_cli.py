"""Independent subprocess CLI tests with actual local model and HTTP requests."""
import json
import os
import subprocess
import sys
import unittest

from ai_workflow_test_support import MixedWorkflowFixture, ROOT, MODEL_SECRET, HTTP_SECRET


class MixedCLITests(MixedWorkflowFixture):
    def invoke(self, *, origins=True, environ=None, database='cli.db', prompt=None):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('LLM_', 'APP_LLM_', 'HTTP_'))}
        env.update(PYTHONPATH=str(ROOT / 'src'), LLM_API_KEY=MODEL_SECRET,
                   LLM_MODEL='fixture-model', LLM_BASE_URL=self.origin + '/v1',
                   HTTP_DEFAULT_HEADERS=json.dumps({'Authorization': 'Bearer ' + HTTP_SECRET}))
        env.update(environ or {})
        command = [sys.executable, '-m', 'workflow_engine.llm_cli', '--database', str(self.directory / database)]
        if prompt is None:
            path = self.directory / 'workflow.json'
            path.write_text(json.dumps(self.workflow().to_dict()), encoding='utf-8')
            command += ['--workflow', str(path)]
        else:
            command += ['--prompt', prompt]
        if origins:
            command += ['--http-allowed-origin', self.origin]
        return subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True,
                              encoding='utf-8', timeout=25)

    def test_cli_runs_serialized_mixed_dag(self):
        process = self.invoke()
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        run = json.loads(process.stdout)
        self.assertEqual(run['status'], 'SUCCESS')
        self.assertEqual(run['llm_usage']['call_count'], 2)
        self.assertEqual(run['nodes']['check']['result']['text'], 'Action completed for item-7')
        self.assertEqual(self.effects, [{'id': 'item-7'}])
        self.assertNotIn(MODEL_SECRET, process.stdout + process.stderr)
        self.assertNotIn(HTTP_SECRET, process.stdout + process.stderr)

    def test_environment_origins_and_explicit_cli_override(self):
        process = self.invoke(origins=False, environ={'HTTP_ALLOWED_ORIGINS': json.dumps([self.origin])})
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        process = self.invoke(environ={'HTTP_ALLOWED_ORIGINS': '["https://other.example"]'}, database='override.db')
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        self.assertEqual(len(self.effects), 2)

    def test_http_requires_configuration_before_submission(self):
        process = self.invoke(origins=False)
        self.assertEqual(process.returncode, 2)
        self.assertIn('http.request.v1', process.stderr)
        self.assertEqual(self.calls, [])

    def test_prompt_mode_remains_compatible_without_http(self):
        process = self.invoke(origins=False, prompt='Summarize')
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        self.assertEqual(json.loads(process.stdout)['llm_usage']['call_count'], 1)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.effects, [])


if __name__ == '__main__':
    unittest.main()
