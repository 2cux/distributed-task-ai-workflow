"""Registry and structured-output contracts; no network or model credentials."""
import json
import unittest
from unittest.mock import patch

from workflow_engine import (
    HTTPConfig, HTTP_HANDLER, LLMConfig, LLM_HANDLER, LLM_JSON_HANDLER,
    LLM_OUTPUT_HANDLER, LLMOutputError, TOOL_HANDLER, ToolNotFoundError,
    ToolRegistry, create_workflow_registry, parse_llm_json,
)


class WorkflowRegistryTests(unittest.TestCase):
    def setUp(self):
        self.config = LLMConfig('test-secret', 'test-model')

    def test_combined_registry_exports_tools_without_exposing_llm_as_a_tool(self):
        tools = ToolRegistry()
        tools.register('echo', lambda value: {'value': value})
        registry = create_workflow_registry(self.config, http_config=HTTPConfig(('https://service.example',)), tools=tools)
        self.assertEqual(set(registry), {LLM_HANDLER, LLM_OUTPUT_HANDLER, LLM_JSON_HANDLER,
                                         HTTP_HANDLER, TOOL_HANDLER, 'echo'})
        self.assertEqual(registry[TOOL_HANDLER]('echo', {'value': 7}), {'value': 7})
        for name in (LLM_HANDLER, LLM_JSON_HANDLER, LLM_OUTPUT_HANDLER, TOOL_HANDLER):
            with self.subTest(name=name), self.assertRaises(ToolNotFoundError):
                registry[TOOL_HANDLER](name, {})
        tools.register('later', lambda: None)
        with self.assertRaises(ToolNotFoundError):
            registry[TOOL_HANDLER]('later', {})

    def test_duplicate_builtin_names_fail_instead_of_overwriting(self):
        for name in (LLM_HANDLER, LLM_JSON_HANDLER, LLM_OUTPUT_HANDLER, HTTP_HANDLER):
            tools = ToolRegistry()
            tools.register(name, lambda: None)
            with self.subTest(name=name), self.assertRaises(ValueError):
                create_workflow_registry(self.config, http_config=HTTPConfig(('https://service.example',)), tools=tools)

    def test_deployment_factory_reads_opt_in_http_configuration(self):
        with patch.dict('os.environ', dict(LLM_API_KEY='test-secret', LLM_MODEL='test-model',
                        HTTP_ALLOWED_ORIGINS='["https://service.example"]',
                        HTTP_DEFAULT_HEADERS='{"Authorization":"Bearer service-secret"}', HTTP_TIMEOUT='3.5'), clear=True):
            registry = create_workflow_registry()
        self.assertIn(HTTP_HANDLER, registry)
        config = HTTPConfig.from_env({'HTTP_ALLOWED_ORIGINS': '["https://service.example"]', 'HTTP_TIMEOUT': '3.5'})
        self.assertEqual(config.timeout, 3.5)
        self.assertEqual(config.allowed_origins, ('https://service.example',))

    def test_http_is_disabled_without_configuration(self):
        with patch.dict('os.environ', {}, clear=True):
            registry = create_workflow_registry(self.config)
        self.assertNotIn(HTTP_HANDLER, registry)
        with self.assertRaises(ToolNotFoundError):
            registry[TOOL_HANDLER](HTTP_HANDLER, {})

    def test_invalid_environment_configuration_does_not_echo_secrets(self):
        for overrides in ({}, {'HTTP_ALLOWED_ORIGINS': 'secret'}, {'HTTP_ALLOWED_ORIGINS': '[]'},
                          {'HTTP_ALLOWED_ORIGINS': '["https://user:secret@service.example"]'},
                          {'HTTP_TIMEOUT': 'nan'}, {'HTTP_MAX_REQUEST_BYTES': '-1'},
                          {'HTTP_DEFAULT_HEADERS': '{secret'}, {'HTTP_DEFAULT_HEADERS': 'null'}):
            env = {'HTTP_ALLOWED_ORIGINS': '["https://service.example"]', **overrides}
            if not overrides:
                env = {}
            with self.subTest(keys=list(overrides)), self.assertRaises(ValueError) as caught:
                HTTPConfig.from_env(env)
            self.assertNotIn('secret', str(caught.exception))


class LLMJSONTests(unittest.TestCase):
    def test_strict_object_parsing_and_required_fields(self):
        value = {'name': HTTP_HANDLER, 'arguments': {'json_body': {'label': '资料', 'items': [1, None]}}}
        result = {'text': json.dumps(value), 'finish_reason': 'stop'}
        self.assertEqual(parse_llm_json(result, required_keys=['name', 'arguments']), value)
        self.assertIsInstance(result['text'], str)

    def test_invalid_ambiguous_truncated_or_incomplete_model_output_is_permanent(self):
        for text, finish in [('secret', 'stop'), ('```json\n{}\n```', 'stop'), ('[]', 'stop'),
                             ('{"v":NaN}', 'stop'), ('{"v":1e999}', 'stop'),
                             ('{"v":1,"v":2}', 'stop'), ('{} trailing-secret', 'stop'),
                             ('{}', 'length'), ('{}', 'stop')]:
            with self.subTest(text=text), self.assertRaises(LLMOutputError) as caught:
                parse_llm_json({'text': text, 'finish_reason': finish}, required_keys=['name'])
            self.assertEqual(caught.exception.failure_category.value, 'permanent')
            self.assertNotIn('secret', str(caught.exception))

    def test_invalid_required_keys_are_rejected(self):
        for keys in ('name', [1], [''], {'name': True}):
            with self.subTest(keys=keys), self.assertRaises(ValueError):
                parse_llm_json({'text': '{}', 'finish_reason': 'stop'}, required_keys=keys)


if __name__ == '__main__':
    unittest.main()
