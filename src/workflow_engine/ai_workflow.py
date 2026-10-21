"""Compose existing LLM, HTTP and tool handlers for durable DAG execution."""
from __future__ import annotations

import json
import os

from .http_tool import HTTPConfig, HTTP_HANDLER, create_http_registry
from .llm import LLMConfig, LLMOutputError, create_llm_registry, validate_llm_output
from .tools import TOOL_HANDLER, ToolExecutor, ToolRegistry


LLM_JSON_HANDLER = 'llm.parse_json.v1'


def parse_llm_json(result: dict, *, required_keys: list[str] | None = None) -> dict:
    """Turn a completed model answer into a JSON object in a separate DAG task.

    No repair, code fences, nonfinite numbers or ambiguous duplicate keys. Invalid
    output fails permanently before downstream effects; the model result survives.
    """
    if required_keys is not None and (type(required_keys) is not list or any(
            not isinstance(key, str) or not key for key in required_keys)):
        raise ValueError('required_keys must be a list of nonempty strings')
    validate_llm_output(result)

    def reject_constant(value):
        raise ValueError('nonfinite JSON')

    def object_pairs(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value

    try:
        value = json.loads(result['text'], parse_constant=reject_constant, object_pairs_hook=object_pairs)
        # Large exponents can overflow even though JSON syntax is valid.
        json.dumps(value, allow_nan=False)
    except (ValueError, TypeError, RecursionError, OverflowError):
        raise LLMOutputError('LLM output must be a strict JSON object') from None
    if type(value) is not dict:
        raise LLMOutputError('LLM output must be a JSON object')
    if required_keys and any(key not in value for key in required_keys):
        raise LLMOutputError('LLM JSON output is missing required keys')
    return value


parse_llm_json.failure_scope = 'llm_validation'


def create_workflow_registry(llm_config: LLMConfig | None = None, *,
                             http_config: HTTPConfig | None = None,
                             tools: ToolRegistry | None = None) -> dict:
    """Unified registry for TaskEngine, management and remote-worker factories.

    LLM settings default to LLM_*/APP_LLM_* environment variables. HTTP is opt-in
    through HTTPConfig or HTTP_ALLOWED_ORIGINS (a JSON array). Only registered
    tools, including HTTP when configured, enter the dynamic tool dispatcher.
    """
    if tools is not None and not isinstance(tools, ToolRegistry):
        raise TypeError('tools must be ToolRegistry or None')
    handlers = create_llm_registry(llm_config)
    handlers[LLM_JSON_HANDLER] = parse_llm_json
    tool_handlers = tools.to_task_registry() if tools is not None else {}
    tool_handlers.pop(TOOL_HANDLER, None)
    if http_config is None and 'HTTP_ALLOWED_ORIGINS' in os.environ:
        http_config = HTTPConfig.from_env()
    if http_config is not None:
        if HTTP_HANDLER in tool_handlers:
            raise ValueError('HTTP handler is already registered in tools')
        tool_handlers[HTTP_HANDLER] = create_http_registry(http_config)[HTTP_HANDLER]
    if handlers.keys() & tool_handlers.keys():
        raise ValueError('tool names conflict with built-in workflow handlers')
    return {**handlers, **tool_handlers, TOOL_HANDLER: ToolExecutor(tool_handlers)}
