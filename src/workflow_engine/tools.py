"""Explicit tool registration and JSON calls on the existing task execution path.

Registration is trusted deployment code. Neither a workflow nor a model response
can import code or add a tool. Exported registries are frozen deployment snapshots.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import json
from threading import RLock
from typing import Callable

from .failures import FailureCategory
from .persistence import PersistenceError, _json


TOOL_HANDLER = 'tool.call.v1'


class ToolError(RuntimeError):
    """A tool may have performed work; automatic replay is unsafe."""

    failure_category = FailureCategory.UNCERTAIN


class ToolInputError(ToolError):
    failure_category = FailureCategory.PERMANENT


class ToolNotFoundError(ToolInputError):
    pass


class ToolTransientError(ToolError):
    """An adapter explicitly knows this failure is safe to retry."""

    failure_category = FailureCategory.TRANSIENT


class ToolResultError(ToolError):
    pass


class ToolExecutionError(ToolError):
    pass


def _snapshot(value, error_class, message):
    try:
        return json.loads(_json(value))
    except PersistenceError:
        raise error_class(message) from None


_SCHEMA_TYPES = {
    'object': lambda value: type(value) is dict,
    'array': lambda value: type(value) is list,
    'string': lambda value: type(value) is str,
    'boolean': lambda value: type(value) is bool,
    'null': lambda value: value is None,
    'number': lambda value: type(value) in (int, float),
    'integer': lambda value: type(value) is int or type(value) is float and value.is_integer(),
}
_SCHEMA_KEYWORDS = {'type', 'properties', 'required', 'additionalProperties', 'items',
                    'enum', 'description', 'minimum', 'maximum',
                    'minLength', 'maxLength', 'minItems', 'maxItems'}


def _json_equal(left, right):
    # Python considers True == 1, including inside containers; JSON Schema doesn't.
    if type(left) in (int, float) and type(right) in (int, float):
        return left == right
    if type(left) is not type(right):
        return False
    if type(left) is list:
        return len(left) == len(right) and all(_json_equal(a, b) for a, b in zip(left, right))
    if type(left) is dict:
        return left.keys() == right.keys() and all(_json_equal(left[k], right[k]) for k in left)
    return left == right


def _check_schema(schema):
    """A deliberately small, local-only JSON Schema subset; fail closed."""
    def invalid():
        raise ValueError('invalid or unsupported tool parameters schema')

    if type(schema) is not dict or schema.keys() - _SCHEMA_KEYWORDS:
        invalid()
    if 'type' in schema and (type(schema['type']) is not str or schema['type'] not in _SCHEMA_TYPES):
        invalid()
    if 'description' in schema and type(schema['description']) is not str:
        invalid()
    if 'properties' in schema:
        if type(schema['properties']) is not dict:
            invalid()
        for child in schema['properties'].values():
            _check_schema(child)
    if 'required' in schema:
        required = schema['required']
        if (type(required) is not list or any(type(name) is not str for name in required)
                or len(set(required)) != len(required)):
            invalid()
    if 'additionalProperties' in schema and type(schema['additionalProperties']) is not bool:
        invalid()
    if 'items' in schema:
        _check_schema(schema['items'])
    if 'enum' in schema:
        values = schema['enum']
        if (type(values) is not list or not values or any(
                _json_equal(value, previous) for i, value in enumerate(values) for previous in values[:i])):
            invalid()
    for key in ('minimum', 'maximum'):
        if key in schema and type(schema[key]) not in (int, float):
            invalid()
    for key in ('minLength', 'maxLength', 'minItems', 'maxItems'):
        if key in schema and (type(schema[key]) is not int or schema[key] < 0):
            invalid()


def _matches_schema(value, schema):
    if 'type' in schema and not _SCHEMA_TYPES[schema['type']](value):
        return False
    if 'enum' in schema and not any(_json_equal(value, allowed) for allowed in schema['enum']):
        return False
    if type(value) is dict:
        properties = schema.get('properties', {})
        if any(name not in value for name in schema.get('required', [])):
            return False
        if schema.get('additionalProperties') is False and value.keys() - properties.keys():
            return False
        return all(_matches_schema(child, properties[name]) for name, child in value.items() if name in properties)
    if type(value) is list and 'items' in schema:
        if not all(_matches_schema(child, schema['items']) for child in value):
            return False
    if type(value) in (int, float):
        if (('minimum' in schema and value < schema['minimum']) or
                ('maximum' in schema and value > schema['maximum'])):
            return False
    if type(value) in (str, list):
        lower, upper = ('minLength', 'maxLength') if type(value) is str else ('minItems', 'maxItems')
        if ((lower in schema and len(value) < schema[lower]) or
                (upper in schema and len(value) > schema[upper])):
            return False
    return True


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    handler: Callable = field(repr=False)
    parameters_schema: dict | None = field(default=None, repr=False)


class _ToolHandler:
    failure_scope = 'tool'

    def __init__(self, definition):
        self.definition = definition
        self._parameters_schema = _snapshot(definition.parameters_schema, ValueError,
                                            'tool parameters schema must be strict JSON')
        if self._parameters_schema is not None:
            _check_schema(self._parameters_schema)
            if self._parameters_schema.get('type') != 'object':
                raise ValueError('tool parameters schema must have type object')
        if (inspect.iscoroutinefunction(definition.handler) or
                inspect.iscoroutinefunction(getattr(definition.handler, '__call__', None))):
            raise TypeError('tool handler must be synchronous')
        try:
            self.signature = inspect.signature(definition.handler)
        except (TypeError, ValueError):
            raise ValueError('tool handler must have an inspectable signature') from None

    def __call__(self, *args, **kwargs):
        # Detach both boundaries: tools cannot mutate submitted input or return
        # a live object that changes after the task has succeeded.
        arguments = _snapshot(dict(args=list(args), kwargs=kwargs), ToolInputError,
                              'tool arguments must be strict JSON')
        args, kwargs = arguments['args'], arguments['kwargs']
        try:
            bound = self.signature.bind(*args, **kwargs)
        except TypeError:
            raise ToolInputError('tool arguments do not match its signature') from None
        if self._parameters_schema is not None:
            # Validate supplied arguments by parameter name for static positional
            # calls as well as dynamic keyword calls. Never serialize defaults.
            supplied = {}
            for name, value in bound.arguments.items():
                kind = self.signature.parameters[name].kind
                if kind is inspect.Parameter.VAR_KEYWORD:
                    if supplied.keys() & value.keys():
                        raise ToolInputError('tool arguments cannot be represented by its schema')
                    supplied.update(value)
                else:
                    supplied[name] = list(value) if kind is inspect.Parameter.VAR_POSITIONAL else value
            if not _matches_schema(supplied, self._parameters_schema):
                raise ToolInputError('tool arguments do not match its parameters schema')
        try:
            result = self.definition.handler(*args, **kwargs)
        except ToolError:
            raise
        except Exception:
            # Arbitrary adapter messages can contain credentials, URLs or body
            # data. Adapters should raise a sanitized ToolError explicitly.
            raise ToolExecutionError('tool execution failed; outcome may be unknown') from None
        return _snapshot(result, ToolResultError, 'tool result must be strict JSON; outcome may be unknown')


class ToolExecutor:
    """Dispatch a model/planner-selected name only within an exported snapshot."""

    failure_scope = 'tool'

    def __init__(self, handlers):
        if not isinstance(handlers, dict) or any(
                not isinstance(name, str) or not name.strip() or name == TOOL_HANDLER or not callable(handler)
                for name, handler in handlers.items()):
            raise TypeError('handlers must map registered tool names to synchronous callables')
        self._handlers = {name: handler if isinstance(handler, _ToolHandler) else
                          _ToolHandler(ToolDefinition(name, '', handler)) for name, handler in handlers.items()}

    def __call__(self, tool_name: str, arguments: dict):
        if not isinstance(tool_name, str) or not tool_name.strip():
            raise ToolInputError('tool_name must be a nonempty string')
        if type(arguments) is not dict:
            raise ToolInputError('arguments must be a JSON object')
        try:
            handler = self._handlers[tool_name]
        except KeyError:
            raise ToolNotFoundError('tool is not registered') from None
        return handler(**arguments)


class ToolRegistry:
    """Register stable names, then export a dict for TaskEngine or RemoteWorker.

    Each exported name owns a distinct callable, satisfying persistent task
    identity even when the same implementation is registered under two names.
    Changing registration later does not change an already exported dispatcher.
    """

    def __init__(self):
        self._handlers = {}
        self._lock = RLock()

    def register(self, name: str, handler: Callable, *, description: str = '',
                 parameters_schema: dict | None = None) -> ToolDefinition:
        if not isinstance(name, str) or not name.strip() or name != name.strip() or name == TOOL_HANDLER:
            raise ValueError('tool name must be nonempty, trimmed and not reserved')
        if not callable(handler):
            raise TypeError('tool handler must be callable')
        if not isinstance(description, str):
            raise TypeError('description must be a string')
        definition = ToolDefinition(name, description, handler,
                                    _snapshot(parameters_schema, ValueError,
                                              'tool parameters schema must be strict JSON'))
        wrapped = _ToolHandler(definition)
        with self._lock:
            if name in self._handlers:
                raise ValueError('tool name is already registered')
            self._handlers[name] = wrapped
        return definition

    def describe(self) -> list[dict]:
        """JSON catalog; implementation objects and worker credentials stay local."""
        with self._lock:
            catalog = []
            for name, handler in self._handlers.items():
                entry = dict(name=name, description=handler.definition.description)
                if handler._parameters_schema is not None:
                    entry['parameters_schema'] = _snapshot(handler._parameters_schema, ValueError,
                                                           'tool parameters schema must be strict JSON')
                catalog.append(entry)
            return catalog

    def to_task_registry(self) -> dict:
        with self._lock:
            handlers = dict(self._handlers)
        return {**handlers, TOOL_HANDLER: ToolExecutor(handlers)}
