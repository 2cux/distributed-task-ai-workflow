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


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    handler: Callable = field(repr=False)


class _ToolHandler:
    failure_scope = 'tool'

    def __init__(self, definition):
        self.definition = definition
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
            self.signature.bind(*args, **kwargs)
        except TypeError:
            raise ToolInputError('tool arguments do not match its signature') from None
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

    def register(self, name: str, handler: Callable, *, description: str = '') -> ToolDefinition:
        if not isinstance(name, str) or not name.strip() or name != name.strip() or name == TOOL_HANDLER:
            raise ValueError('tool name must be nonempty, trimmed and not reserved')
        if not callable(handler):
            raise TypeError('tool handler must be callable')
        if not isinstance(description, str):
            raise TypeError('description must be a string')
        definition = ToolDefinition(name, description, handler)
        wrapped = _ToolHandler(definition)
        with self._lock:
            if name in self._handlers:
                raise ValueError('tool name is already registered')
            self._handlers[name] = wrapped
        return definition

    def describe(self) -> list[dict]:
        """JSON catalog; implementation objects and worker credentials stay local."""
        with self._lock:
            return [dict(name=name, description=handler.definition.description)
                    for name, handler in self._handlers.items()]

    def to_task_registry(self) -> dict:
        with self._lock:
            handlers = dict(self._handlers)
        return {**handlers, TOOL_HANDLER: ToolExecutor(handlers)}
