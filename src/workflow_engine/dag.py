"""Serializable DAG definitions and detached runtime snapshots.

Handlers are registry names, never executable code from workflow input. OutputRef
binds a predecessor's JSON output to a downstream keyword argument, making model,
retrieval, tool and validation calls ordinary independently retryable tasks.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Any
from uuid import uuid4

from .persistence import _json
from .task import Task
from .llm_usage import LLMBudget


def _definition_object(value, allowed, required):
    if (type(value) is not dict or value.keys() - set(allowed) or
            set(required) - value.keys()):
        raise ValueError('definition object has missing or unsupported fields')


def _definition_array(value, name):
    if type(value) is not list:
        raise TypeError(f'{name} must be an array')
    return value


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PAUSED = "PAUSED"


class NodeStatus(str, Enum):
    WAITING = "WAITING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class OutputRef:
    node_id: str
    path: tuple[str | int, ...] = ()

    def __post_init__(self):
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValueError("OutputRef.node_id must be nonempty")
        if not isinstance(self.path, tuple) or any(type(p) not in (str, int) for p in self.path):
            raise ValueError("OutputRef.path must contain string keys or integer indices")

    @classmethod
    def from_dict(cls, value: dict) -> OutputRef:
        _definition_object(value, ('node_id', 'path'), ('node_id',))
        return cls(value['node_id'], tuple(_definition_array(value.get('path', []), 'output path')))


@dataclass(frozen=True)
class InputBinding:
    """Bind an output into a keyword argument, optionally inside a JSON body.

    target=('json_body', 'items', 0, 'id') updates that leaf. Parent containers
    must exist in static kwargs; a dictionary leaf may be new. Lists never grow.
    """

    target: tuple[str | int, ...]
    source: OutputRef

    def __post_init__(self):
        if (not isinstance(self.target, tuple) or not self.target or
                type(self.target[0]) is not str or not self.target[0] or
                any(type(part) not in (str, int) or type(part) is int and part < 0
                    for part in self.target)):
            raise ValueError('binding target must start with a keyword and contain keys or nonnegative indices')
        if not isinstance(self.source, OutputRef):
            raise TypeError('binding source must be OutputRef')

    @classmethod
    def from_dict(cls, value: dict) -> InputBinding:
        _definition_object(value, ('target', 'source'), ('target', 'source'))
        return cls(tuple(_definition_array(value['target'], 'binding target')),
                   OutputRef.from_dict(value['source']))


def _path_value(value, path):
    for part in path:
        if type(part) is int and isinstance(value, list) and part >= 0:
            value = value[part]
        elif type(part) is str and isinstance(value, dict):
            value = value[part]
        else:
            raise ValueError('output or binding path does not match its JSON container')
    return value


@dataclass(frozen=True)
class DAGNode:
    id: str
    handler: str
    depends_on: tuple[str, ...] = ()
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, OutputRef] = field(default_factory=dict)
    priority: int = 0
    max_retries: int | None = None
    timeout: float | None = None
    idempotent: bool = False
    scheduled_at: float | None = None
    bindings: tuple[InputBinding, ...] = ()

    def __post_init__(self):
        if not isinstance(self.handler, str) or not self.handler:
            raise ValueError("handler must be a nonempty registry name")
        if not isinstance(self.depends_on, tuple) or any(
                not isinstance(key, str) or not key for key in self.depends_on):
            raise ValueError("depends_on must be a tuple of nonempty node IDs")
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError("duplicate dependencies")
        if not isinstance(self.inputs, dict) or any(
                not isinstance(key, str) or not isinstance(ref, OutputRef)
                for key, ref in self.inputs.items()):
            raise TypeError("inputs must map keyword names to OutputRef")
        # Reuse the task contract for timeout, retry, priority and JSON validation.
        Task(self.id, lambda: None, id=self.id, args=self.args, kwargs=self.kwargs,
             priority=self.priority, max_retries=self.max_retries, timeout=self.timeout,
             idempotent=self.idempotent, scheduled_at=self.scheduled_at)
        _json(list(self.args))
        _json(self.kwargs)
        if self.inputs.keys() & self.kwargs.keys():
            raise ValueError("inputs cannot overwrite static kwargs")
        if not isinstance(self.bindings, tuple) or any(not isinstance(b, InputBinding) for b in self.bindings):
            raise TypeError('bindings must be a tuple of InputBinding')
        targets = [(key,) for key in self.inputs]
        for binding in self.bindings:
            target = binding.target
            if any(target[:len(other)] == other or other[:len(target)] == target for other in targets):
                raise ValueError('input bindings must not overlap')
            targets.append(target)

    @property
    def dependencies(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.depends_on, *(ref.node_id for ref in self.inputs.values()),
                                    *(binding.source.node_id for binding in self.bindings))))

    def materialize_kwargs(self, outputs: dict[str, Any]) -> dict:
        """Resolve detached JSON inputs before opening the durable dispatch gate."""
        kwargs = json.loads(_json(self.kwargs))
        for argument, reference in self.inputs.items():
            kwargs[argument] = json.loads(_json(_path_value(outputs[reference.node_id], reference.path)))
        for binding in self.bindings:
            value = _path_value(outputs[binding.source.node_id], binding.source.path)
            parent = _path_value(kwargs, binding.target[:-1])
            leaf = binding.target[-1]
            if type(leaf) is int and isinstance(parent, list):
                # Assignment validates bounds; negative indices were rejected.
                parent[leaf] = json.loads(_json(value))
            elif type(leaf) is str and isinstance(parent, dict):
                parent[leaf] = json.loads(_json(value))
            else:
                raise ValueError('binding target does not match its JSON container')
        return kwargs

    def to_dict(self) -> dict:
        value = dict(id=self.id, handler=self.handler, depends_on=list(self.depends_on),
                    args=list(self.args), kwargs=self.kwargs,
                    inputs={key: dict(node_id=ref.node_id, path=list(ref.path))
                            for key, ref in self.inputs.items()}, priority=self.priority,
                    max_retries=self.max_retries, timeout=self.timeout,
                    idempotent=self.idempotent, scheduled_at=self.scheduled_at)
        # Keep existing workflows' canonical definitions unchanged.
        if self.bindings:
            value['bindings'] = [dict(target=list(binding.target), source=dict(
                node_id=binding.source.node_id, path=list(binding.source.path))) for binding in self.bindings]
        return value

    @classmethod
    def from_dict(cls, value: dict) -> DAGNode:
        _definition_object(value, ('id', 'handler', 'depends_on', 'args', 'kwargs', 'inputs',
                                  'priority', 'max_retries', 'timeout', 'idempotent', 'scheduled_at',
                                  'bindings'), ('id', 'handler'))
        value = dict(value)
        value['args'] = tuple(_definition_array(value.get('args', []), 'args'))
        value['depends_on'] = tuple(_definition_array(value.get('depends_on', []), 'depends_on'))
        for field in ('inputs', 'kwargs'):
            if field in value and type(value[field]) is not dict:
                raise TypeError(f'{field} must be an object')
        value['inputs'] = {key: OutputRef.from_dict(ref)
                           for key, ref in value.get('inputs', {}).items()}
        value['bindings'] = tuple(InputBinding.from_dict(binding)
            for binding in _definition_array(value.get('bindings', []), 'bindings'))
        return cls(**value)


@dataclass(frozen=True)
class DAGWorkflow:
    nodes: tuple[DAGNode, ...]
    id: str = field(default_factory=lambda: uuid4().hex)
    name: str = "workflow"
    llm_budget: LLMBudget | None = None

    def __post_init__(self):
        if self.llm_budget is not None and not isinstance(self.llm_budget, LLMBudget):
            raise TypeError('llm_budget must be LLMBudget or None')
        if not isinstance(self.id, str) or not self.id or not isinstance(self.name, str) or not self.name:
            raise ValueError("workflow ID and name must be nonempty")
        if not isinstance(self.nodes, tuple) or not self.nodes or any(
                not isinstance(node, DAGNode) for node in self.nodes):
            raise ValueError("nodes must be a nonempty tuple of DAGNode")
        self.topological_order()

    def topological_order(self) -> tuple[str, ...]:
        nodes = {node.id: node for node in self.nodes}
        if len(nodes) != len(self.nodes):
            raise ValueError("duplicate node IDs")
        degrees = {key: len(node.dependencies) for key, node in nodes.items()}
        successors = {key: [] for key in nodes}
        for node in self.nodes:
            for dependency in node.dependencies:
                if dependency not in nodes:
                    raise ValueError(f"unknown dependency {dependency!r}")
                successors[dependency].append(node.id)
        ready = deque(key for key, degree in degrees.items() if degree == 0)
        order = []
        while ready:
            key = ready.popleft()
            order.append(key)
            for child in successors[key]:
                degrees[child] -= 1
                if degrees[child] == 0:
                    ready.append(child)
        if len(order) != len(nodes):
            raise ValueError("workflow contains a cycle")
        return tuple(order)

    def to_dict(self) -> dict:
        value = dict(id=self.id, name=self.name, nodes=[node.to_dict() for node in self.nodes])
        if self.llm_budget is not None:
            value['llm_budget'] = self.llm_budget.to_dict()
        return value

    @classmethod
    def from_dict(cls, value: dict) -> DAGWorkflow:
        _definition_object(value, ('id', 'name', 'nodes', 'llm_budget'), ('id', 'nodes'))
        return cls(nodes=tuple(DAGNode.from_dict(node)
                              for node in _definition_array(value['nodes'], 'nodes')),
                   id=value['id'], name=value.get('name', 'workflow'),
                   llm_budget=LLMBudget.from_dict(value['llm_budget']) if 'llm_budget' in value else None)


@dataclass(frozen=True)
class NodeRun:
    id: str
    task_id: str
    status: NodeStatus
    result: Any = None
    error: str | None = None
    attempt_count: int = 0
    retry_count: int = 0


@dataclass(frozen=True)
class WorkflowRun:
    id: str
    name: str
    status: WorkflowStatus
    nodes: dict[str, NodeRun]
    llm_usage: dict = field(default_factory=dict)
    llm_budget: dict = field(default_factory=dict)

    @property
    def terminal(self) -> bool:
        return self.status in (WorkflowStatus.SUCCESS, WorkflowStatus.FAILED)
