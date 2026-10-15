"""Serializable DAG definitions and detached runtime snapshots.

Handlers are registry names, never executable code from workflow input. OutputRef
binds a predecessor's JSON output to a downstream keyword argument, making model,
retrieval, tool and validation calls ordinary independently retryable tasks.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4

from .persistence import _json
from .task import Task
from .llm_usage import LLMBudget


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

    @property
    def dependencies(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.depends_on, *(ref.node_id for ref in self.inputs.values()))))

    def to_dict(self) -> dict:
        return dict(id=self.id, handler=self.handler, depends_on=list(self.depends_on),
                    args=list(self.args), kwargs=self.kwargs,
                    inputs={key: dict(node_id=ref.node_id, path=list(ref.path))
                            for key, ref in self.inputs.items()}, priority=self.priority,
                    max_retries=self.max_retries, timeout=self.timeout,
                    idempotent=self.idempotent, scheduled_at=self.scheduled_at)

    @classmethod
    def from_dict(cls, value: dict) -> DAGNode:
        value = dict(value)
        value['args'] = tuple(value.get('args', ()))
        value['depends_on'] = tuple(value.get('depends_on', ()))
        value['inputs'] = {key: OutputRef(ref['node_id'], tuple(ref.get('path', ())))
                           for key, ref in value.get('inputs', {}).items()}
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
        return cls(nodes=tuple(DAGNode.from_dict(node) for node in value['nodes']),
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
