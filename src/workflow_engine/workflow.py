"""Workflow orchestration -> TaskEngine -> workers; no business execution here."""

from __future__ import annotations

import math
import time
from uuid import uuid4

from .dag import DAGWorkflow, WorkflowRun
from .taskengine import TaskEngine


class WorkflowOrchestrator:
    """Reconcile durable dependencies; recover by constructing this over the same DB.

    tick() is suitable for a control-plane loop with remote registered workers.
    run() optionally provisions local workers through TaskEngine for convenience.
    Replays of interrupted tasks still require the existing engine recovery policy.
    """

    def __init__(self, engine: TaskEngine, *, poll_interval: float = .01):
        if not isinstance(engine, TaskEngine):
            raise TypeError("engine must be TaskEngine")
        if not engine.persistent:
            raise ValueError("DAG orchestration requires a persistent TaskEngine")
        self._seconds(poll_interval, 'poll_interval')
        self.engine = engine
        self.poll_interval = poll_interval

    @staticmethod
    def _seconds(value, name):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite positive seconds")

    def submit(self, workflow: DAGWorkflow) -> WorkflowRun:
        return self.engine.submit_workflow(workflow)

    def get(self, workflow_id: str) -> WorkflowRun | None:
        return self.engine.get_workflow(workflow_id)

    def tick(self, workflow_id: str | None = None) -> list[WorkflowRun]:
        ids = [workflow_id] if workflow_id is not None else self.engine.active_workflow_ids()
        return [self.engine.advance_workflow(key) for key in ids]

    def run(self, workflow_id: str, *, execute: bool = True, timeout: float | None = None) -> WorkflowRun:
        """Wait for completion, keeping independent branches running after failure.

        execute=False uses externally provisioned workers. A timeout stops local
        consumers gracefully and preserves state; it does not kill business calls.
        """
        if not isinstance(execute, bool):
            raise TypeError("execute must be bool")
        if timeout is not None:
            self._seconds(timeout, 'timeout')
        first = self.tick(workflow_id)[0]
        if first.terminal:
            return first
        deadline = None if timeout is None else time.monotonic() + timeout
        workers = []
        try:
            if execute:
                identity = uuid4().hex
                for slot in range(self.engine.max_workers):
                    worker = self.engine.create_worker(f'dag-{identity}-{slot}', poll_interval=self.poll_interval)
                    workers.append(worker)
                    worker.start()
            while True:
                run = self.tick(workflow_id)[0]
                if run.terminal:
                    return run
                for worker in workers:
                    if worker.last_error is not None:
                        raise RuntimeError('workflow worker failed') from worker.last_error
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(f"workflow {workflow_id!r} did not finish in time")
                time.sleep(self.poll_interval)
        finally:
            for worker in workers:
                worker.stop()
            for worker in workers:
                worker.shutdown()
