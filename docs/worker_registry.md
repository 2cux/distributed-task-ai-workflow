# Worker Registry — phase 2, iteration 1

This module adds durable Worker identity and heartbeat observations. It does **not** automatically recover tasks or reclaim execution ownership. A late heartbeat only indicates a *suspected* failure, not proof that a process stopped.

## Example

```python
from workflow_engine import WorkerRegistry

registry = WorkerRegistry("tasks.sqlite3")
worker = registry.register("worker-1")
registry.heartbeat("worker-1", worker.session_id)
print(registry.is_overdue("worker-1", timeout_seconds=30))
registry.stop("worker-1", worker.session_id)
```

Workers must explicitly call heartbeat periodically; no background heartbeat sender has been added yet. The database path can be the same SQLite database used by TaskEngine, with the new registry stored in its own table. An ACTIVE Worker ID cannot be registered again simply because its heartbeat is overdue. Only an explicitly STOPPED session can reuse the ID, and old session IDs cannot alter a new session.

## Scope and caveats

- The registry persists Worker identity, session ID, registration time, heartbeat time, and stop time.
- `is_overdue()` reads liveness data and does not mutate it or recover tasks.
- The `owner` currently recorded in the task store is an attempt token. It is **not yet** bound to a WorkerRegistry session ID.
- This is not a distributed execution protocol; shared local SQLite files remain the supported persistence model.
- Clock skew may affect heartbeat judgments when producers and observers have different clocks. Production use will require a clear database-time authority or equivalent coordination design.
- Worker processes that crash remain ACTIVE in this iteration and need operator intervention; a subsequent protocol must establish safe takeover and fencing before automated recovery.

## Tests

```powershell
python -m pip install -e .
python -m pytest tests/test_worker_registry.py -q
python -m pytest -q
```

The next iteration should define WorkerSession lifecycle orchestration and a heartbeat sender, followed by explicit task-ownership fencing and safe recovery semantics. Those are not implemented here.
