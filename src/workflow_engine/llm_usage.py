"""Workflow LLM accounting from committed task attempts and existing token records.

Only generation tasks participate. Copies of their results (validation, tools,
task snapshots and repeated events) never create an additional generation attempt.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json


TOKEN_FIELDS = ('prompt_tokens', 'completion_tokens', 'total_tokens')


@dataclass(frozen=True)
class LLMBudget:
    """Immutable attempt cap and known-token stop threshold; None means unlimited."""

    max_calls: int | None = None
    max_total_tokens: int | None = None

    def __post_init__(self):
        for name, value in asdict(self).items():
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f'{name} must be a nonnegative integer or null')

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict:
            raise ValueError('llm_budget must be an object')
        return cls(**value)


def is_llm_handler(name, handler):
    # Stable built-in name also covers remote control-plane placeholders.
    return name == 'llm.chat.v1' or getattr(handler, 'failure_scope', None) == 'llm'


def workflow_usage(db, workflow_id):
    """Read inside the caller's transaction, without loading executable handlers.

    Task/attempt identity is authoritative, including when providers reuse or omit
    response IDs. Manual reconciliation updates an attempt, never adds a call.
    """
    rows = db.execute('''SELECT t.id,t.status,t.attempt_count,t.result FROM tasks t
        JOIN workflow_nodes n ON n.task_id=t.id
        WHERE n.workflow_id=? AND n.is_llm=1''', (workflow_id,)).fetchall()
    attempts = {}
    running = 0
    for row in rows:
        for number in range(1, row['attempt_count'] + 1):
            attempts[row['id'], number] = (None, None)
        running += row['status'] == 'RUNNING'
    events = db.execute('''SELECT e.* FROM task_events e JOIN workflow_nodes n ON n.task_id=e.task_id
        WHERE n.workflow_id=? AND n.is_llm=1 AND e.event_type IN
        ('SUCCEEDED','FAILED','INTERRUPTED','MANUAL_RESOLUTION','RESTORED')
        ORDER BY e.task_id,e.sequence''', (workflow_id,))
    for event in events:
        key = event['task_id'], event['attempt_count']
        if key not in attempts:
            continue  # Input binding or budget rejection before an attempt.
        detail = json.loads(event['details'])
        kind = event['event_type']
        if kind == 'SUCCEEDED' or (kind == 'MANUAL_RESOLUTION' and detail.get('action') == 'succeed'):
            attempts[key] = ('SUCCESS', detail.get('result'))
        elif kind in ('FAILED', 'INTERRUPTED'):
            attempts[key] = ('FAILED', None)
        elif kind == 'RESTORED' and event['status'] in ('SUCCESS', 'FAILED'):
            attempts[key] = (event['status'], detail.get('result'))
    for row in rows:
        key = row['id'], row['attempt_count']
        if key in attempts and attempts[key][0] is None and row['status'] in ('SUCCESS', 'FAILED'):
            attempts[key] = (row['status'], json.loads(row['result']) if row['result'] else None)
    calls = len(attempts)
    successes = sum(status == 'SUCCESS' for status, _ in attempts.values())
    failures = sum(status == 'FAILED' for status, _ in attempts.values())
    totals = dict.fromkeys(TOKEN_FIELDS, 0)
    missing = dict.fromkeys(TOKEN_FIELDS, 0)
    unknown = 0
    for _, result in attempts.values():
        usage = result.get('usage') if isinstance(result, dict) else None
        complete = True
        for name in TOKEN_FIELDS:
            value = usage.get(name) if isinstance(usage, dict) else None
            if type(value) is int and value >= 0:
                totals[name] += value
            else:
                missing[name] += 1
                complete = False
        unknown += not complete
    # Partial totals always mean known usage only; null means no observation.
    return dict(call_count=calls, successful_calls=successes, failed_calls=failures,
                running_calls=running, unknown_outcome_calls=calls-successes-failures-running,
                failure_rate=failures / (successes + failures) if successes + failures else 0.0,
                **{name: None if calls and missing[name] == calls else value for name, value in totals.items()},
                unknown_usage_calls=unknown, missing_usage_calls=missing,
                usage_complete=unknown == 0)


def budget_status(budget, usage):
    """Token caps gate later dispatch; unknown completed usage fails closed."""
    budget = budget or {}
    calls = budget.get('max_calls')
    tokens = budget.get('max_total_tokens')
    reason = None
    if calls is not None and usage['call_count'] >= calls:
        reason = 'max_calls'
    elif tokens is not None:
        if (usage['total_tokens'] or 0) >= tokens:
            reason = 'max_total_tokens'
        elif usage['missing_usage_calls']['total_tokens'] > usage['running_calls']:
            reason = 'unknown_token_usage'
    return dict(limits=dict(max_calls=calls, max_total_tokens=tokens),
                remaining_calls=None if calls is None else max(0, calls-usage['call_count']),
                remaining_total_tokens=(None if tokens is None else
                    None if usage['missing_usage_calls']['total_tokens'] else max(0, tokens-(usage['total_tokens'] or 0))),
                blocked=reason is not None, reason=reason,
                waiting_for_usage=tokens is not None and usage['running_calls'] > 0 and reason is None)
