"""Portable failure categories shared by execution, retry and worker transport."""
from __future__ import annotations

from enum import Enum


class FailureCategory(str, Enum):
    PERMANENT = 'permanent'
    TRANSIENT = 'transient'
    UNCERTAIN = 'uncertain'


def classify_task_failure(task) -> FailureCategory | None:
    """Classify LLM failures conservatively; ordinary tasks retain their policy.

    A handler marker also covers aliases. Stable names cover control-plane
    registries whose callables are placeholders rather than model adapters.
    Never infer replay safety from arbitrary exception messages.
    """
    error = task.error
    category = getattr(error, 'failure_category', None)
    if category is not None:
        return FailureCategory(category)
    scope = getattr(task.callable, 'failure_scope', None)
    handler = getattr(task, '_handler_name', None)
    if scope not in ('llm', 'llm_validation') and handler not in ('llm.chat.v1', 'llm.validate.v1'):
        return None
    error_type = getattr(error, 'original_type', type(error).__name__)
    if isinstance(error, (ValueError, TypeError)) or error_type in ('ValueError', 'TypeError', 'LLMOutputError'):
        return FailureCategory.PERMANENT
    # Includes task soft timeouts, process interruption, and unclassified errors.
    # idempotent=True alone cannot prove the provider did not process a request.
    return FailureCategory.UNCERTAIN


def annotate_task_failure(task) -> None:
    category = classify_task_failure(task)
    if task.error is not None and category is not None:
        task.error.failure_category = category


def failure_metadata(error) -> dict:
    category = getattr(error, 'failure_category', None)
    if category is None:
        return {}
    category = FailureCategory(category)
    return dict(failure_category=category.value,
                retryable=category is FailureCategory.TRANSIENT)


def restore_failure_metadata(error, data) -> None:
    """Restore only validated portable metadata, without importing error classes."""
    error.original_type = data['type']
    if 'failure_category' in data:
        category = FailureCategory(data['failure_category'])
        if 'retryable' in data and (type(data['retryable']) is not bool or
                                   data['retryable'] != (category is FailureCategory.TRANSIENT)):
            raise ValueError('failure category and retryable disagree')
        error.failure_category = category
