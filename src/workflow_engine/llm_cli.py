"""Run a real LLM DAG from a prompt or a serialized workflow definition."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path

from .dag import DAGNode, DAGWorkflow, WorkflowStatus
from .llm import LLMConfig, LLM_HANDLER, create_llm_registry
from .retry import RetryPolicy
from .taskengine import TaskEngine
from .workflow import WorkflowOrchestrator
from .llm_usage import LLMBudget


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run an OpenAI-compatible LLM DAG and persist answers/token usage')
    parser.add_argument('--database', required=True, help='SQLite journal path')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--prompt', help='Create a single LLM node with this prompt')
    source.add_argument('--workflow', type=Path, help='DAG JSON file (stable id allows resuming)')
    parser.add_argument('--model', help='Override LLM_MODEL')
    parser.add_argument('--base-url', help='Override LLM_BASE_URL (include the API version path)')
    parser.add_argument('--max-workers', type=int, default=1)
    parser.add_argument('--max-retries', type=int, default=0, help='Retries may incur additional provider charges')
    parser.add_argument('--max-calls', type=int, help='Workflow LLM attempt limit (prompt mode)')
    parser.add_argument('--max-total-tokens', type=int, help='Gate later LLM calls at this known token usage (prompt mode)')
    args = parser.parse_args(argv)
    env = dict(os.environ)
    if args.model is not None:
        env['LLM_MODEL'] = args.model
    if args.base_url is not None:
        env['LLM_BASE_URL'] = args.base_url
    try:
        if args.workflow and (args.max_calls is not None or args.max_total_tokens is not None):
            raise ValueError('set llm_budget in workflow JSON; budget flags require --prompt')
        registry = create_llm_registry(LLMConfig.from_env(env))
        budget = (LLMBudget(args.max_calls, args.max_total_tokens)
                  if args.max_calls is not None or args.max_total_tokens is not None else None)
        workflow = (DAGWorkflow.from_dict(json.loads(args.workflow.read_text(encoding='utf-8-sig')))
                    if args.workflow else DAGWorkflow((DAGNode('generate', LLM_HANDLER,
                                                              kwargs={'prompt': args.prompt}),), llm_budget=budget))
        engine = TaskEngine(database_path=args.database, task_registry=registry,
                            max_workers=args.max_workers,
                            retry_policy=RetryPolicy(args.max_retries, initial_delay=1))
        orchestrator = WorkflowOrchestrator(engine)
        orchestrator.submit(workflow)
    except (ValueError, TypeError, OSError, KeyError) as error:
        parser.error(str(error))
    result = orchestrator.run(workflow.id)
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0 if result.status is WorkflowStatus.SUCCESS else 1


if __name__ == '__main__':
    raise SystemExit(main())
