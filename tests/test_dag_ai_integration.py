"""AI pipeline integration, using deterministic model/tool adapters (no credentials)."""
from pathlib import Path
import tempfile
import unittest

from workflow_engine import (DAGNode, DAGWorkflow, NodeStatus, OutputRef,
                             TaskEngine, WorkflowOrchestrator, WorkflowStatus)


class DAGAIIntegrationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1], prefix='.dag-ai-tests-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'ai.db'

    def test_retrieval_model_tool_validation_pipeline_persists_outputs(self):
        calls = []

        def retrieve(query):
            calls.append('retrieve')
            return {'documents': [{'text': f'context for {query}'}]}

        def model(context, model_name):
            calls.append(model_name)
            return {'tool': {'arguments': {'text': context.upper()}}, 'usage': {'tokens': 12}}

        def tool(arguments):
            calls.append('tool')
            return {'answer': arguments['text']}

        def validate(answer):
            calls.append('validate')
            if not answer.startswith('CONTEXT'):
                raise ValueError('invalid model answer')
            return {'valid': True, 'answer': answer}

        registry = dict(retrieve=retrieve, model=model, tool=tool, validate=validate)
        orchestrator = WorkflowOrchestrator(TaskEngine(database_path=self.path, task_registry=registry, max_workers=2))
        workflow = DAGWorkflow((
            DAGNode('retrieve', 'retrieve', kwargs={'query': 'DAG'}),
            DAGNode('model', 'model', kwargs={'model_name': 'test-model'},
                    inputs={'context': OutputRef('retrieve', ('documents', 0, 'text'))}, timeout=2),
            DAGNode('tool', 'tool', inputs={'arguments': OutputRef('model', ('tool', 'arguments'))}),
            DAGNode('validate', 'validate', inputs={'answer': OutputRef('tool', ('answer',))}),
        ), name='retrieval-model-tool-validation')
        orchestrator.submit(workflow)
        result = orchestrator.run(workflow.id, timeout=10)
        self.assertEqual(result.status, WorkflowStatus.SUCCESS)
        self.assertEqual(result.nodes['validate'].result, {'valid': True, 'answer': 'CONTEXT FOR DAG'})
        self.assertEqual(calls, ['retrieve', 'test-model', 'tool', 'validate'])
        reopened = WorkflowOrchestrator(TaskEngine(database_path=self.path, task_registry=registry))
        self.assertEqual(reopened.get(workflow.id), result)
        self.assertEqual(reopened.run(workflow.id, timeout=1), result)
        self.assertEqual(len(calls), 4)

    def test_missing_model_output_fails_binding_and_skips_descendants(self):
        calls = []

        def model():
            return {'wrong_field': 'value'}

        def tool(data):
            calls.append(data)
            return data

        engine = TaskEngine(database_path=self.path, task_registry={'model': model, 'tool': tool})
        orchestrator = WorkflowOrchestrator(engine)
        workflow = DAGWorkflow((DAGNode('model', 'model'),
            DAGNode('tool', 'tool', inputs={'data': OutputRef('model', ('missing',))}),
            DAGNode('tail', 'tool', kwargs={'data': 1}, depends_on=('tool',))))
        orchestrator.submit(workflow)
        result = orchestrator.run(workflow.id, timeout=5)
        self.assertEqual(result.status, WorkflowStatus.FAILED)
        self.assertEqual(result.nodes['tool'].status, NodeStatus.FAILED)
        self.assertIn('input binding failed', result.nodes['tool'].error)
        self.assertEqual(result.nodes['tool'].attempt_count, 0)
        self.assertEqual(result.nodes['tail'].status, NodeStatus.SKIPPED)
        self.assertEqual(calls, [])
