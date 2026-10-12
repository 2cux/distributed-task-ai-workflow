"""Definition validation, independent of the execution integration suites."""
import unittest

from workflow_engine import DAGNode, DAGWorkflow, OutputRef


class DAGDefinitionTests(unittest.TestCase):
    def test_rejects_cycles_missing_dependencies_and_duplicate_ids(self):
        invalid = [
            (DAGNode('a', 'f', depends_on=('a',)),),
            (DAGNode('a', 'f', depends_on=('b',)), DAGNode('b', 'f', depends_on=('a',))),
            (DAGNode('a', 'f', inputs={'x': OutputRef('unknown')}),),
            (DAGNode('a', 'f'), DAGNode('a', 'f')),
        ]
        for nodes in invalid:
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                DAGWorkflow(nodes)

    def test_output_references_add_edges_and_definition_roundtrips(self):
        workflow = DAGWorkflow((
            DAGNode('last', 'f', inputs={'data': OutputRef('first', ('items', 0))}),
            DAGNode('first', 'f', kwargs={'prompt': 'hello'}, timeout=1, max_retries=2),
        ))
        self.assertEqual(workflow.topological_order(), ('first', 'last'))
        self.assertEqual(DAGWorkflow.from_dict(workflow.to_dict()), workflow)

    def test_conflicting_binding_is_rejected(self):
        with self.assertRaises(ValueError):
            DAGNode('a', 'f', kwargs={'x': 1}, inputs={'x': OutputRef('b')})
