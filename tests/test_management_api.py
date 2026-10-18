"""API contracts tested through sockets, not private dispatch calls."""
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

from management_test_support import ADMIN, READER, ManagementTestCase
from workflow_engine import ManagementService, TaskEngine


class ManagementAPIContractTests(ManagementTestCase):
    def test_submit_query_paginate_and_idempotency_under_concurrent_clients(self):
        self.start_service()
        with ThreadPoolExecutor(6) as pool:
            replies = list(pool.map(lambda _: self.call('/api/v1/workflows', self.workflow()), range(12)))
        self.assertEqual([reply[0] for reply in replies], [202] * 12)
        task_ids = {reply[1]['nodes'][0]['task_id'] for reply in replies}
        self.assertEqual(len(task_ids), 1)
        self.assertEqual(self.call('/api/v1/workflows', self.workflow(value=-8))[0], 409)
        for index in range(4):
            self.submit('page-' + str(index))
        first = self.call('/api/v1/workflows?limit=2')[1]
        second = self.call('/api/v1/workflows?limit=2&cursor=' + str(first['next_cursor']))[1]
        third = self.call('/api/v1/workflows?limit=2&cursor=' + str(second['next_cursor']))[1]
        self.assertEqual([item['id'] for page in (first, second, third) for item in page['items']],
                         ['run', 'page-0', 'page-1', 'page-2', 'page-3'])
        self.assertIsNone(third['next_cursor'])
        self.assertEqual(len(self.call('/api/v1/tasks?workflow_id=run')[1]['items']), 1)
        self.assertEqual(self.call('/api/v1/handlers')[1], {'items': ['abs']})

    def test_dag_output_binding_executes_via_worker_and_events_poll_without_gaps(self):
        self.start_service({'abs': abs, 'scale': lambda value: value * 2})
        definition = self.workflow()
        definition['nodes'].append(dict(id='child', handler='scale', inputs={'value': {'node_id': 'root'}}))
        self.assertEqual(self.call('/api/v1/workflows', definition)[0], 202)
        before = self.call('/api/v1/workflows/run/events?limit=100')[1]['items']
        worker = self.worker()
        self.wait_for(lambda: self.call('/api/v1/workflows/run')[1]['status'] == 'SUCCESS')
        self.assertTrue(worker.shutdown(5))
        run = self.call('/api/v1/workflows/run')[1]
        self.assertEqual(run['nodes'][1]['result'], 14)
        after = self.call('/api/v1/workflows/run/events?limit=100&cursor=' + str(before[-1]['cursor']))[1]['items']
        whole = self.call('/api/v1/workflows/run/events?limit=100')[1]['items']
        self.assertEqual(before + after, whole)
        self.assertEqual(len({event['cursor'] for event in whole}), len(whole))
        for node in run['nodes']:
            task = self.call('/api/v1/tasks/' + node['task_id'])[1]
            self.assertEqual((task['attempt_count'], task['status']), (1, 'SUCCESS'))
            first = self.call('/api/v1/tasks/' + node['task_id'] + '/events?limit=2')[1]
            rest = self.call('/api/v1/tasks/' + node['task_id'] + '/events?cursor=2')[1]
            self.assertEqual([event['sequence'] for event in first['items'] + rest['items']], [1, 2, 3, 4])

    def test_auth_roles_request_id_and_public_probes(self):
        self.start_service()
        self.assertEqual(self.call('/health/live', token=None)[0], 200)
        self.assertEqual(self.call('/health/ready', token=None)[0], 200)
        for path in ('/api/v1/workflows', '/metrics', '/openapi.json', '/api/v1/audit'):
            self.assertEqual(self.call(path, token=None)[0], 401)
            self.assertEqual(self.call(path, token='bad-secret')[0], 401)
            self.assertEqual(self.call(path, token=READER)[0], 200)
        self.assertEqual(self.call('/api/v1/workflows', self.workflow(), token=READER)[0], 403)
        self.assertEqual(self.call('/api/v1/workflows')[1]['items'], [])
        status, value, headers = self.call('/api/v1/tasks/missing', headers={'X-Request-ID': 'caller-123'})
        self.assertEqual(status, 404)
        self.assertEqual(value['request_id'], headers['X-Request-ID'])
        self.assertEqual(headers['X-Request-ID'], 'caller-123')

    def test_invalid_definitions_and_protocol_do_not_partially_submit(self):
        self.start_service(max_request_bytes=1024)
        bad = [b'null', b'[]', b'{', b'{"id":"a","id":"b","nodes":[]}',
               b'{"id":"x","nodes":[],"number":NaN}', b'{"id":"x","nodes":[],"number":1e999}']
        for raw in bad:
            self.assertEqual(self.call('/api/v1/workflows', raw=raw)[0], 400)
        definitions = [dict(id='bad', nodes=[dict(id='n', handler='missing')]),
                       dict(id='bad', nodes=[dict(id='n', handler='abs', args='oops')]),
                       dict(id='bad', nodes=[dict(id='n', handler='abs', depends_on=['n'])]),
                       dict(id='bad', nodes=[dict(id='n', handler='abs', timeout=True)]),
                       dict(id='bad', nodes=[dict(id='n', handler='abs', kwargs={'x': 1}, inputs={'x': {'node_id': 'n'}})]),
                       dict(id='bad', nodes=[dict(id='n', handler='abs', args=[-1]), dict(id='n', handler='abs')])]
        for definition in definitions:
            self.assertEqual(self.call('/api/v1/workflows', definition)[0], 400, definition)
        self.assertEqual(self.call('/api/v1/workflows', raw=b' ' * 1025)[0], 413)
        self.assertEqual(self.call('/api/v1/workflows', self.workflow(), headers={'Content-Type': 'text/plain'})[0], 415)
        self.assertEqual(self.call('/api/v1/workflows', self.workflow(), headers={'Transfer-Encoding': 'chunked'})[0], 400)
        self.assertEqual(self.call('/api/v1/overview')[1]['attempts'], 0)
        self.assertEqual(self.call('/api/v1/tasks')[1]['items'], [])

    def test_invalid_queries_and_missing_routes_have_stable_error_codes(self):
        self.start_service()
        for query in ('limit=0', 'limit=101', 'cursor=-1', 'cursor=9223372036854775808',
                      'limit=1&limit=2', 'status=BOGUS', 'sort=desc'):
            self.assertEqual(self.call('/api/v1/workflows?' + query)[0], 400)
        self.assertEqual(self.call('/unknown')[0], 404)
        self.assertEqual(self.call('/api/v1/workflows/missing')[0], 404)
        self.assertEqual(self.call('/api/v1/workflows/missing/events')[0], 404)
        self.assertEqual(self.call('/api/v1/workflows', method='DELETE')[0], 405)
        self.assertEqual(self.call('/api/v1/workflows/run/unknown')[0], 405)

    def test_pause_resume_are_audited_and_gate_claims(self):
        self.start_service()
        run = self.submit()
        operator = dict(operator='operator-one', reason='maintenance')
        self.assertEqual(self.call('/api/v1/workflows/run/pause', operator)[1]['status'], 'PAUSED')
        worker = self.worker()
        self.assertIsNone(self.engine._store.claim_next(worker.session_id, 'bypass-owner'))
        self.assertEqual(self.call('/api/v1/tasks/' + run['nodes'][0]['task_id'])[1]['attempt_count'], 0)
        self.assertEqual(self.call('/api/v1/workflows/run/resume', operator)[0], 200)
        self.wait_for(lambda: self.call('/api/v1/workflows/run')[1]['status'] == 'SUCCESS')
        audit = self.call('/api/v1/audit')[1]['items']
        self.assertEqual([event['action'] for event in audit], ['WORKFLOW_PAUSED', 'WORKFLOW_RESUMED'])
        self.assertEqual(audit[0]['operator'], 'operator-one')
        self.assertEqual(audit[0]['details']['reason'], 'maintenance')

    def test_encoded_workflow_id_and_openapi_contract(self):
        self.start_service()
        key = 'flow / 中文?'
        self.submit(key)
        self.assertEqual(self.call('/api/v1/workflows/' + quote(key, safe=''))[1]['id'], key)
        schema = self.call('/openapi.json')[1]
        self.assertEqual(schema['openapi'], '3.1.0')
        self.assertIn('/api/v1/workflows/{id}/events', schema['paths'])
        self.assertEqual(schema['paths']['/api/v1/workflows']['post']['requestBody']['content']
                         ['application/json']['schema']['$ref'], '#/components/schemas/WorkflowDefinition')

    def test_constructor_guards_and_lifecycle(self):
        with self.assertRaises(ValueError):
            ManagementService(TaskEngine(), ADMIN)
        self.start_service()
        for options in (dict(host='0.0.0.0'), dict(max_concurrency=True), dict(port=-1),
                        dict(database_concurrency=True), dict(database_concurrency=0),
                        dict(database_concurrency=257),
                        dict(readonly_token=ADMIN), dict(request_timeout=float('nan'))):
            with self.assertRaises((ValueError, TypeError)):
                ManagementService(self.engine, ADMIN, **options)
        with self.assertRaises(RuntimeError):
            self.service.start()
        self.assertTrue(self.service.shutdown(5))
        self.assertTrue(self.service.shutdown(5))
        with self.assertRaises(RuntimeError):
            self.service.start()


if __name__ == '__main__':
    import unittest
    unittest.main()
