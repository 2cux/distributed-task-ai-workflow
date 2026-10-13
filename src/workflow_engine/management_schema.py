"""OpenAPI contract served by ManagementService, without framework dependencies."""


def openapi_document():
    ref = lambda name: {'$ref': '#/components/schemas/' + name}
    text = {'type': 'string', 'minLength': 1}
    json_value = {'description': 'Any finite JSON value.'}
    operator = dict(type='object', additionalProperties=False, required=['operator', 'reason'],
                    properties=dict(operator=text, reason=text))
    node = dict(type='object', additionalProperties=False, required=['id', 'handler'], properties={
        'id': text, 'handler': text, 'depends_on': {'type': 'array', 'items': text},
        'args': {'type': 'array', 'items': json_value}, 'kwargs': {'type': 'object', 'additionalProperties': json_value},
        'inputs': {'type': 'object', 'additionalProperties': ref('OutputRef')},
        'priority': {'type': 'integer'}, 'max_retries': {'type': ['integer', 'null'], 'minimum': 0},
        'timeout': {'type': ['number', 'null'], 'exclusiveMinimum': 0},
        'scheduled_at': {'type': ['number', 'null'], 'minimum': 0}, 'idempotent': {'type': 'boolean'}})
    schemas = {'Node': node, 'OutputRef': dict(type='object', additionalProperties=False,
        required=['node_id'], properties={'node_id': text, 'path': {'type': 'array', 'items': {'type': ['string', 'integer']}}}),
        'WorkflowDefinition': dict(type='object', additionalProperties=False, required=['id', 'nodes'],
            properties={'id': {**text, 'maxLength': 256}, 'name': {**text, 'maxLength': 512},
                        'nodes': {'type': 'array', 'minItems': 1, 'maxItems': 1000, 'items': ref('Node')}}),
        'OperatorAction': operator,
        'Resolution': {**operator, 'required': ['operator', 'reason', 'action'],
                       'properties': {**operator['properties'], 'action': {'enum': ['retry', 'succeed', 'fail']},
                                      'result': json_value, 'extra_retries': {'type': 'integer', 'minimum': 0}}},
        'Error': dict(type='object', required=['error', 'request_id'], properties={
            'error': dict(type='object', required=['code', 'message'], properties={'code': text, 'message': text}),
            'request_id': text}),
        'Page': dict(type='object', required=['items', 'next_cursor'], properties={
            'items': {'type': 'array', 'items': {'type': 'object'}},
            'next_cursor': {'type': ['integer', 'null'], 'description': 'Use the last item cursor to poll event streams after reaching the end.'}}),
        'Workflow': dict(type='object', required=['id', 'name', 'status', 'definition', 'nodes', 'holds'], properties={
            'id': text, 'name': text, 'status': {'enum': ['PENDING', 'RUNNING', 'PAUSED', 'SUCCESS', 'FAILED']},
            'definition': ref('WorkflowDefinition'), 'nodes': {'type': 'array', 'items': {'type': 'object'}},
            'holds': {'type': 'array', 'items': text}})}
    def record(required, properties):
        return dict(type='object', required=required.split(), properties=properties)

    count = {'type': 'integer', 'minimum': 0}
    nullable_text = {'type': ['string', 'null']}
    nullable_time = {'type': ['number', 'null']}
    schemas.update({
        'WorkflowSummary': record('id name status', {key: schemas['Workflow']['properties'][key] for key in ('id', 'name', 'status')}),
        'NodeRun': record('id task_id status task_status attempt_count retry_count', {
            'id': text, 'task_id': text, 'status': {'enum': ['WAITING', 'READY', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED', 'SKIPPED']},
            'task_status': {'enum': ['PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED']},
            'attempt_count': count, 'retry_count': count, 'result': json_value, 'error': json_value}),
        'Task': record('id definition status attempt_count retry_count retry_limit', {
            'id': text, 'definition': {'type': 'object'}, 'status': {'enum': ['PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED']},
            'attempt_count': count, 'retry_count': count, 'retry_limit': count,
            'result': json_value, 'error': json_value, 'last_error': json_value,
            'retry_at': nullable_time, 'scheduled_at': nullable_time, 'worker_session': nullable_text,
            'workflow_blocked': {'type': 'boolean'}, 'holds': {'type': 'array', 'items': text}}),
        'Worker': record('worker_id session_id status registered_at last_heartbeat_at stopped_at', {
            'worker_id': text, 'session_id': text, 'status': {'enum': ['ACTIVE', 'STOPPED']},
            'registered_at': {'type': 'number'}, 'last_heartbeat_at': {'type': 'number'}, 'stopped_at': nullable_time}),
        'Session': record('worker_id session_id state host_id last_heartbeat_at heartbeat_timeout stop_requested', {
            'worker_id': text, 'session_id': text, 'host_id': text, 'state': {'enum': ['ACTIVE', 'STOPPING', 'SUSPECT', 'FENCED', 'STOPPED']},
            'last_heartbeat_at': {'type': 'number'}, 'heartbeat_timeout': {'type': 'number'},
            'stopped_evidence': nullable_text, 'stop_requested': {'type': 'integer', 'enum': [0, 1]}}),
        'Incident': record('id kind task_id state attempt_count reason created_at updated_at', {
            'id': text, 'kind': text, 'task_id': text, 'session_id': nullable_text, 'workflow_id': nullable_text,
            'host_id': nullable_text, 'state': {'enum': ['OPEN', 'RESOLVED']}, 'attempt_count': count,
            'reason': text, 'created_at': {'type': 'number'}, 'updated_at': {'type': 'number'}, 'resolution': nullable_text}),
        'Event': record('cursor sequence', {
            'cursor': count, 'sequence': count, 'task_id': text, 'node_id': text,
            'event_type': text, 'timestamp': {'type': ['string', 'number']},
            'previous_status': nullable_text, 'status': text, 'attempt_count': count, 'retry_count': count,
            'details': {'type': 'object'}, 'action': text, 'target_id': text, 'operator': text,
            'session_id': text, 'state': text, 'reason': text})})
    schemas['Workflow']['properties']['nodes']['items'] = ref('NodeRun')
    for name in ('WorkflowSummary', 'Task', 'Worker', 'Session', 'Incident', 'Event'):
        schemas[name + 'Page'] = {**schemas['Page'], 'properties': {
            **schemas['Page']['properties'], 'items': {'type': 'array', 'items': ref(name)}}}
    paths = {}
    pagination = [{'name': 'cursor', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 0, 'default': 0}},
                  {'name': 'limit', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 100, 'default': 50}}]

    def operation(path, method, summary, *, request=None, response=None, status='200', page=False, public=False):
        parameters = []
        if '{id}' in path:
            parameters.append({'name': 'id', 'in': 'path', 'required': True, 'schema': text})
        parameters += pagination if page else []
        value = dict(summary=summary, operationId=method + '_' + path.strip('/').replace('/', '_').replace('{id}', 'id'),
                     parameters=parameters, security=[] if public else [{'bearerAuth': []}], responses={
            status: {'description': 'Accepted' if status == '202' else 'OK',
                     'content': {'application/json': {'schema': ref(response) if response else {'type': 'object'}}}},
            **{code: {'description': description, 'content': {'application/json': {'schema': ref('Error')}}}
               for code, description in (('400', 'Invalid request'), ('401', 'Authentication required'),
                 ('403', 'Admin credential required'), ('404', 'Not found'), ('405', 'Unsupported method'), ('409', 'Conflict'),
                 ('413', 'Size limit exceeded'), ('415', 'Unsupported content type'), ('500', 'Internal error'),
                 ('503', 'Unavailable or capacity exhausted'))}})
        if request:
            value['requestBody'] = {'required': True, 'content': {'application/json': {'schema': ref(request)}}}
        paths.setdefault(path, {})[method] = value
        return value

    for health in ('live', 'ready'):
        operation('/health/' + health, 'get', 'Process liveness' if health == 'live' else 'Database and coordinator readiness', public=True)
    metrics = operation('/metrics', 'get', 'Prometheus metrics (read credential allowed)')
    metrics['responses']['200']['content'] = {'text/plain': {'schema': {'type': 'string'}}}
    operation('/openapi.json', 'get', 'This OpenAPI contract')
    operation('/api/v1/overview', 'get', 'Coherent persisted engine totals')
    operation('/api/v1/handlers', 'get', 'Registered handler names')
    operation('/api/v1/audit', 'get', 'Incremental recovery/operator audit', page=True, response='EventPage')
    states = {'workflows': ['PENDING', 'RUNNING', 'PAUSED', 'SUCCESS', 'FAILED'],
              'tasks': ['PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED'],
              'workers': ['ACTIVE', 'STOPPED'], 'sessions': ['ACTIVE', 'STOPPING', 'SUSPECT', 'FENCED', 'STOPPED'],
              'incidents': ['OPEN', 'RESOLVED']}
    for resource in states:
        path = '/api/v1/' + resource
        model = {'workflows': 'WorkflowSummary', 'tasks': 'Task', 'workers': 'Worker',
                 'sessions': 'Session', 'incidents': 'Incident'}[resource]
        listing = operation(path, 'get', 'List ' + resource + ' in insertion order', page=True, response=model + 'Page')
        listing['parameters'].append({'name': 'status', 'in': 'query', 'schema': {'enum': states[resource]}})
        if resource == 'tasks':
            listing['parameters'].append({'name': 'workflow_id', 'in': 'query', 'schema': text})
        operation(path + '/{id}', 'get', 'Read persisted ' + resource + ' snapshot', response='Workflow' if resource == 'workflows' else model)
        if resource in ('workflows', 'tasks', 'sessions'):
            operation(path + '/{id}/events', 'get', 'Poll ordered events; cursor is append order', page=True, response='EventPage')
    operation('/api/v1/workflows', 'post', 'Submit a DAG atomically; same id and definition is an idempotent replay',
              request='WorkflowDefinition', response='Workflow', status='202')
    for action in ('pause', 'resume'):
        operation('/api/v1/workflows/{id}/' + action, 'post', action.capitalize() + ' workflow dispatch (admin required)',
                  request='OperatorAction', response='Workflow')
    operation('/api/v1/sessions/{id}/stop', 'post', 'Request graceful worker drain (admin required)', request='OperatorAction', response='Session', status='202')
    operation('/api/v1/incidents/{id}/resolve', 'post', 'Reconcile a failed attempt using existing recovery guards (admin required)', request='Resolution', response='Incident')
    return {'openapi': '3.1.0', 'info': {'title': 'Workflow Management API', 'version': '1.0.0',
             'description': 'Bearer admin or read-only credentials. Mutations require admin. Stable workflow IDs provide submission idempotency. Workers are provisioned separately.'},
            'paths': paths, 'components': {'securitySchemes': {'bearerAuth': {'type': 'http', 'scheme': 'bearer'}}, 'schemas': schemas}}
