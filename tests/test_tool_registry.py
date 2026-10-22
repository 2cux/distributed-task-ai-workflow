"""Tool contract tests: no network, database or model credentials required."""
from copy import deepcopy
import json
import unittest

from workflow_engine import (Executor, RetryPolicy, Task, TaskEngine, TaskStatus, TOOL_HANDLER,
                             ToolExecutionError, ToolExecutor, ToolInputError, ToolNotFoundError, ToolRegistry,
                             ToolResultError, ToolTransientError)


class ToolRegistryTests(unittest.TestCase):
    def test_schema_checks_static_and_dynamic_calls_before_effects(self):
        calls = []
        schema = {'type': 'object', 'properties': {
            'count': {'type': 'integer', 'minimum': 1, 'maximum': 3},
            'data': {'type': 'array', 'minItems': 1, 'maxItems': 2, 'items': {
                'type': 'object', 'properties': {'name': {'type': 'string', 'minLength': 1, 'maxLength': 4}},
                'required': ['name'], 'additionalProperties': False}},
        }, 'required': ['count', 'data'], 'additionalProperties': False}
        def effect(count, data=None, **extra):
            calls.append((count, data, extra))
            return count
        registry = ToolRegistry()
        registry.register('effect', effect, parameters_schema=schema)
        handlers = registry.to_task_registry()
        invalid = [dict(count=True, data=[{'name': 'ok'}]),
                   dict(count='secret-input', data=[{'name': 'ok'}]),
                   dict(count=0, data=[{'name': 'ok'}]), dict(count=4, data=[{'name': 'ok'}]),
                   dict(count=1), dict(count=1, data=[]), dict(count=1, data=[{}, {}, {}]),
                   dict(count=1, data=[{'name': ''}]), dict(count=1, data=[{'name': 'large'}]),
                   dict(count=1, data=[{'name': 'ok', 'extra': 1}]),
                   dict(count=1, data=[{'name': 'ok'}], extra='secret-input'),
                   dict(count=float('nan'), data=[{'name': 'ok'}])]
        for arguments in invalid:
            for dynamic in (False, True):
                with self.subTest(arguments=arguments, dynamic=dynamic), self.assertRaises(ToolInputError) as caught:
                    if dynamic:
                        handlers[TOOL_HANDLER]('effect', arguments)
                    else:
                        handlers['effect'](arguments['count'], **{k: v for k, v in arguments.items() if k != 'count'})
                self.assertNotIn('secret-input', str(caught.exception))
                self.assertEqual(caught.exception.failure_category.value, 'permanent')
        self.assertEqual(calls, [])
        self.assertEqual(handlers['effect'](2, [{'name': '资料'}]), 2)
        self.assertEqual(handlers[TOOL_HANDLER]('effect', dict(count=1.0, data=[{'name': 'ok'}])), 1.0)
        self.assertEqual(len(calls), 2)

    def test_schema_and_catalog_are_detached_from_all_public_objects(self):
        schema = {'type': 'object', 'properties': {'value': {'type': 'integer'}}, 'required': ['value']}
        original = deepcopy(schema)
        registry = ToolRegistry()
        definition = registry.register('echo', lambda value: value, parameters_schema=schema)
        handlers = registry.to_task_registry()
        schema['properties']['value']['type'] = 'string'
        definition.parameters_schema['properties'].clear()
        catalog = registry.describe()
        self.assertEqual(catalog, [{'name': 'echo', 'description': '', 'parameters_schema': original}])
        catalog[0]['parameters_schema']['properties']['value']['type'] = 'boolean'
        registry.register('later', lambda: None)
        self.assertEqual(registry.describe()[0]['parameters_schema'], original)
        for invoke in (lambda: handlers['echo']('bad'), lambda: handlers[TOOL_HANDLER]('echo', {'value': 'bad'})):
            with self.assertRaises(ToolInputError):
                invoke()
        self.assertEqual(handlers['echo'](1), 1)

    def test_catalog_never_introspects_or_serializes_defaults_and_annotations(self):
        class Secret:
            def __repr__(self):
                raise AssertionError('default or annotation repr must not be used')
        secret = Secret()
        def action(value: secret = secret):
            return 'ok'
        registry = ToolRegistry()
        registry.register('legacy', action)
        registry.register('typed', action, parameters_schema={'type': 'object'})
        self.assertEqual(json.loads(json.dumps(registry.describe())), [
            {'name': 'legacy', 'description': ''},
            {'name': 'typed', 'description': '', 'parameters_schema': {'type': 'object'}},
        ])
        self.assertEqual(registry.to_task_registry()['typed'](), 'ok')

    def test_invalid_schemas_fail_registration_atomically(self):
        cyclic = {'type': 'object'}
        cyclic['properties'] = {'loop': cyclic}
        invalid = [True, [], 'object', {}, {'type': 'array'}, {'type': ['object']},
                   {'type': 'object', '$ref': 'https://secret.invalid/schema'},
                   {'type': 'object', 'properties': []},
                   {'type': 'object', 'properties': {'x': {'type': 'unknown'}}},
                   {'type': 'object', 'properties': {'x': False}},
                   {'type': 'object', 'required': 'x'}, {'type': 'object', 'required': [1]},
                   {'type': 'object', 'required': ['x', 'x']},
                   {'type': 'object', 'additionalProperties': {}},
                   {'type': 'object', 'items': []}, {'type': 'object', 'enum': []},
                   {'type': 'object', 'enum': [1, 1.0]},
                   {'type': 'object', 'enum': [{'x': [1]}, {'x': [1.0]}]},
                   {'type': 'object', 'description': object()},
                   {'type': 'object', 'properties': {1: {}}},
                   {'type': 'object', 'minimum': True}, {'type': 'object', 'minimum': float('inf')},
                   {'type': 'object', 'maxLength': True}, {'type': 'object', 'minItems': -1}, cyclic]
        for schema in invalid:
            registry = ToolRegistry()
            with self.subTest(schema_type=type(schema).__name__), self.assertRaises(ValueError) as caught:
                registry.register('bad', lambda: None, parameters_schema=schema)
            self.assertNotIn('secret.invalid', str(caught.exception))
            self.assertEqual(registry.describe(), [])
            self.assertEqual(set(registry.to_task_registry()), {TOOL_HANDLER})

    def test_schema_types_and_enum_use_json_semantics(self):
        for kind, valid, invalid in [('boolean', True, 1), ('null', None, 'null'),
                                     ('number', 1.5, True), ('integer', 1.0, 1.5),
                                     ('string', 'x', 1), ('array', [], {}), ('object', {}, [])]:
            registry = ToolRegistry()
            registry.register('echo', lambda value: value, parameters_schema={
                'type': 'object', 'properties': {'value': {'type': kind}}})
            invoke = registry.to_task_registry()[TOOL_HANDLER]
            with self.subTest(kind=kind):
                self.assertEqual(invoke('echo', {'value': valid}), valid)
                with self.assertRaises(ToolInputError):
                    invoke('echo', {'value': invalid})
        registry = ToolRegistry()
        registry.register('enum', lambda value: value, parameters_schema={
            'type': 'object', 'properties': {'value': {'enum': [{'x': [1]}, True, None]}}})
        invoke = registry.to_task_registry()['enum']
        for valid in ({'x': [1.0]}, True, None):
            self.assertEqual(invoke(valid), valid)
        for invalid in ({'x': [True]}, 1, False):
            with self.assertRaises(ToolInputError):
                invoke(invalid)

    def test_variadic_and_positional_only_arguments_and_signature_gate(self):
        calls = []
        def action(value, /, *items, flag=False, **extra):
            calls.append(1)
            return [value, list(items), flag, extra]
        registry = ToolRegistry()
        registry.register('action', action, parameters_schema={
            'type': 'object', 'properties': {'value': {'type': 'integer'},
                'items': {'type': 'array', 'items': {'type': 'string'}}, 'flag': {'type': 'boolean'},
                'label': {'type': 'string'}}, 'additionalProperties': False})
        invoke = registry.to_task_registry()['action']
        self.assertEqual(invoke(1, 'x', flag=True, label='ok'), [1, ['x'], True, {'label': 'ok'}])
        for args, kwargs in [((1, 2), {}), ((1,), {'value': 'collision'}), ((), {'value': 1})]:
            with self.assertRaises(ToolInputError):
                invoke(*args, **kwargs)
        self.assertEqual(calls, [1])

    def test_standalone_executor_enforces_the_same_tool_boundaries(self):
        with self.assertRaises(TypeError):
            ToolExecutor({'bad': None})
        async def asynchronous():
            return 1
        with self.assertRaises(TypeError):
            ToolExecutor({'async': asynchronous})
        executor = ToolExecutor({'echo': lambda value: value})
        self.assertEqual(executor('echo', {'value': 42}), 42)
        with self.assertRaises(ToolInputError):
            executor('echo', {'value': object()})
    def test_static_and_dynamic_dispatch_have_same_json_result(self):
        registry = ToolRegistry()
        registry.register('math.add.v1', lambda left, right: {'sum': left + right}, description='Add two values')
        handlers = registry.to_task_registry()
        self.assertEqual(handlers['math.add.v1'](2, right=3), {'sum': 5})
        task = Executor().execute(Task('tool', handlers[TOOL_HANDLER],
                                      kwargs=dict(tool_name='math.add.v1', arguments=dict(left=2, right=3))))
        self.assertEqual((task.status, task.result), (TaskStatus.SUCCESS, {'sum': 5}))
        self.assertEqual(registry.describe(), [{'name': 'math.add.v1', 'description': 'Add two values'}])

    def test_duplicate_reserved_and_invalid_registrations_are_rejected(self):
        registry = ToolRegistry()
        registry.register('tool', lambda: 1)
        for name in ('tool', TOOL_HANDLER, '', ' ', ' spaced ', None):
            with self.subTest(name=name), self.assertRaises(ValueError):
                registry.register(name, lambda: 2)
        with self.assertRaises(TypeError):
            registry.register('noncallable', 1)
        async def asynchronous():
            return 1
        with self.assertRaises(TypeError):
            registry.register('async', asynchronous)

    def test_export_is_a_snapshot_and_aliases_have_distinct_callables(self):
        registry = ToolRegistry()
        fn = lambda: 7
        registry.register('one', fn)
        old = registry.to_task_registry()
        registry.register('two', fn)
        new = registry.to_task_registry()
        self.assertIsNot(new['one'], new['two'])
        with self.assertRaises(ToolNotFoundError):
            old[TOOL_HANDLER]('two', {})
        old['two'] = lambda: 'not part of the dispatcher'
        with self.assertRaises(ToolNotFoundError):
            old[TOOL_HANDLER]('two', {})
        self.assertEqual(new[TOOL_HANDLER]('two', {}), 7)

    def test_unknown_names_and_bad_arguments_never_invoke_tools(self):
        calls = []
        registry = ToolRegistry()
        registry.register('action', lambda required: calls.append(required))
        invoke = registry.to_task_registry()[TOOL_HANDLER]
        for name, arguments in (('unknown', {}), ('__import__', {}), ('', {}),
                                ('action', []), ('action', {}), ('action', {'required': object()}),
                                ('action', {'required': 1, 'unexpected': True})):
            with self.subTest(name=name), self.assertRaises(ToolInputError):
                invoke(name, arguments)
        self.assertEqual(calls, [])

    def test_inputs_and_outputs_are_detached(self):
        returned = {'items': [1]}
        def mutate(data):
            data['items'].append(2)
            return returned
        registry = ToolRegistry()
        registry.register('mutate', mutate)
        argument = {'items': [0]}
        result = registry.to_task_registry()[TOOL_HANDLER]('mutate', {'data': argument})
        self.assertEqual(argument, {'items': [0]})
        returned['items'].append(3)
        self.assertEqual(result, {'items': [1]})

    def test_non_json_outputs_fail_after_one_effect_without_retry(self):
        for value in (object(), (1, 2), {1: 'key'}, float('nan')):
            calls = []
            registry = ToolRegistry()
            def effect():
                calls.append(1)
                return value
            registry.register('effect', effect)
            engine = TaskEngine(retry_policy=RetryPolicy(3))
            task = engine.submit(Task('effect', registry.to_task_registry()['effect']))
            engine.start()
            self.assertIsInstance(task.error, ToolResultError)
            self.assertEqual((calls, task.attempt_count, task.retry_count), ([1], 1, 0))

    def test_unclassified_exception_is_sanitized_and_not_replayed(self):
        calls = []
        def effect():
            calls.append(1)
            raise ValueError('secret-token from remote service')
        registry = ToolRegistry()
        registry.register('effect', effect)
        engine = TaskEngine(retry_policy=RetryPolicy(3))
        task = engine.submit(Task('effect', registry.to_task_registry()['effect']))
        engine.start()
        self.assertIsInstance(task.error, ToolExecutionError)
        self.assertNotIn('secret-token', str(task.error))
        self.assertEqual(calls, [1])

    def test_explicit_transient_failure_uses_engine_retry_budget(self):
        calls = []
        def tool():
            calls.append(1)
            if len(calls) == 1:
                raise ToolTransientError('service rejected before execution')
            return {'done': True}
        registry = ToolRegistry()
        registry.register('safe', tool)
        engine = TaskEngine(retry_policy=RetryPolicy(1))
        task = engine.submit(Task('safe', registry.to_task_registry()[TOOL_HANDLER],
                                  kwargs=dict(tool_name='safe', arguments={})))
        engine.start()
        self.assertEqual((task.status, task.result, task.retry_count, len(calls)),
                         (TaskStatus.SUCCESS, {'done': True}, 1, 2))

    def test_soft_timeout_never_automatically_replays_registered_tools(self):
        # The timeout executor can fail without entering a tool wrapper.
        # Its marker still has to prevent the engine starting another attempt.
        from threading import Event
        entered, release = Event(), Event()
        def action():
            entered.set()
            release.wait(2)
        registry = ToolRegistry()
        registry.register('action', action)
        engine = TaskEngine(retry_policy=RetryPolicy(3))
        task = engine.submit(Task('action', registry.to_task_registry()['action'], timeout=.02))
        try:
            engine.start()
            self.assertTrue(entered.is_set())
            self.assertEqual((task.status, task.attempt_count, task.retry_count), (TaskStatus.FAILED, 1, 0))
        finally:
            release.set()


if __name__ == '__main__':
    unittest.main()
