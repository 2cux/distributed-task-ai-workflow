"""Tool contract tests: no network, database or model credentials required."""
import unittest

from workflow_engine import (Executor, RetryPolicy, Task, TaskEngine, TaskStatus, TOOL_HANDLER,
                             ToolExecutionError, ToolExecutor, ToolInputError, ToolNotFoundError, ToolRegistry,
                             ToolResultError, ToolTransientError)


class ToolRegistryTests(unittest.TestCase):
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
