"""Independent HTTP transport and failure tests against a real loopback server."""
import json
import unittest
from urllib.parse import parse_qs, urlsplit

from workflow_engine import (FailureCategory, HTTPConfig, HTTPRequestHandler, HTTPResponseError,
                             HTTPStatusError, HTTPTransportError, HTTP_HANDLER, TOOL_HANDLER,
                             ToolInputError, create_http_registry)
from tool_test_support import local_service


class HTTPToolTests(unittest.TestCase):
    def setUp(self):
        self.service = local_service(self)
        self.config = HTTPConfig((self.service.origin,), default_headers={'Authorization': 'Bearer worker-secret'})
        self.handler = HTTPRequestHandler(self.config)

    def test_get_encodes_unicode_query_and_preserves_existing_parameters(self):
        result = self.handler(self.service.origin + '/search?existing=1',
                              params={'q': '工作流 & DAG', 'tag': ['one', 'two']})
        query = parse_qs(urlsplit(self.service.calls[0]['path']).query)
        self.assertEqual(query, dict(existing=['1'], q=['工作流 & DAG'], tag=['one', 'two']))
        self.assertEqual(result['status_code'], 200)
        self.assertEqual(result['json']['method'], 'GET')
        self.assertEqual(json.loads(result['text']), result['json'])
        self.assertEqual(self.service.calls[0]['headers']['Authorization'], 'Bearer worker-secret')

    def test_json_post_executes_external_action_and_returns_structured_data(self):
        handlers = create_http_registry(self.config)
        result = handlers[TOOL_HANDLER](HTTP_HANDLER, dict(url=self.service.origin + '/actions',
                    method='POST', json_body={'name': '执行', 'items': [1, True, None]}))
        self.assertEqual(self.service.effects, [{'name': '执行', 'items': [1, True, None]}])
        self.assertEqual(result['json']['body'], self.service.effects[0])
        self.assertEqual(self.service.calls[0]['headers']['Content-Type'], 'application/json')

    def test_text_body_all_methods_and_json_null_are_supported(self):
        for method in ('PUT', 'PATCH', 'DELETE', 'OPTIONS'):
            result = self.handler(self.service.origin + '/echo', method=method.lower(), text_body='文本')
            self.assertEqual(result['json']['body'], '文本')
            self.assertEqual(result['json']['method'], method)
        result = self.handler(self.service.origin + '/echo', method='POST', json_body=None)
        self.assertIsNone(result['json']['body'])
        result = self.handler(self.service.origin + '/echo', method='HEAD')
        self.assertEqual((result['json'], result['text']), (None, ''))

    def test_text_response_and_no_content(self):
        self.service.replies.append((200, '纯文本'.encode(), {}))
        result = self.handler(self.service.origin + '/text', response_format='text')
        self.assertEqual((result['text'], result['json']), ('纯文本', None))
        self.service.replies.append((204, b'', {}))
        self.assertIsNone(self.handler(self.service.origin + '/empty')['json'])
        self.service.replies.append((205, b'', {}))
        self.assertIsNone(self.handler(self.service.origin + '/reset')['json'])
        self.service.replies.append((200, b'', {}))
        self.assertEqual(self.handler(self.service.origin + '/empty-text', response_format='text')['text'], '')

    def test_invalid_input_is_rejected_before_any_network_request(self):
        for kwargs in (
                dict(url='file:///etc/passwd'), dict(url='http://user:secret@localhost/'),
                dict(url='https://example.invalid/'), dict(url=self.service.origin + '.evil/path'),
                dict(url=self.service.origin + '/fragment#private'), dict(url=self.service.origin + '/\n'),
                dict(method='CONNECT'), dict(method=1), dict(json_body={}),
                dict(method='POST', json_body={}, text_body='both'), dict(method='POST', text_body=4),
                dict(params={'nested': {}}), dict(params={'bad': float('nan')}), dict(params=[]),
                dict(headers={'Authorization': 'override'}), dict(headers={'AUTHORIZATION': 'override'}),
                dict(headers={'Host': 'elsewhere'}), dict(headers={'X-Test': 'bad\r\nInjected: yes'}),
                dict(headers={'Content-Length': '3'}), dict(headers={'x': 'a', 'X': 'b'}),
                dict(response_format='binary')):
            with self.subTest(fields=list(kwargs)), self.assertRaises(ToolInputError):
                self.handler(**dict({'url': self.service.origin}, **kwargs))
        self.assertEqual(self.service.calls, [])

    def test_origin_matches_scheme_and_effective_port_not_prefix(self):
        for url in ('https://example.com', 'http://example.com:81', 'http://example.com.evil',
                    'http://example.com@evil.invalid'):
            handler = HTTPRequestHandler(HTTPConfig(('http://example.com:80',)))
            with self.subTest(url=url), self.assertRaises(ToolInputError):
                handler(url)

    def test_redirect_is_rejected_and_auth_is_never_forwarded(self):
        target = local_service(self)
        self.service.replies.append((302, {'private': 'remote-secret'}, {'Location': target.origin + '/capture'}))
        with self.assertRaises(HTTPStatusError) as caught:
            self.handler(self.service.origin + '/redirect')
        self.assertEqual(caught.exception.status_code, 302)
        self.assertEqual(caught.exception.failure_category, FailureCategory.PERMANENT)
        self.assertEqual(len(self.service.calls), 1)
        self.assertEqual(target.calls, [])
        self.assertNotIn('secret', str(caught.exception))

    def test_status_failure_does_not_leak_remote_body_and_never_retries_internally(self):
        for method, code, category in (('GET', 503, FailureCategory.TRANSIENT),
                                       ('POST', 503, FailureCategory.UNCERTAIN),
                                       ('GET', 429, FailureCategory.TRANSIENT),
                                       ('POST', 408, FailureCategory.UNCERTAIN),
                                       ('GET', 401, FailureCategory.PERMANENT)):
            self.service.replies.append((code, {'token': 'private-remote-body'}, {}))
            before = len(self.service.calls)
            with self.subTest(method=method, code=code), self.assertRaises(HTTPStatusError) as caught:
                self.handler(self.service.origin + '/error?key=private-url-key', method=method)
            self.assertEqual(caught.exception.failure_category, category)
            self.assertEqual(len(self.service.calls), before + 1)
            self.assertNotIn('private', str(caught.exception))

    def test_request_and_response_limits_fail_at_the_correct_boundary(self):
        handler = HTTPRequestHandler(HTTPConfig((self.service.origin,), max_request_bytes=4, max_response_bytes=8))
        with self.assertRaises(ToolInputError):
            handler(self.service.origin + '/actions', method='POST', json_body={'large': 'x' * 20})
        self.assertEqual(self.service.calls, [])
        with self.assertRaises(HTTPResponseError):
            handler(self.service.origin + '/items')
        self.assertEqual(len(self.service.calls), 1)

    def test_malformed_json_and_nonfinite_numbers_are_uncertain(self):
        for raw in (b'{bad', b'{"a":NaN}', b'\xff'):
            self.service.replies.append((200, raw, {}))
            with self.subTest(raw=raw), self.assertRaises(HTTPResponseError) as caught:
                self.handler(self.service.origin + '/bad')
            self.assertEqual(caught.exception.failure_category, FailureCategory.UNCERTAIN)

    def test_truncated_but_syntactically_valid_json_is_not_accepted(self):
        # read(limit) can return a short body without raising IncompleteRead.
        self.service.replies.append((200, b'{"ok":true}', {'Content-Length': '100'}))
        with self.assertRaisesRegex(HTTPResponseError, 'truncated'):
            self.handler(self.service.origin + '/truncated')

    def test_timeout_is_bounded_and_distinguishes_reads_from_writes(self):
        self.service.delay = .15
        handler = HTTPRequestHandler(HTTPConfig((self.service.origin,), timeout=.03))
        for method, category in (('GET', FailureCategory.TRANSIENT), ('POST', FailureCategory.UNCERTAIN)):
            with self.subTest(method=method), self.assertRaises(HTTPTransportError) as caught:
                handler(self.service.origin + '/slow', method=method)
            self.assertEqual(caught.exception.failure_category, category)
            self.assertIn('timed out', str(caught.exception))

    def test_worker_config_is_validated_and_credentials_are_detached(self):
        headers = {'Authorization': 'Bearer worker-secret'}
        config = HTTPConfig((self.service.origin,), default_headers=headers)
        headers['Authorization'] = 'changed'
        self.assertEqual(config.default_headers['authorization'], 'Bearer worker-secret')
        self.assertNotIn('worker-secret', repr(config))
        with self.assertRaises(TypeError):
            config.default_headers['x'] = 'cannot mutate'
        for kwargs in (dict(allowed_origins=()), dict(allowed_origins=('file:///tmp',)),
                       dict(allowed_origins=(self.service.origin + '/path',)),
                       dict(timeout=True), dict(timeout=0), dict(timeout=float('inf')),
                       dict(max_response_bytes=False), dict(max_request_bytes=0)):
            with self.subTest(fields=list(kwargs)), self.assertRaises((ValueError, ToolInputError)):
                HTTPConfig(**dict({'allowed_origins': (self.service.origin,)}, **kwargs))

    def test_only_selected_response_headers_are_persistable(self):
        self.service.replies.append((200, {}, {'Set-Cookie': 'session=private', 'ETag': 'v1'}))
        result = self.handler(self.service.origin)
        self.assertEqual(result['headers']['etag'], 'v1')
        self.assertNotIn('set-cookie', result['headers'])


if __name__ == '__main__':
    unittest.main()
