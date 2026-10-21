"""Bounded HTTP(S) tool calls, without redirects, proxies or hidden retries."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from http.client import HTTPException
import json
import math
import os
import re
import socket
import ssl
from types import MappingProxyType
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .failures import FailureCategory
from .tools import ToolError, ToolInputError, ToolRegistry, _snapshot


HTTP_HANDLER = 'http.request.v1'
_MISSING = object()
_READ_METHODS = frozenset(('GET', 'HEAD', 'OPTIONS'))
_METHODS = _READ_METHODS | {'POST', 'PUT', 'PATCH', 'DELETE'}
_BLOCKED_HEADERS = {'host', 'content-length', 'transfer-encoding', 'connection',
                    'proxy-authorization', 'proxy-connection', 'upgrade', 'te', 'trailer', 'expect'}
_RESPONSE_HEADERS = {'content-type', 'content-length', 'etag', 'last-modified', 'retry-after'}


def _url(value):
    if (not isinstance(value, str) or not value or
            any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ToolInputError('URL must be an HTTP(S) URL without whitespace or credentials')
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname or
                parsed.username is not None or parsed.password is not None or parsed.fragment):
            raise ValueError
        host = parsed.hostname.encode('idna').decode('ascii').lower()
        port = parsed.port if parsed.port is not None else (443 if parsed.scheme == 'https' else 80)
    except (ValueError, UnicodeError):
        raise ToolInputError('URL must be an HTTP(S) URL without credentials or fragment') from None
    return parsed, (parsed.scheme, host, port)


def _headers(value):
    if not isinstance(value, Mapping):
        raise ToolInputError('headers must map header names to strings')
    result = {}
    for name, content in value.items():
        if (not isinstance(name, str) or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) or
                name.lower() in _BLOCKED_HEADERS or name.lower() in result or
                not isinstance(content, str) or any(ord(c) < 32 or ord(c) > 255 or ord(c) == 127 for c in content)):
            raise ToolInputError('invalid, duplicate or transport-managed HTTP header')
        result[name.lower()] = content
    return result


@dataclass(frozen=True)
class HTTPConfig:
    # Explicit service origins include scheme and effective port. No wildcard.
    allowed_origins: tuple[str, ...]
    timeout: float = 10
    max_request_bytes: int = 1024 * 1024
    max_response_bytes: int = 1024 * 1024
    default_headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    _origins: frozenset = field(init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.allowed_origins, tuple) or not self.allowed_origins:
            raise ValueError('allowed_origins must be a nonempty tuple of service origins')
        origins = set()
        for value in self.allowed_origins:
            parsed, origin = _url(value)
            if parsed.path not in ('', '/') or parsed.query:
                raise ValueError('allowed_origins must contain service origins without paths or queries')
            origins.add(origin)
        object.__setattr__(self, '_origins', frozenset(origins))
        if type(self.timeout) not in (int, float) or not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError('timeout must be finite positive seconds')
        for limit in (self.max_request_bytes, self.max_response_bytes):
            if type(limit) is not int or limit <= 0:
                raise ValueError('HTTP size limits must be positive integers')
        object.__setattr__(self, 'default_headers', MappingProxyType(_headers(self.default_headers)))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> HTTPConfig:
        """Read opt-in HTTP worker settings; never infer origins from DAG input."""
        env = os.environ if environ is None else environ
        try:
            origins = json.loads(env.get('HTTP_ALLOWED_ORIGINS', 'null'))
            if type(origins) is not list:
                raise ValueError
            headers = json.loads(env.get('HTTP_DEFAULT_HEADERS', '{}'))
            return cls(tuple(origins), timeout=float(env.get('HTTP_TIMEOUT', '10')),
                       max_request_bytes=int(env.get('HTTP_MAX_REQUEST_BYTES', str(1024 * 1024))),
                       max_response_bytes=int(env.get('HTTP_MAX_RESPONSE_BYTES', str(1024 * 1024))),
                       default_headers=headers)
        except (ValueError, TypeError, ToolInputError):
            raise ValueError('invalid HTTP worker configuration') from None


class HTTPStatusError(ToolError):
    def __init__(self, status_code: int, method: str):
        self.status_code = status_code
        if status_code in (408, 425, 429) or 500 <= status_code < 600 and status_code not in (501, 505):
            self.failure_category = (FailureCategory.TRANSIENT if method in _READ_METHODS
                                     else FailureCategory.UNCERTAIN)
        else:
            self.failure_category = FailureCategory.PERMANENT
        super().__init__(f'HTTP tool returned status {status_code}')


class HTTPTransportError(ToolError):
    def __init__(self, method: str, *, timeout: bool = False):
        self.failure_category = (FailureCategory.TRANSIENT if method in _READ_METHODS
                                 else FailureCategory.UNCERTAIN)
        super().__init__('HTTP tool request timed out' if timeout else 'HTTP tool transport failed')


class HTTPResponseError(ToolError):
    """A request finished but its output cannot be used; do not replay effects."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTPRequestHandler:
    """One attempt, one request; retries belong to the engine.

    Returns strict JSON with status_code, selected response headers, text and
    json. Credentials are injected from worker config and cannot be overridden.
    response_format='text' avoids JSON decoding; empty 204/HEAD results use null.
    """

    failure_scope = 'tool'

    def __init__(self, config: HTTPConfig):
        if not isinstance(config, HTTPConfig):
            raise TypeError('config must be HTTPConfig')
        self.config = config

    def __call__(self, url: str, *, method: str = 'GET', params: dict | None = None,
                 headers: dict | None = None, json_body=_MISSING, text_body: str | None = None,
                 response_format: str = 'json') -> dict:
        parsed, origin = _url(url)
        if origin not in self.config._origins:
            raise ToolInputError('HTTP origin is not allowed by worker configuration')
        if not isinstance(method, str) or method.upper() not in _METHODS:
            raise ToolInputError('unsupported HTTP method')
        method = method.upper()
        if response_format not in ('json', 'text'):
            raise ToolInputError('response_format must be json or text')
        if params is not None:
            params = _snapshot(params, ToolInputError, 'query parameters must be strict JSON')
            if type(params) is not dict or any(
                    type(part) not in (str, int, float, bool)
                    for value in params.values() for part in (value if type(value) is list else [value])):
                raise ToolInputError('query parameters must contain scalars or lists of scalars')
            try:
                query = urlencode(params, doseq=True)
            except UnicodeError:
                raise ToolInputError('invalid query parameter encoding') from None
            parsed = parsed._replace(query='&'.join(filter(None, (parsed.query, query))))
        request_headers = _headers({} if headers is None else headers)
        if request_headers.keys() & self.config.default_headers.keys():
            raise ToolInputError('request headers cannot override worker-configured headers')
        request_headers.update(self.config.default_headers)
        if json_body is not _MISSING and text_body is not None:
            raise ToolInputError('use only one of json_body and text_body')
        if method in ('GET', 'HEAD') and (json_body is not _MISSING or text_body is not None):
            raise ToolInputError('GET and HEAD cannot have a request body')
        data = None
        try:
            if json_body is not _MISSING:
                payload = _snapshot(json_body, ToolInputError, 'request body must be strict JSON')
                data = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8')
                request_headers.setdefault('content-type', 'application/json')
            elif text_body is not None:
                if not isinstance(text_body, str):
                    raise ToolInputError('text_body must be a string')
                data = text_body.encode('utf-8')
                request_headers.setdefault('content-type', 'text/plain; charset=utf-8')
            if data is not None and len(data) > self.config.max_request_bytes:
                raise ToolInputError('HTTP request body exceeds size limit')
            request = Request(urlunsplit(parsed), data=data, method=method, headers=request_headers)
        except (ValueError, UnicodeError):
            raise ToolInputError('invalid HTTP request encoding') from None
        try:
            # Each concurrent attempt owns its opener and response. Ambient proxy
            # settings cannot redirect an explicitly configured service call.
            with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=self.config.timeout) as response:
                if not 200 <= response.status < 300:
                    raise HTTPStatusError(response.status, method)
                expected_length = response.length
                raw = response.read(self.config.max_response_bytes + 1)
                response_headers = {key.lower(): value for key, value in response.headers.items()
                                    if key.lower() in _RESPONSE_HEADERS}
                charset = response.headers.get_content_charset() or 'utf-8'
                status = response.status
        except HTTPError as error:
            status = error.code
            error.close()
            # Do not persist remote error bodies, URL query secrets or headers.
            raise HTTPStatusError(status, method) from None
        except (ssl.SSLCertVerificationError, ValueError, UnicodeError):
            raise ToolInputError('HTTP URL or TLS configuration is invalid') from None
        except URLError as error:
            if isinstance(error.reason, ssl.SSLCertVerificationError):
                raise ToolInputError('HTTP TLS certificate verification failed') from None
            raise HTTPTransportError(method, timeout=isinstance(error.reason, (TimeoutError, socket.timeout))) from None
        except (OSError, HTTPException) as error:
            raise HTTPTransportError(method, timeout=isinstance(error, TimeoutError)) from None
        if len(raw) > self.config.max_response_bytes:
            raise HTTPResponseError('HTTP response exceeds size limit; outcome may be unknown')
        if expected_length is not None and len(raw) != expected_length:
            raise HTTPResponseError('HTTP response was truncated; outcome may be unknown')
        try:
            text = raw.decode(charset)
            value = json.loads(text) if response_format == 'json' and raw else None
            result = dict(status_code=status, headers=response_headers, text=text, json=value)
            return _snapshot(result, HTTPResponseError, 'HTTP response must be strict JSON')
        except (ValueError, LookupError, RecursionError):
            raise HTTPResponseError('HTTP response has invalid text or JSON; outcome may be unknown') from None


def create_http_registry(config: HTTPConfig) -> dict:
    """Export both http.request.v1 and tool.call.v1 for existing deployment APIs."""
    registry = ToolRegistry()
    registry.register(HTTP_HANDLER, HTTPRequestHandler(config), description='Call a configured HTTP service')
    return registry.to_task_registry()
