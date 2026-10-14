"""OpenAI-compatible text generation handlers for durable DAG nodes.

Credentials live in worker configuration, never in task arguments. Each handler
invocation makes exactly one HTTP request; the engine owns retry scheduling.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from http.client import HTTPException
import json
import math
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


LLM_HANDLER = 'llm.chat.v1'
DEFAULT_BASE_URL = 'https://api.openai.com/v1'


class LLMError(RuntimeError):
    """Sanitized model API failure, safe to persist in task events."""


class LLMHTTPError(LLMError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f'LLM API returned HTTP {status_code}')


class LLMTimeoutError(LLMError):
    """The HTTP client timed out; the provider may still have generated tokens."""


class LLMResponseError(LLMError):
    """The provider did not return a valid text completion."""


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{name} must be a nonempty string')
    return value


@dataclass(frozen=True)
class LLMConfig:
    api_key: str = field(repr=False)
    model: str
    base_url: str = DEFAULT_BASE_URL
    timeout: float = 60
    max_response_bytes: int = 4 * 1024 * 1024

    def __post_init__(self):
        _text(self.api_key, 'api_key')
        if any(ord(char) < 33 or ord(char) > 126 for char in self.api_key):
            raise ValueError('api_key must contain only visible ASCII characters')
        _text(self.model, 'model')
        _text(self.base_url, 'base_url')
        try:
            url = urlsplit(self.base_url)
            valid = (url.scheme in ('http', 'https') and url.hostname and
                     url.username is None and url.password is None and
                     not url.query and not url.fragment)
            url.port  # Validate malformed or out-of-range ports eagerly.
        except ValueError:
            valid = False
        if not valid or any(char.isspace() or ord(char) < 32 for char in self.base_url):
            raise ValueError('base_url must be an HTTP(S) URL without credentials, query or fragment')
        object.__setattr__(self, 'base_url', self.base_url.rstrip('/'))
        if (type(self.timeout) not in (int, float) or
                not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError('timeout must be finite positive seconds')
        if type(self.max_response_bytes) is not int or self.max_response_bytes <= 0:
            raise ValueError('max_response_bytes must be a positive integer')

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> LLMConfig:
        """Read LLM_API_KEY, LLM_MODEL, LLM_BASE_URL and LLM_TIMEOUT.

        Missing key/model fails before workflow submission. No .env file is
        implicitly loaded, and no credentials are written to disk.
        """
        env = os.environ if environ is None else environ
        try:
            timeout = float(env.get('LLM_TIMEOUT', '60'))
        except (TypeError, ValueError):
            raise ValueError('LLM_TIMEOUT must be finite positive seconds') from None
        return cls(api_key=env.get('LLM_API_KEY', ''), model=env.get('LLM_MODEL', ''),
                   base_url=env.get('LLM_BASE_URL', DEFAULT_BASE_URL), timeout=timeout)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward a bearer credential to a redirected endpoint.
        return None


def _reject_constant(value):
    raise ValueError('nonfinite JSON')


class OpenAIChatHandler:
    """Callable text-only, non-streaming Chat Completions adapter.

    Supply either prompt (optionally system_prompt) or a messages list. Output
    is JSON and includes text, model, response_id, finish_reason and usage.
    Missing provider usage stays unknown (null), never a fabricated zero.
    """

    def __init__(self, config: LLMConfig):
        if not isinstance(config, LLMConfig):
            raise TypeError('config must be LLMConfig')
        self.config = config

    def __call__(self, prompt: str | None = None, *, system_prompt: str | None = None,
                 messages: list[dict[str, str]] | None = None, model: str | None = None,
                 temperature: float | None = None, max_tokens: int | None = None,
                 max_completion_tokens: int | None = None) -> dict:
        if messages is None:
            _text(prompt, 'prompt')
            messages = []
            if system_prompt is not None:
                messages.append(dict(role='system', content=_text(system_prompt, 'system_prompt')))
            messages.append(dict(role='user', content=prompt))
        else:
            if prompt is not None or system_prompt is not None:
                raise ValueError('messages cannot be combined with prompt or system_prompt')
            if type(messages) is not list or not messages:
                raise ValueError('messages must be a nonempty list of text messages')
            for message in messages:
                if (type(message) is not dict or set(message) != {'role', 'content'} or
                        message['role'] not in ('system', 'developer', 'user', 'assistant') or
                        not isinstance(message['content'], str)):
                    raise ValueError('each message requires a supported role and string content')
        requested_model = self.config.model if model is None else _text(model, 'model')
        payload = dict(model=requested_model, messages=messages, stream=False)
        if temperature is not None:
            if (type(temperature) not in (int, float) or not math.isfinite(temperature)
                    or not 0 <= temperature <= 2):
                raise ValueError('temperature must be between 0 and 2')
            payload['temperature'] = temperature
        if max_tokens is not None and max_completion_tokens is not None:
            raise ValueError('use only one of max_tokens and max_completion_tokens')
        for name, value in (('max_tokens', max_tokens), ('max_completion_tokens', max_completion_tokens)):
            if value is not None:
                if type(value) is not int or value <= 0:
                    raise ValueError(f'{name} must be a positive integer')
                payload[name] = value
        data = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8')
        request = Request(self.config.base_url + '/chat/completions', data=data, method='POST',
                          headers={'Authorization': 'Bearer ' + self.config.api_key,
                                   'Content-Type': 'application/json', 'Accept': 'application/json'})
        try:
            # Each call owns its opener and response, including concurrent DAG branches.
            with build_opener(_NoRedirect()).open(request, timeout=self.config.timeout) as response:
                if response.status != 200:
                    raise LLMHTTPError(response.status)
                raw = response.read(self.config.max_response_bytes + 1)
        except HTTPError as error:
            status = error.code
            error.close()
            # Error bodies/URLs may echo credentials; persist only the status code.
            raise LLMHTTPError(status) from None
        except TimeoutError:
            raise LLMTimeoutError('LLM API request timed out; outcome may be unknown') from None
        except URLError as error:
            if isinstance(error.reason, TimeoutError):
                raise LLMTimeoutError('LLM API request timed out; outcome may be unknown') from None
            raise LLMError('LLM API connection failed; outcome may be unknown') from None
        except (OSError, HTTPException, ValueError):
            raise LLMError('LLM API transport failed; outcome may be unknown') from None
        if len(raw) > self.config.max_response_bytes:
            raise LLMResponseError('LLM API response exceeds max_response_bytes')
        try:
            value = json.loads(raw, parse_constant=_reject_constant)
            choice = value['choices'][0]
            text = choice['message']['content']
            if not isinstance(text, str):
                raise ValueError('missing text')
            usage = value.get('usage')
            if usage is not None and type(usage) is not dict:
                raise ValueError('invalid usage')
            usage = dict(usage or {})
            for key in ('prompt_tokens', 'completion_tokens', 'total_tokens'):
                count = usage.setdefault(key, None)
                if count is not None and (type(count) is not int or count < 0):
                    raise ValueError('invalid token count')
            for field_value in (value.get('id'), value.get('model'), choice.get('finish_reason')):
                if field_value is not None and not isinstance(field_value, str):
                    raise ValueError('invalid response metadata')
            # Preserve optional provider usage details (cached/reasoning tokens).
            json.dumps(usage, allow_nan=False)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError, RecursionError):
            raise LLMResponseError('LLM API returned an invalid text completion or token usage') from None
        return dict(text=text, model=value.get('model') or requested_model,
                    requested_model=requested_model, response_id=value.get('id'),
                    finish_reason=choice.get('finish_reason'), usage=usage)


def create_llm_registry(config: LLMConfig | None = None) -> dict[str, OpenAIChatHandler]:
    """Registry factory also usable by workflow-management --registry."""
    return {LLM_HANDLER: OpenAIChatHandler(config if config is not None else LLMConfig.from_env())}
