# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import threading
from contextvars import Token
from functools import wraps
from typing import Any, Callable, Dict, Optional

from wrapt import wrap_function_wrapper

from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import GenAiProviderNameValues

_logger = logging.getLogger(__name__)

PROVIDER_MAP = {
    "bedrock": GenAiProviderNameValues.AWS_BEDROCK.value,
    "aws": GenAiProviderNameValues.AWS_BEDROCK.value,
    "openai": GenAiProviderNameValues.OPENAI.value,
    "anthropic": GenAiProviderNameValues.ANTHROPIC.value,
    "claude": GenAiProviderNameValues.ANTHROPIC.value,
    "azure": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "azure_openai": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "google": GenAiProviderNameValues.GCP_GEN_AI.value,
    "vertex": GenAiProviderNameValues.GCP_VERTEX_AI.value,
    "gemini": GenAiProviderNameValues.GCP_GEMINI.value,
    "cohere": GenAiProviderNameValues.COHERE.value,
    "mistral": GenAiProviderNameValues.MISTRAL_AI.value,
    "groq": GenAiProviderNameValues.GROQ.value,
    "deepseek": GenAiProviderNameValues.DEEPSEEK.value,
    "perplexity": GenAiProviderNameValues.PERPLEXITY.value,
}


class DictWithLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._data: Dict[Any, Any] = {}

    def get(self, key: Any) -> Any:
        with self._lock:
            return self._data.get(key)

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def pop(self, key: Any) -> Any:
        with self._lock:
            return self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __contains__(self, key: Any) -> bool:
        with self._lock:
            return key in self._data

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


def serialize_to_json_string(value: Any, max_depth: int = 10) -> str:
    json_safe_types = (str, int, float, bool, dict, list, tuple, type(None))

    def _sanitize(obj: Any, depth: int) -> Any:
        if depth <= 0:
            return "..."
        if isinstance(obj, dict):
            return {k: _sanitize(v, depth - 1) for k, v in obj.items() if isinstance(v, json_safe_types)}
        if isinstance(obj, (list, tuple)):
            return [_sanitize(item, depth - 1) for item in obj if isinstance(item, json_safe_types)]
        return obj

    try:
        return json.dumps(_sanitize(value, max_depth))
    except (TypeError, ValueError):
        return str(value)


def try_wrap(
    module: str, name: str, wrapper: Callable[..., Any], should_wrap: Optional[Callable[..., bool]] = None
) -> None:
    try:
        if should_wrap is not None and not should_wrap():
            return
        wrap_function_wrapper(module, name, wrapper)
    except Exception:  # pylint: disable=broad-except
        _logger.debug("Failed to wrap %s.%s, instrumentation may be incomplete", module, name)


def try_unwrap(module: Any, name: str) -> None:
    try:
        unwrap(module, name)
    except Exception:  # pylint: disable=broad-except
        _logger.debug("Failed to unwrap %s.%s", module, name)


def skip_instrumentation_if_suppressed(fn: Callable) -> Callable:
    if not callable(fn):
        return fn

    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return None
        return fn(*args, **kwargs)

    return wrapper


def try_detach(token: Token) -> None:
    # context.detach() fails when it run in different async
    # contexts, if there's failure in detaching we should just pass
    # the exception as there is no longer the active context where it's either already been
    # garbage collected or will be when its async scope ends.
    # https://github.com/open-telemetry/opentelemetry-python/issues/2606
    try:
        context._RUNTIME_CONTEXT.detach(token)  # pylint: disable=protected-access
    except Exception:  # pylint: disable=broad-except
        pass
