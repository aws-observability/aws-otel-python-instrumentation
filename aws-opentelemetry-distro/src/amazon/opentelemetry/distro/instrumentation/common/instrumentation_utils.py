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

# TODO: Remove these constants once the OTel semantic conventions release includes invoke_workflow.
# https://github.com/open-telemetry/semantic-conventions/pull/3249
GEN_AI_WORKFLOW_NAME = "gen_ai.workflow.name"
OPERATION_INVOKE_WORKFLOW = "invoke_workflow"

PROVIDER_MAP = {
    "bedrock": GenAiProviderNameValues.AWS_BEDROCK.value,
    "aws": GenAiProviderNameValues.AWS_BEDROCK.value,
    "langchain_aws": GenAiProviderNameValues.AWS_BEDROCK.value,
    "openai": GenAiProviderNameValues.OPENAI.value,
    "anthropic": GenAiProviderNameValues.ANTHROPIC.value,
    "claude": GenAiProviderNameValues.ANTHROPIC.value,
    "azure": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "azure_openai": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "google": GenAiProviderNameValues.GCP_GEN_AI.value,
    "langchain_google_genai": GenAiProviderNameValues.GCP_GEN_AI.value,
    "vertex": GenAiProviderNameValues.GCP_VERTEX_AI.value,
    "vertexai": GenAiProviderNameValues.GCP_VERTEX_AI.value,
    "gemini": GenAiProviderNameValues.GCP_GEMINI.value,
    "cohere": GenAiProviderNameValues.COHERE.value,
    "langchain_cohere": GenAiProviderNameValues.COHERE.value,
    "mistral": GenAiProviderNameValues.MISTRAL_AI.value,
    "mistralai": GenAiProviderNameValues.MISTRAL_AI.value,
    "groq": GenAiProviderNameValues.GROQ.value,
    "langchain_groq": GenAiProviderNameValues.GROQ.value,
    "deepseek": GenAiProviderNameValues.DEEPSEEK.value,
    "langchain_deepseek": GenAiProviderNameValues.DEEPSEEK.value,
    "perplexity": GenAiProviderNameValues.PERPLEXITY.value,
    "xai": GenAiProviderNameValues.X_AI.value,
    "langchain_xai": GenAiProviderNameValues.X_AI.value,
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


def content_to_parts(content: Any) -> list:  # pylint: disable=too-many-branches
    """Convert a GenAI message's content into GenAI message parts, mapping each block to
    its typed part per the input and output message schemas:
    https://github.com/open-telemetry/semantic-conventions-genai/blob/main/model/gen-ai/gen-ai-input-messages.json
    https://github.com/open-telemetry/semantic-conventions-genai/blob/main/model/gen-ai/gen-ai-output-messages.json
    """
    if isinstance(content, str):
        return [{"type": "text", "content": content}] if content else []
    if isinstance(content, dict):
        content = [content]
    elif not isinstance(content, list):
        return [{"type": "text", "content": str(content)}] if content else []

    parts: list = []
    for block in content:
        if isinstance(block, str):
            if block:
                parts.append({"type": "text", "content": block})
            continue
        if not isinstance(block, dict):
            parts.append({"type": "text", "content": str(block)})
            continue

        block_type = block.get("type", "")
        if block_type == "text":
            text = block.get("text", "")
            if text:
                parts.append({"type": "text", "content": str(text)})
        elif block_type in ("thinking", "reasoning"):
            reasoning = block.get("thinking") or block.get("reasoning") or block.get("content") or ""
            if reasoning:
                parts.append({"type": "reasoning", "content": str(reasoning)})
        elif block_type == "image_url":
            url = (block.get("image_url") or {}).get("url", "")
            if not url:
                continue
            if url.startswith("data:"):
                mime = url[len("data:") :].split(",", 1)[0].split(";", 1)[0] or "image/*"
                parts.append({"type": "blob", "modality": "image", "mime_type": mime, "content": url})
            else:
                parts.append({"type": "uri", "modality": "image", "uri": url})
        elif block_type == "image":
            parts.append(
                {
                    "type": "blob",
                    "modality": "image",
                    "mime_type": block.get("media_type") or block.get("mime_type") or "image/*",
                    "content": block.get("data", ""),
                }
            )
        else:
            part = dict(block)
            part["type"] = block_type or "text"
            parts.append(part)
    return parts


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
