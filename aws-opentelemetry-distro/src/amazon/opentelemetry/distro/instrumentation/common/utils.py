# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from typing import Any, Callable, Optional

from wrapt import wrap_function_wrapper

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


def serialize_to_json(value: Any, max_depth: int = 3) -> str:
    """Serialize a value to JSON string with depth truncation."""

    def _truncate(obj: Any, depth: int) -> Any:
        if depth <= 0:
            return "..."
        if isinstance(obj, dict):
            return {k: _truncate(v, depth - 1) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_truncate(item, depth - 1) for item in obj]
        return obj

    try:
        return json.dumps(_truncate(value, max_depth))
    except (TypeError, ValueError):
        return str(value)


def try_wrap(
    module: str, name: str, wrapper: Callable[..., Any], should_wrap: Optional[Callable[..., bool]] = None
) -> None:
    if should_wrap is not None and not should_wrap():
        return
    try:
        wrap_function_wrapper(module, name, wrapper)
    except Exception:  # pylint: disable=broad-except
        _logger.debug("Failed to wrap %s.%s, instrumentation may be incomplete", module, name)


def try_unwrap(module: Any, name: str) -> None:
    try:
        unwrap(module, name)
    except Exception:  # pylint: disable=broad-except
        _logger.debug("Failed to unwrap %s.%s", module, name)
