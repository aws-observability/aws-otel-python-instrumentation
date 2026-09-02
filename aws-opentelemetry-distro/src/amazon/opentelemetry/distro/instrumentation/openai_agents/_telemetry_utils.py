# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from collections.abc import Mapping, Sequence
from typing import Any, Optional
from urllib.parse import urlparse

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    PROVIDER_MAP,
    first_not_none,
    get_value,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOutputTypeValues,
    GenAiProviderNameValues,
)
from opentelemetry.util.types import AttributeValue


def get_usage_value(usages: Sequence[Any], *paths: tuple[str, ...]) -> Any:
    for usage in usages:
        for path in paths:
            value = usage
            for key in path:
                value = get_value(value, key)
                if value is None:
                    break
            if value is not None:
                return value
    return None


def has_usage(usage: Any) -> bool:
    for path in (
        ("input_tokens",),
        ("prompt_tokens",),
        ("output_tokens",),
        ("completion_tokens",),
        ("total_tokens",),
        ("input_tokens_details", "cached_tokens"),
        ("prompt_tokens_details", "cached_tokens"),
        ("input_tokens_details", "cache_write_tokens"),
        ("prompt_tokens_details", "cache_write_tokens"),
        ("output_tokens_details", "reasoning_tokens"),
        ("completion_tokens_details", "reasoning_tokens"),
        ("cache_read_input_tokens",),
        ("cache_creation_input_tokens",),
        ("reasoning_tokens",),
    ):
        value = usage
        for key in path:
            value = get_value(value, key)
            if value is None:
                break
        if value not in (None, 0):
            return True
    return False


def get_finish_reasons(response: Any, attributes: Mapping[str, AttributeValue]) -> list[str]:
    reasons = _to_item_list(get_value(response, "finish_reasons"))
    reasons.extend(
        get_value(choice, "finish_reason")
        for choice in _to_item_list(get_value(response, "choices"))
        if get_value(choice, "finish_reason") is not None
    )
    normalized_reasons = [
        {
            "end_turn": "stop",
            "stop": "stop",
            "tool_calls": "tool_call",
            "tool_use": "tool_call",
            "max_tokens": "length",
            "length": "length",
            "content_filter": "content_filter",
        }.get(str(reason), str(reason))
        for reason in reasons
    ]
    if normalized_reasons:
        return list(dict.fromkeys(normalized_reasons))

    reason = first_not_none(
        get_value(response, "incomplete_reason"),
        get_value(get_value(response, "incomplete_details"), "reason"),
    )
    if reason:
        return [{"max_output_tokens": "length", "content_filter": "content_filter"}.get(str(reason), str(reason))]
    if get_value(response, "status") in ("failed", "cancelled"):
        return ["error"]
    max_tokens = attributes.get(GEN_AI_REQUEST_MAX_TOKENS)
    output_tokens = attributes.get(GEN_AI_USAGE_OUTPUT_TOKENS)
    if isinstance(max_tokens, int) and isinstance(output_tokens, int) and output_tokens >= max_tokens > 0:
        return ["length"]
    return []


def get_response_request_config(response: Any) -> dict[str, Any]:
    if response is None:
        return {}
    model_config: dict[str, Any] = {}
    for key in ("temperature", "top_p", "max_output_tokens", "top_logprobs", "tools", "text"):
        value = get_value(response, key)
        if value is not None:
            model_config[key] = value
    return model_config


def get_request_output_type(model_config: Mapping[str, Any]) -> Optional[str]:
    response_format = first_not_none(
        get_value(model_config.get("text"), "format"),
        model_config.get("response_format"),
    )
    format_type = first_not_none(get_value(response_format, "type"), response_format)
    if not isinstance(format_type, str):
        return None
    return {
        "text": GenAiOutputTypeValues.TEXT.value,
        "json": GenAiOutputTypeValues.JSON.value,
        "json_object": GenAiOutputTypeValues.JSON.value,
        "json_schema": GenAiOutputTypeValues.JSON.value,
    }.get(format_type)


def get_agent_output_type(output_type: Any) -> Optional[str]:
    if not output_type:
        return None
    if str(output_type) in ("str", "text"):
        return GenAiOutputTypeValues.TEXT.value
    return GenAiOutputTypeValues.JSON.value


def resolve_provider(model: Any, base_url: Any, provider_hint: Any = None) -> str:
    explicit_candidates: list[str] = []
    if provider_hint:
        explicit_candidates.append(str(provider_hint))
    if model and "/" in str(model):
        explicit_candidates.append(str(model).split("/", 1)[0])
    for candidate in explicit_candidates:
        normalized_candidate = re.sub(r"[^a-z0-9._-]+", "_", candidate.strip().lower())
        if normalized_candidate:
            return PROVIDER_MAP.get(normalized_candidate, normalized_candidate)

    if base_url:
        hostname = urlparse(str(base_url)).hostname or ""
        if hostname.endswith((".services.ai.azure.com", ".models.ai.azure.com")):
            return GenAiProviderNameValues.AZURE_AI_INFERENCE.value
        for label in reversed(hostname.split(".")):
            for candidate in label.split("-"):
                provider = PROVIDER_MAP.get(candidate.lower())
                if provider is not None:
                    return provider
    return GenAiProviderNameValues.OPENAI.value


def _to_item_list(items: Any) -> list[Any]:
    if items is None:
        return []
    if isinstance(items, (str, bytes, Mapping)):
        return [items]
    if isinstance(items, Sequence):
        return list(items)
    return [items]
