# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from collections.abc import Mapping, Sequence
from typing import Any, Optional
from urllib.parse import urlparse

from pydantic import BaseModel

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    PROVIDER_MAP,
    first_not_none,
    get_value,
    serialize_to_json_string,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPENAI_REQUEST_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT,
    GEN_AI_OUTPUT_TYPE,
    GEN_AI_REQUEST_CHOICE_COUNT,
    GEN_AI_REQUEST_FREQUENCY_PENALTY,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_PRESENCE_PENALTY,
    GEN_AI_REQUEST_SEED,
    GEN_AI_REQUEST_STOP_SEQUENCES,
    GEN_AI_REQUEST_STREAM,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_K,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
    GenAiOutputTypeValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.server_attributes import SERVER_ADDRESS, SERVER_PORT
from opentelemetry.util.types import AttributeValue

GEN_AI_REQUEST_REASONING_LEVEL = "gen_ai.request.reasoning.level"


class _TelemetryHelpers:
    @staticmethod
    def set_request_attributes(attributes: dict[str, AttributeValue], model_config: Mapping[str, Any]) -> None:
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            model_config.get("temperature"),
        )
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_REQUEST_TOP_P, model_config.get("top_p"))
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_REQUEST_TOP_K, model_config.get("top_k"))
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            first_not_none(
                model_config.get("max_tokens"),
                model_config.get("max_output_tokens"),
                model_config.get("max_completion_tokens"),
            ),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_FREQUENCY_PENALTY,
            model_config.get("frequency_penalty"),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_PRESENCE_PENALTY,
            model_config.get("presence_penalty"),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            _TelemetryHelpers._to_string_sequence(
                first_not_none(model_config.get("stop_sequences"), model_config.get("stop"))
            ),
        )
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_REQUEST_SEED, model_config.get("seed"))
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_CHOICE_COUNT,
            first_not_none(model_config.get("choice_count"), model_config.get("n")),
        )
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_REQUEST_STREAM, model_config.get("stream"))
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_OPENAI_REQUEST_SERVICE_TIER,
            model_config.get("service_tier"),
        )

        base_url = model_config.get("base_url") or model_config.get("api_base")
        if base_url:
            parsed_url = urlparse(str(base_url))
            _TelemetryHelpers.set_attribute(attributes, SERVER_ADDRESS, parsed_url.hostname)
            _TelemetryHelpers.set_attribute(attributes, SERVER_PORT, parsed_url.port)

        _TelemetryHelpers.set_tool_definitions(
            attributes,
            model_config.get("tools") or model_config.get("functions"),
        )
        _TelemetryHelpers.set_attribute(
            attributes, GEN_AI_OUTPUT_TYPE, _TelemetryHelpers._get_request_output_type(model_config)
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_REQUEST_REASONING_LEVEL,
            first_not_none(model_config.get("reasoning_effort"), get_value(model_config.get("reasoning"), "effort")),
        )

    @staticmethod
    def set_response_attributes(attributes: dict[str, AttributeValue], response: Any) -> None:
        response_id = get_value(response, "id")
        _TelemetryHelpers.set_attribute(
            attributes, GEN_AI_RESPONSE_ID, None if response_id in ("__fake_id__", "") else response_id
        )
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_RESPONSE_MODEL, get_value(response, "model"))
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_OPENAI_RESPONSE_SERVICE_TIER,
            get_value(response, "service_tier"),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT,
            get_value(response, "system_fingerprint"),
        )

    @staticmethod
    def set_usage_attributes(attributes: dict[str, AttributeValue], usage: Any, detailed_usage: Any = None) -> None:
        detailed_usage = first_not_none(detailed_usage, usage)
        input_details = first_not_none(
            get_value(detailed_usage, "input_tokens_details"),
            get_value(detailed_usage, "prompt_tokens_details"),
            get_value(usage, "input_tokens_details"),
            get_value(usage, "prompt_tokens_details"),
        )
        output_details = first_not_none(
            get_value(detailed_usage, "output_tokens_details"),
            get_value(detailed_usage, "completion_tokens_details"),
            get_value(usage, "output_tokens_details"),
            get_value(usage, "completion_tokens_details"),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_USAGE_INPUT_TOKENS,
            first_not_none(
                get_value(usage, "input_tokens"),
                get_value(usage, "prompt_tokens"),
                get_value(detailed_usage, "input_tokens"),
                get_value(detailed_usage, "prompt_tokens"),
            ),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_USAGE_OUTPUT_TOKENS,
            first_not_none(
                get_value(usage, "output_tokens"),
                get_value(usage, "completion_tokens"),
                get_value(detailed_usage, "output_tokens"),
                get_value(detailed_usage, "completion_tokens"),
            ),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
            first_not_none(
                get_value(input_details, "cached_tokens"),
                get_value(detailed_usage, "cache_read_input_tokens"),
                get_value(usage, "cache_read_input_tokens"),
            ),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
            first_not_none(
                get_value(input_details, "cache_write_tokens"),
                get_value(detailed_usage, "cache_write_input_tokens"),
                get_value(detailed_usage, "cache_creation_input_tokens"),
                get_value(usage, "cache_write_input_tokens"),
                get_value(usage, "cache_creation_input_tokens"),
            ),
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
            first_not_none(
                get_value(output_details, "reasoning_tokens"),
                get_value(detailed_usage, "reasoning_tokens"),
                get_value(usage, "reasoning_tokens"),
            ),
        )

    @staticmethod
    def set_tool_definitions(attributes: dict[str, AttributeValue], tools: Any) -> None:
        definitions: list[dict[str, Any]] = []
        for tool in _TelemetryHelpers._to_item_list(tools):
            if isinstance(tool, str):
                definitions.append({"type": "function", "name": tool})
                continue
            tool_data = _TelemetryHelpers._to_mapping(tool)
            if tool_data is None:
                continue
            function = _TelemetryHelpers._to_mapping(tool_data.get("function")) or {}
            definition: dict[str, Any] = {"type": tool_data.get("type") or "function"}
            for key, source_key in (("name", "name"), ("description", "description"), ("parameters", "parameters")):
                value = first_not_none(tool_data.get(source_key), function.get(source_key))
                if value is not None:
                    definition[key] = value
            definitions.append(definition)
        if definitions:
            attributes[GEN_AI_TOOL_DEFINITIONS] = serialize_to_json_string(definitions)

    @staticmethod
    def _get_request_output_type(model_config: Mapping[str, Any]) -> Optional[str]:
        modalities = _TelemetryHelpers._to_string_sequence(model_config.get("modalities")) or []
        if "audio" in modalities:
            return GenAiOutputTypeValues.SPEECH.value
        if "image" in modalities:
            return GenAiOutputTypeValues.IMAGE.value
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

    @staticmethod
    def get_finish_reasons(response: Any, attributes: Mapping[str, AttributeValue]) -> list[str]:
        reasons = _TelemetryHelpers._to_item_list(get_value(response, "finish_reasons"))
        reasons.extend(
            get_value(choice, "finish_reason")
            for choice in _TelemetryHelpers._to_item_list(get_value(response, "choices"))
        )
        finish_reason = get_value(response, "finish_reason")
        if finish_reason is not None:
            reasons.append(finish_reason)
        normalized = [
            _TelemetryHelpers._normalize_finish_reason({"finish_reason": reason}, False)
            for reason in reasons
            if reason is not None
        ]
        if not normalized:
            fallback = _TelemetryHelpers.get_finish_reason(response, attributes)
            if fallback is not None:
                normalized.append(fallback)
        return list(dict.fromkeys(normalized))

    @staticmethod
    def get_finish_reason(response: Any, attributes: Mapping[str, AttributeValue]) -> Optional[str]:
        reasons = _TelemetryHelpers._to_item_list(get_value(response, "finish_reasons"))
        if not reasons:
            reasons = [
                get_value(choice, "finish_reason")
                for choice in _TelemetryHelpers._to_item_list(get_value(response, "choices"))
            ]
        for reason in reasons:
            if reason is not None:
                return _TelemetryHelpers._normalize_finish_reason({"finish_reason": reason}, False)

        reason = first_not_none(
            get_value(response, "incomplete_reason"),
            get_value(get_value(response, "incomplete_details"), "reason"),
        )
        if reason:
            return _TelemetryHelpers._normalize_finish_reason({"finish_reason": reason}, False)
        if get_value(response, "status") in ("failed", "cancelled"):
            return "error"
        max_tokens = attributes.get(GEN_AI_REQUEST_MAX_TOKENS)
        output_tokens = attributes.get(GEN_AI_USAGE_OUTPUT_TOKENS)
        if isinstance(max_tokens, int) and isinstance(output_tokens, int) and output_tokens >= max_tokens > 0:
            return "length"
        return None

    @staticmethod
    def _normalize_finish_reason(
        item: Mapping[str, Any],
        has_tool_calls: bool,
        default_finish_reason: Optional[str] = None,
    ) -> str:
        if has_tool_calls:
            return "tool_call"
        raw_reason = item.get("finish_reason") or item.get("stop_reason")
        if raw_reason is None and item.get("status") == "incomplete":
            return "length"
        if raw_reason is None:
            return default_finish_reason or "stop"
        return {
            "end_turn": "stop",
            "stop": "stop",
            "tool_calls": "tool_call",
            "tool_use": "tool_call",
            "max_tokens": "length",
            "max_output_tokens": "length",
            "length": "length",
            "content_filter": "content_filter",
        }.get(str(raw_reason), str(raw_reason))

    @staticmethod
    def resolve_provider(model: Any, base_url: Any, custom_provider: Any = None) -> str:
        if custom_provider:
            candidate = re.sub(r"[^a-z0-9._-]+", "_", str(custom_provider).strip().lower())
            return PROVIDER_MAP.get(candidate, candidate)
        if model and "/" in str(model):
            candidate = re.sub(r"[^a-z0-9._-]+", "_", str(model).split("/", 1)[0].strip().lower())
            return PROVIDER_MAP.get(candidate, candidate)
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

    @staticmethod
    def _to_string_sequence(value: Any) -> Optional[list[str]]:
        if value is None:
            return None
        if isinstance(value, (str, bytes)):
            return [str(value)]
        if isinstance(value, Sequence):
            return [str(item) for item in value]
        return [str(value)]

    @staticmethod
    def _to_item_list(items: Any) -> list[Any]:
        if items is None:
            return []
        if isinstance(items, (str, bytes, Mapping, BaseModel)):
            return [items]
        if isinstance(items, Sequence):
            return list(items)
        return [items]

    @staticmethod
    def _to_mapping(value: Any) -> Optional[dict[str, Any]]:
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, BaseModel):
            return value.model_dump()
        return None

    @staticmethod
    def set_attribute(attributes: dict[str, AttributeValue], key: str, value: Any) -> None:
        if isinstance(value, (bool, bytes, float, int, str, list, tuple)):
            attributes[key] = value
