# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping
from inspect import isawaitable, iscoroutinefunction
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

from agents.tracing import get_current_span
from agents.tracing.span_data import FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData
from openai import NotGiven, Omit

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import get_value
from amazon.opentelemetry.distro.instrumentation.openai_agents._shared import _TelemetryHelpers
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import Span
from opentelemetry.util.types import AttributeValue


class OpenAIAgentWrapper:
    """Wrap GenAI calls to capture details that OpenAI Agents tracing callbacks do not expose."""

    def __init__(self, processor: Any) -> None:
        self._processor = processor

    def capture_openai_request_attributes(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Responses and Chat Completions create methods to capture request attributes."""
        openai_span = get_current_span()
        span_data = get_value(openai_span, "span_data")
        if not isinstance(span_data, (GenerationSpanData, ResponseSpanData)):
            return wrapped(*args, **kwargs)
        if (
            isinstance(span_data, GenerationSpanData)
            and get_value(get_value(span_data, "model_config"), "model_impl") == "litellm"
        ):
            return wrapped(*args, **kwargs)
        span = self._processor.get_otel_span(openai_span.span_id)
        self._capture_llm_request_attributes(span, instance, kwargs)
        return wrapped(*args, **kwargs)

    @staticmethod
    def capture_litellm_request_model_config(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap model_config_for_trace to preserve LiteLLM provider configuration attributes."""
        config = wrapped(*args, **kwargs)
        model_settings = kwargs.get("model_settings") or (args[0] if args else None)
        extra_args = get_value(model_settings, "extra_args")
        if isinstance(config, dict) and isinstance(extra_args, Mapping):
            for key in ("api_base", "base_url", "custom_llm_provider"):
                value = extra_args.get(key)
                if value is not None and key in ("api_base", "base_url"):
                    try:
                        parts = urlsplit(str(value))
                        value = urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, "", ""))
                    except ValueError:
                        value = None
                if value is not None and not config.get(key):
                    config[key] = value
        return config

    def capture_litellm_completion_attributes(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap LiteLLM completion and acompletion to capture request and response attributes."""
        if kwargs.get("acompletion") and not iscoroutinefunction(wrapped):
            return wrapped(*args, **kwargs)
        openai_span = get_current_span()
        if not isinstance(get_value(openai_span, "span_data"), (GenerationSpanData, ResponseSpanData)):
            return wrapped(*args, **kwargs)
        span = self._processor.get_otel_span(openai_span.span_id)
        if span is None:
            return wrapped(*args, **kwargs)
        self._capture_llm_request_attributes(span, instance, kwargs)
        response = wrapped(*args, **kwargs)
        if isawaitable(response):
            return self._capture_litellm_acompletion_response_attributes(span, response, kwargs.get("stream"))
        if kwargs.get("stream") and hasattr(response, "__iter__"):
            return self._capture_litellm_completion_stream_response_attributes(span, response)
        self._capture_llm_response_attributes(span, response)
        return response

    async def _capture_litellm_acompletion_response_attributes(self, span: Span, response: Any, stream: Any) -> Any:
        awaited_response = await response
        if stream and hasattr(awaited_response, "__aiter__"):
            return self._capture_litellm_acompletion_stream_response_attributes(span, awaited_response)
        self._capture_llm_response_attributes(span, awaited_response)
        return awaited_response

    def _capture_litellm_completion_stream_response_attributes(self, span: Span, stream: Any) -> Any:
        try:
            for chunk in stream:
                self._capture_llm_response_attributes(span, chunk)
                yield chunk
        finally:
            close = get_value(stream, "close")
            if callable(close):
                close()

    async def _capture_litellm_acompletion_stream_response_attributes(self, span: Span, stream: Any) -> Any:
        try:
            async for chunk in stream:
                self._capture_llm_response_attributes(span, chunk)
                yield chunk
        finally:
            close = get_value(stream, "aclose")
            if callable(close):
                await close()

    def capture_tool_call_attributes(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Agents tool-call helpers to capture the tool name and call ID attributes."""
        openai_span = get_current_span()
        span_data = get_value(openai_span, "span_data")
        if not isinstance(span_data, (FunctionSpanData, HandoffSpanData)):
            return wrapped(*args, **kwargs)
        span = self._processor.get_otel_span(openai_span.span_id)
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = get_value(tool_call, "name")
        call_id = get_value(tool_call, "call_id")
        if span is not None and (name or call_id):
            attributes: dict[str, AttributeValue] = {}
            _TelemetryHelpers.set_attribute(attributes, GEN_AI_TOOL_CALL_ID, call_id)
            if isinstance(span_data, HandoffSpanData):
                _TelemetryHelpers.set_attribute(attributes, GEN_AI_TOOL_NAME, name)
                if name:
                    span.update_name(f"{GenAiOperationNameValues.EXECUTE_TOOL.value} {name}")
            span.set_attributes(attributes)
        return wrapped(*args, **kwargs)

    @staticmethod
    def _capture_llm_request_attributes(span: Optional[Span], instance: Any, kwargs: Any) -> None:
        if span is None or not span.is_recording():
            return
        params = {
            key: value
            for key, value in kwargs.items()
            if value is not None
            and not isinstance(value, (NotGiven, Omit))
            and key
            in {
                "api_base",
                "base_url",
                "choice_count",
                "custom_llm_provider",
                "frequency_penalty",
                "functions",
                "max_completion_tokens",
                "max_output_tokens",
                "max_tokens",
                "modalities",
                "model",
                "n",
                "parallel_tool_calls",
                "presence_penalty",
                "reasoning",
                "reasoning_effort",
                "response_format",
                "seed",
                "service_tier",
                "stop",
                "stop_sequences",
                "store",
                "stream",
                "temperature",
                "text",
                "tool_choice",
                "tools",
                "top_k",
                "top_logprobs",
                "top_p",
                "truncation",
                "verbosity",
            }
        }
        base_url = get_value(get_value(instance, "_client"), "base_url")
        if base_url is not None:
            params["base_url"] = str(base_url)

        attributes: dict[str, AttributeValue] = {}
        request_model = params.get("model")
        _TelemetryHelpers.set_attribute(attributes, GEN_AI_REQUEST_MODEL, request_model)
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_PROVIDER_NAME,
            _TelemetryHelpers.resolve_provider(
                request_model,
                params.get("base_url") or params.get("api_base"),
                params.get("custom_llm_provider"),
            ),
        )
        _TelemetryHelpers.set_request_attributes(attributes, params)
        if request_model:
            operation = str(get_value(span, "name")).split(" ", 1)[0]
            span.update_name(f"{operation} {request_model}")
        span.set_attributes(attributes)

    @staticmethod
    def _capture_llm_response_attributes(span: Span, response: Any) -> None:
        if not span.is_recording():
            return

        attributes: dict[str, AttributeValue] = {}
        _TelemetryHelpers.set_response_attributes(attributes, response)
        usage = get_value(response, "usage")
        _TelemetryHelpers.set_usage_attributes(attributes, usage, usage)
        current_attributes = get_value(span, "attributes") or {}
        finish_reasons = list(get_value(current_attributes, GEN_AI_RESPONSE_FINISH_REASONS) or ())
        finish_reasons.extend(
            _TelemetryHelpers.get_finish_reasons(
                response,
                {**current_attributes, **attributes},
            )
        )
        _TelemetryHelpers.set_attribute(
            attributes,
            GEN_AI_RESPONSE_FINISH_REASONS,
            finish_reasons or None,
        )
        span.set_attributes(attributes)
