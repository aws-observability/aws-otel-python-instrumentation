# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable, Mapping
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

from agents.tracing import get_current_span
from agents.tracing.span_data import FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData
from openai import NotGiven, Omit

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import get_value
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


class GenAIContextCapture:
    """Add GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    def __init__(self, get_otel_span: Callable[[str], Optional[Span]]) -> None:
        self._get_otel_span = get_otel_span

    def record_openai_request(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        openai_span = get_current_span()
        span_data = get_value(openai_span, "span_data")
        if (
            isinstance(span_data, GenerationSpanData)
            and get_value(get_value(span_data, "model_config"), "model_impl") == "litellm"
        ):
            return wrapped(*args, **kwargs)
        span = self._resolve_current_span((GenerationSpanData, ResponseSpanData))
        self._capture_model_request_params(span, instance, kwargs)
        return wrapped(*args, **kwargs)

    @staticmethod
    def record_litellm_model_config(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
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

    def record_litellm_completion(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        if kwargs.get("acompletion"):
            return wrapped(*args, **kwargs)
        span = self._resolve_current_span((GenerationSpanData, ResponseSpanData))
        if span is None:
            return wrapped(*args, **kwargs)
        self._capture_model_request_params(span, instance, kwargs)
        response = wrapped(*args, **kwargs)
        if kwargs.get("stream") and hasattr(response, "__iter__"):
            return self._capture_litellm_completion_stream(span, response)
        self._set_model_span_attributes(span, response=response)
        return response

    async def record_litellm_acompletion(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        span = self._resolve_current_span((GenerationSpanData, ResponseSpanData))
        if span is None:
            return await wrapped(*args, **kwargs)
        self._capture_model_request_params(span, instance, kwargs)
        response = await wrapped(*args, **kwargs)
        if kwargs.get("stream") and hasattr(response, "__aiter__"):
            return self._capture_litellm_acompletion_stream(span, response)
        self._set_model_span_attributes(span, response=response)
        return response

    @classmethod
    def _capture_litellm_completion_stream(cls, span: Span, stream: Any) -> Any:
        try:
            for chunk in stream:
                cls._set_model_span_attributes(span, response=chunk)
                yield chunk
        finally:
            close = get_value(stream, "close")
            if callable(close):
                close()

    @classmethod
    async def _capture_litellm_acompletion_stream(cls, span: Span, stream: Any) -> Any:
        try:
            async for chunk in stream:
                cls._set_model_span_attributes(span, response=chunk)
                yield chunk
        finally:
            close = get_value(stream, "aclose")
            if callable(close):
                await close()

    def record_tool_call(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        span = self._resolve_current_span((FunctionSpanData, HandoffSpanData))
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = get_value(tool_call, "name")
        call_id = get_value(tool_call, "call_id")
        if span is not None and (name or call_id):
            from ._processor import OpenTelemetryTracingProcessor  # pylint: disable=import-outside-toplevel

            attributes: dict[str, AttributeValue] = {}
            OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_CALL_ID, call_id)
            if get_value(span, "name") == f"{GenAiOperationNameValues.EXECUTE_TOOL.value} handoff":
                OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_NAME, name)
                if name:
                    span.update_name(f"{GenAiOperationNameValues.EXECUTE_TOOL.value} {name}")
            span.set_attributes(attributes)
        return wrapped(*args, **kwargs)

    def _resolve_current_span(self, span_data_types: tuple[type[Any], ...]) -> Optional[Span]:
        openai_span = get_current_span()
        if openai_span is None or not isinstance(openai_span.span_data, span_data_types):
            return None
        return self._get_otel_span(openai_span.span_id)

    @staticmethod
    def _capture_model_request_params(span: Optional[Span], instance: Any, kwargs: Any) -> None:
        if span is None:
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

        GenAIContextCapture._set_model_span_attributes(span, request_params=params)

    @staticmethod
    def _set_model_span_attributes(
        span: Span,
        request_params: Optional[Mapping[str, Any]] = None,
        response: Any = None,
    ) -> None:
        if not span.is_recording():
            return
        from ._processor import OpenTelemetryTracingProcessor  # pylint: disable=import-outside-toplevel

        attributes: dict[str, AttributeValue] = {}
        if request_params is not None:
            request_model = request_params.get("model")
            OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, request_model)
            OpenTelemetryTracingProcessor._set_attribute(
                attributes,
                GEN_AI_PROVIDER_NAME,
                OpenTelemetryTracingProcessor._resolve_provider(
                    request_model,
                    request_params.get("base_url") or request_params.get("api_base"),
                    request_params.get("custom_llm_provider"),
                ),
            )
            OpenTelemetryTracingProcessor._set_request_attributes(attributes, request_params)
            if request_model:
                operation = str(get_value(span, "name")).split(" ", 1)[0]
                span.update_name(f"{operation} {request_model}")
        if response is not None:
            OpenTelemetryTracingProcessor._set_response_payload_attributes(attributes, response)
            usage = get_value(response, "usage")
            OpenTelemetryTracingProcessor._set_usage_attributes(attributes, usage, usage)
            current_attributes = get_value(span, "attributes") or {}
            OpenTelemetryTracingProcessor._set_attribute(
                attributes,
                GEN_AI_RESPONSE_FINISH_REASONS,
                list(
                    dict.fromkeys(
                        list(get_value(current_attributes, GEN_AI_RESPONSE_FINISH_REASONS) or ())
                        + OpenTelemetryTracingProcessor._get_finish_reasons(
                            response,
                            {**current_attributes, **attributes},
                        )
                    )
                )
                or None,
            )
        span.set_attributes(attributes)
