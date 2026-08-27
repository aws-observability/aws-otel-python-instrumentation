# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Mapping
from typing import Any, Optional, Union

from agents.tracing import Span as AgentsSpan
from agents.tracing import Trace as AgentsTrace
from agents.tracing import TracingProcessor
from agents.tracing.span_data import AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData
from typing_extensions import override

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    GEN_AI_WORKFLOW_NAME,
    OPERATION_INVOKE_WORKFLOW,
    DictWithLock,
    serialize_to_json_string,
    to_tool_attribute_value,
    try_detach,
)
from opentelemetry import context
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_CHOICE_COUNT,
    GEN_AI_REQUEST_FREQUENCY_PENALTY,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_PRESENCE_PENALTY,
    GEN_AI_REQUEST_SEED,
    GEN_AI_REQUEST_STOP_SEQUENCES,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_K,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, set_span_in_context
from opentelemetry.util.types import AttributeValue

from ._messages import normalize_input_messages, normalize_output_messages

_logger = logging.getLogger(__name__)


def _first_not_none(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_traceable_dict = getattr(value, "to_traceable_dict", None)
    if callable(to_traceable_dict):
        result = to_traceable_dict()
        return dict(result) if isinstance(result, Mapping) else {}
    value_dict = getattr(value, "__dict__", None)
    return dict(value_dict) if isinstance(value_dict, Mapping) else {}


def _get_value(value: Any, name: str) -> Any:
    return value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)


class OpenTelemetryTracingProcessor(TracingProcessor):
    """Translate OpenAI Agents SDK tracing callbacks into OpenTelemetry spans."""

    def __init__(self, tracer: Tracer) -> None:
        self._tracer = tracer
        self._workflow_spans = DictWithLock()
        self._otel_spans = DictWithLock()
        self._tokens = DictWithLock()
        self._span_parents = DictWithLock()
        self._agent_content = DictWithLock()

    @override
    def on_trace_start(self, trace: AgentsTrace) -> None:
        attributes = {
            GEN_AI_OPERATION_NAME: OPERATION_INVOKE_WORKFLOW,
            GEN_AI_WORKFLOW_NAME: trace.name,
            GEN_AI_PROVIDER_NAME: GenAiProviderNameValues.OPENAI.value,
        }
        span = self._tracer.start_span(
            f"{OPERATION_INVOKE_WORKFLOW} {trace.name}",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        )
        self._workflow_spans.put(trace.trace_id, span)
        self._tokens.put(trace.trace_id, context.attach(set_span_in_context(span)))

    @override
    def on_trace_end(self, trace: AgentsTrace) -> None:
        token = self._tokens.pop(trace.trace_id)
        if token is not None:
            try_detach(token)
        span = self._workflow_spans.pop(trace.trace_id)
        if span is not None:
            span.set_status(Status(StatusCode.OK))
            span.end()

    @override
    def on_span_start(self, span: AgentsSpan[Any]) -> None:
        self._span_parents.put(span.span_id, span.parent_id)
        span_data = span.span_data
        if not isinstance(span_data, (AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData)):
            return

        if isinstance(span_data, AgentSpanData):
            self._agent_content.put(
                span.span_id,
                {
                    "input_messages": None,
                    "output_messages": None,
                    "system_instructions": None,
                    "request_model": None,
                },
            )

        operation = self._set_operation_name(span_data)
        parent_context = self._resolve_parent_context(span)
        attributes = {
            GEN_AI_OPERATION_NAME: operation,
            GEN_AI_PROVIDER_NAME: GenAiProviderNameValues.OPENAI.value,
        }
        otel_span = self._tracer.start_span(
            self._set_span_name(span_data, operation),
            context=parent_context,
            kind=self._set_span_kind(span_data),
            attributes=attributes,
        )
        self._otel_spans.put(span.span_id, otel_span)
        self._tokens.put(span.span_id, context.attach(set_span_in_context(otel_span)))

    @override
    def on_span_end(self, span: AgentsSpan[Any]) -> None:
        token = self._tokens.pop(span.span_id)
        if token is not None:
            try_detach(token)
        otel_span = self._otel_spans.pop(span.span_id)
        if otel_span is None:
            self._span_parents.pop(span.span_id)
            self._agent_content.pop(span.span_id)
            return

        try:
            attributes, content = self._span_attributes(span)
            if isinstance(span.span_data, GenerationSpanData):
                operation = self._set_operation_name(span.span_data)
                attributes[GEN_AI_OPERATION_NAME] = operation
                otel_span.update_name(self._set_span_name(span.span_data, operation))
            if content:
                self._roll_up_agent_content(span, content)
            otel_span.set_attributes(attributes)

            if isinstance(span.span_data, ResponseSpanData):
                response_model = attributes.get(GEN_AI_RESPONSE_MODEL)
                if response_model:
                    otel_span.update_name(f"{GenAiOperationNameValues.CHAT.value} {response_model}")

            self._set_span_status(otel_span, span.error)
        except Exception as error:  # pylint: disable=broad-exception-caught
            _logger.warning("Failed to enrich OpenAI Agents span %s: %s", span.span_id, error)
            self._set_span_status(otel_span, span.error)
        finally:
            otel_span.end()
            self._span_parents.pop(span.span_id)
            self._agent_content.pop(span.span_id)

    @override
    def shutdown(self) -> None:
        for token in reversed(self._tokens.pop_all()):
            try_detach(token)
        open_spans = list(reversed(self._otel_spans.pop_all()))
        open_spans.extend(reversed(self._workflow_spans.pop_all()))
        for span in open_spans:
            if span.is_recording():
                span.set_attribute(ERROR_TYPE, "_OTHER")
                span.set_status(Status(StatusCode.ERROR, "Trace ended before span completion"))
                span.end()
        self._span_parents.clear()
        self._agent_content.clear()

    @override
    def force_flush(self) -> None:  # pylint: disable=no-self-use
        return None

    @staticmethod
    def _set_operation_name(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData],
    ) -> str:
        if isinstance(span_data, AgentSpanData):
            return GenAiOperationNameValues.INVOKE_AGENT.value
        if isinstance(span_data, FunctionSpanData):
            return GenAiOperationNameValues.EXECUTE_TOOL.value
        if isinstance(span_data, GenerationSpanData):
            return (
                GenAiOperationNameValues.CHAT.value
                if any(_get_value(item, "role") for item in (span_data.input or []))
                else GenAiOperationNameValues.TEXT_COMPLETION.value
            )
        return GenAiOperationNameValues.CHAT.value

    @staticmethod
    def _set_span_kind(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData],
    ) -> SpanKind:
        if isinstance(span_data, (GenerationSpanData, ResponseSpanData)):
            return SpanKind.CLIENT
        return SpanKind.INTERNAL

    @staticmethod
    def _set_span_name(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData],
        operation_name: str,
    ) -> str:
        if isinstance(span_data, (AgentSpanData, FunctionSpanData)):
            return f"{operation_name} {span_data.name}"
        if isinstance(span_data, GenerationSpanData) and span_data.model:
            return f"{operation_name} {span_data.model}"
        return operation_name

    def _resolve_parent_context(self, span: AgentsSpan[Any]) -> Optional[context.Context]:
        parent_id = span.parent_id
        visited = set()
        while parent_id and parent_id not in visited:
            visited.add(parent_id)
            parent_span = self._otel_spans.get(parent_id)
            if parent_span is not None:
                return set_span_in_context(parent_span)
            parent_id = self._span_parents.get(parent_id)

        workflow_span = self._workflow_spans.get(span.trace_id)
        return set_span_in_context(workflow_span) if workflow_span is not None else None

    def _span_attributes(self, span: AgentsSpan[Any]) -> tuple[dict[str, AttributeValue], dict[str, Any]]:
        span_data = span.span_data
        if isinstance(span_data, GenerationSpanData):
            return self._generation_attributes(span_data)
        if isinstance(span_data, ResponseSpanData):
            return self._response_attributes(span_data)
        if isinstance(span_data, FunctionSpanData):
            return self._function_attributes(span_data), {}
        if isinstance(span_data, AgentSpanData):
            return self._agent_attributes(span.span_id, span_data), {}
        return {}, {}

    @staticmethod
    def _generation_attributes(span_data: GenerationSpanData) -> tuple[dict[str, AttributeValue], dict[str, Any]]:
        attributes: dict[str, AttributeValue] = {}
        content: dict[str, Any] = {}
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, span_data.model)

        model_config = _as_mapping(span_data.model_config)
        request_attributes = (
            (GEN_AI_REQUEST_TEMPERATURE, ("temperature",)),
            (GEN_AI_REQUEST_TOP_P, ("top_p",)),
            (GEN_AI_REQUEST_TOP_K, ("top_k",)),
            (GEN_AI_REQUEST_MAX_TOKENS, ("max_tokens",)),
            (GEN_AI_REQUEST_FREQUENCY_PENALTY, ("frequency_penalty",)),
            (GEN_AI_REQUEST_PRESENCE_PENALTY, ("presence_penalty",)),
            (GEN_AI_REQUEST_STOP_SEQUENCES, ("stop_sequences", "stop")),
            (GEN_AI_REQUEST_SEED, ("seed",)),
            (GEN_AI_REQUEST_CHOICE_COUNT, ("choice_count", "n")),
        )
        for attribute, keys in request_attributes:
            OpenTelemetryTracingProcessor._set_attribute(
                attributes,
                attribute,
                _first_not_none(*(model_config.get(key) for key in keys)),
            )

        usage = _as_mapping(span_data.usage)
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_INPUT_TOKENS,
            _first_not_none(usage.get("input_tokens"), usage.get("prompt_tokens")),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_OUTPUT_TOKENS,
            _first_not_none(usage.get("output_tokens"), usage.get("completion_tokens")),
        )

        system_instructions, input_messages = normalize_input_messages(span_data.input)
        output_messages = normalize_output_messages(span_data.output)
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content.update(
            {
                "input_messages": input_messages or None,
                "output_messages": output_messages or None,
                "system_instructions": system_instructions or None,
                "request_model": span_data.model,
            }
        )
        return attributes, content

    @staticmethod
    def _response_attributes(span_data: ResponseSpanData) -> tuple[dict[str, AttributeValue], dict[str, Any]]:
        attributes: dict[str, AttributeValue] = {}
        response = span_data.response
        response_id = getattr(response, "id", None)
        response_model = getattr(response, "model", None)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_ID, response_id)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_MODEL, response_model)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, response_model)

        usage = getattr(span_data, "usage", None) or getattr(response, "usage", None)
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_USAGE_INPUT_TOKENS, _get_value(usage, "input_tokens")
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_USAGE_OUTPUT_TOKENS, _get_value(usage, "output_tokens")
        )

        system_instructions, input_messages = normalize_input_messages(getattr(span_data, "input", None))
        response_instructions, _ = normalize_input_messages(
            {"role": "system", "content": getattr(response, "instructions", None)}
        )
        if response_instructions:
            system_instructions = response_instructions
        output_messages = normalize_output_messages(getattr(response, "output", None))
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content = {
            "input_messages": input_messages or None,
            "output_messages": output_messages or None,
            "system_instructions": system_instructions or None,
            "request_model": response_model,
        }
        return attributes, content

    @staticmethod
    def _function_attributes(span_data: FunctionSpanData) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_NAME, span_data.name)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_TYPE, "function")
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_ARGUMENTS, to_tool_attribute_value(span_data.input)
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_RESULT, to_tool_attribute_value(span_data.output)
        )
        return attributes

    def _agent_attributes(self, span_id: str, span_data: AgentSpanData) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        self._set_attribute(attributes, GEN_AI_AGENT_NAME, span_data.name)
        content = self._agent_content.get(span_id) or {}
        self._set_attribute(attributes, GEN_AI_REQUEST_MODEL, content.get("request_model"))
        self._set_message_attributes(
            attributes,
            content.get("system_instructions") or [],
            content.get("input_messages") or [],
            content.get("output_messages") or [],
        )
        return attributes

    def _roll_up_agent_content(self, span: AgentsSpan[Any], content: dict[str, Any]) -> None:
        parent_id = span.parent_id
        visited = set()
        while parent_id and parent_id not in visited:
            visited.add(parent_id)
            agent_content = self._agent_content.get(parent_id)
            if agent_content is not None:
                for key in ("input_messages", "system_instructions", "request_model"):
                    if agent_content.get(key) is None and content.get(key) is not None:
                        agent_content[key] = content[key]
                if content.get("output_messages") is not None:
                    agent_content["output_messages"] = content["output_messages"]
                return
            parent_id = self._span_parents.get(parent_id)

    @staticmethod
    def _set_message_attributes(
        attributes: dict[str, AttributeValue],
        system_instructions: list[dict[str, Any]],
        input_messages: list[dict[str, Any]],
        output_messages: list[dict[str, Any]],
    ) -> None:
        if system_instructions:
            attributes[GEN_AI_SYSTEM_INSTRUCTIONS] = serialize_to_json_string(system_instructions)
        if input_messages:
            attributes[GEN_AI_INPUT_MESSAGES] = serialize_to_json_string(input_messages)
        if output_messages:
            attributes[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(output_messages)

    @staticmethod
    def _set_attribute(attributes: dict[str, AttributeValue], key: str, value: Any) -> None:
        if value is not None:
            attributes[key] = value

    @staticmethod
    def _set_span_status(span: Span, error: Any) -> None:
        if not error:
            span.set_status(Status(StatusCode.OK))
            return

        message = str(error.get("message") or "OpenAI Agents span failed")
        data = error.get("data") if isinstance(error.get("data"), Mapping) else {}
        error_message = data.get("error")
        description = f"{message}: {error_message}" if error_message else message
        error_type = _first_not_none(data.get("type"), data.get("error_type"), data.get("exception_type"), "_OTHER")
        span.set_attribute(ERROR_TYPE, error_type)
        span.set_status(Status(StatusCode.ERROR, description))
