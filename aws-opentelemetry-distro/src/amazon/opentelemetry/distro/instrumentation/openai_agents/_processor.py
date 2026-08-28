# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import re
from collections.abc import Mapping, Sequence
from contextvars import Token
from dataclasses import dataclass
from typing import Any, Optional, Union
from urllib.parse import urlparse

from agents.tracing import Span as AgentsSpan
from agents.tracing import Trace as AgentsTrace
from agents.tracing import TracingProcessor
from agents.tracing.span_data import (
    AgentSpanData,
    FunctionSpanData,
    GenerationSpanData,
    HandoffSpanData,
    ResponseSpanData,
)
from pydantic import BaseModel
from typing_extensions import override

from amazon.opentelemetry.distro._gen_ai._context import attach_llm_span_context
from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    PROVIDER_MAP,
    DictWithLock,
    content_to_parts,
    serialize_to_json_string,
    to_tool_attribute_value,
    try_detach,
)
from opentelemetry import context
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPENAI_REQUEST_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_OUTPUT_TYPE,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_CHOICE_COUNT,
    GEN_AI_REQUEST_FREQUENCY_PENALTY,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_PRESENCE_PENALTY,
    GEN_AI_REQUEST_SEED,
    GEN_AI_REQUEST_STOP_SEQUENCES,
    GEN_AI_REQUEST_STREAM,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_K,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
    GEN_AI_WORKFLOW_NAME,
    GenAiOperationNameValues,
    GenAiOutputTypeValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.server_attributes import SERVER_ADDRESS, SERVER_PORT
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, set_span_in_context
from opentelemetry.util.types import AttributeValue

from ._gen_ai_context_capture import GenAIContextCapture

_logger = logging.getLogger(__name__)


@dataclass
class _SpanEntry:
    span: Span
    token: Optional[Token] = None
    agent_content: Optional["_AgentContent"] = None
    error: Any = None


@dataclass
class _AgentContent:
    input_messages: Optional[list[dict[str, Any]]] = None
    output_messages: Optional[list[dict[str, Any]]] = None
    system_instructions: Optional[list[dict[str, Any]]] = None
    request_attributes: Optional[dict[str, AttributeValue]] = None


class OpenTelemetryTracingProcessor(TracingProcessor):
    """Translate OpenAI Agents SDK tracing callbacks into OpenTelemetry spans."""

    def __init__(self, tracer: Tracer) -> None:
        self._tracer = tracer
        # Maps OpenAI trace IDs to root OTel workflow spans and context tokens.
        # Used to parent top-level spans and close workflows when traces end.
        self._openai_trace_id_to_otel_workflow_entry = DictWithLock()
        # Maps OpenAI span IDs to OTel spans, context tokens, and shared agent content.
        # Used to resolve parents, finish spans, and carry state through unhandled spans.
        self._openai_span_id_to_otel_span_entry = DictWithLock()

    @override
    def on_trace_start(self, trace: AgentsTrace) -> None:
        attributes = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.INVOKE_WORKFLOW.value,
            GEN_AI_WORKFLOW_NAME: trace.name,
            GEN_AI_PROVIDER_NAME: GenAiProviderNameValues.OPENAI.value,
        }
        span = self._tracer.start_span(
            f"{GenAiOperationNameValues.INVOKE_WORKFLOW.value} {trace.name}",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        )
        token = context.attach(set_span_in_context(span))
        self._openai_trace_id_to_otel_workflow_entry.put(trace.trace_id, _SpanEntry(span=span, token=token))

    @override
    def on_trace_end(self, trace: AgentsTrace) -> None:
        entry = self._openai_trace_id_to_otel_workflow_entry.pop(trace.trace_id)
        if entry is None:
            return
        if entry.token is not None:
            try_detach(entry.token)
        self._set_span_status(entry.span, entry.error)
        entry.span.end()

    @override
    def on_span_start(self, span: AgentsSpan[Any]) -> None:
        parent_entry = self._resolve_parent_entry(span)
        span_data = span.span_data
        if not isinstance(
            span_data, (AgentSpanData, FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData)
        ):
            if parent_entry is not None:
                self._openai_span_id_to_otel_span_entry.put(
                    span.span_id,
                    _SpanEntry(span=parent_entry.span, agent_content=parent_entry.agent_content),
                )
            return

        if isinstance(span_data, (GenerationSpanData, ResponseSpanData)):
            GenAIContextCapture.reset_request_params()
        if isinstance(span_data, (FunctionSpanData, HandoffSpanData)):
            GenAIContextCapture.reset_tool_call()

        agent_content = parent_entry.agent_content if parent_entry is not None else None
        if isinstance(span_data, AgentSpanData):
            agent_content = _AgentContent()

        operation = self._set_operation_name(span_data)
        attributes = {
            GEN_AI_OPERATION_NAME: operation,
            GEN_AI_PROVIDER_NAME: GenAiProviderNameValues.OPENAI.value,
        }
        kind = self._set_span_kind(span_data)
        otel_span = self._tracer.start_span(
            self._set_span_name(span_data, operation),
            context=set_span_in_context(parent_entry.span) if parent_entry is not None else None,
            kind=kind,
            attributes=attributes,
        )
        token = attach_llm_span_context(otel_span, collapse_http_span=kind == SpanKind.CLIENT)
        self._openai_span_id_to_otel_span_entry.put(
            span.span_id,
            _SpanEntry(span=otel_span, token=token, agent_content=agent_content),
        )

    @override
    def on_span_end(self, span: AgentsSpan[Any]) -> None:
        entry = self._openai_span_id_to_otel_span_entry.pop(span.span_id)
        span_data = span.span_data
        if not isinstance(
            span_data, (AgentSpanData, FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData)
        ):
            return
        if entry is None or entry.token is None:
            return

        try_detach(entry.token)
        otel_span = entry.span

        try:
            attributes, content = self._set_span_attributes(span, entry.agent_content)
            if isinstance(span_data, (GenerationSpanData, ResponseSpanData)):
                operation = self._set_operation_name(span_data)
                attributes[GEN_AI_OPERATION_NAME] = operation
                model = self._first_not_none(
                    attributes.get(GEN_AI_REQUEST_MODEL), attributes.get(GEN_AI_RESPONSE_MODEL)
                )
                otel_span.update_name(f"{operation} {model}" if model else operation)
            elif isinstance(span_data, HandoffSpanData):
                otel_span.update_name(self._set_span_name(span_data, self._set_operation_name(span_data)))
            if content is not None and entry.agent_content is not None:
                agent_content = entry.agent_content
                agent_content.input_messages = self._first_not_none(
                    agent_content.input_messages, content.input_messages
                )
                agent_content.system_instructions = self._first_not_none(
                    agent_content.system_instructions, content.system_instructions
                )
                agent_content.request_attributes = self._first_not_none(
                    agent_content.request_attributes, content.request_attributes
                )
                agent_content.output_messages = self._first_not_none(
                    content.output_messages, agent_content.output_messages
                )
            otel_span.set_attributes(attributes)
        except Exception as error:  # pylint: disable=broad-exception-caught
            _logger.warning("Failed to enrich OpenAI Agents span %s: %s", span.span_id, error)
        finally:
            self._set_span_status(otel_span, span.error)
            self._record_trace_error(span)
            otel_span.end()

    @override
    def shutdown(self) -> None:
        open_entries = list(reversed(self._openai_span_id_to_otel_span_entry.pop_all()))
        open_entries.extend(reversed(self._openai_trace_id_to_otel_workflow_entry.pop_all()))
        for entry in open_entries:
            self._close_incomplete_span(entry)

    @override
    def force_flush(self) -> None:
        flush = getattr(getattr(self._tracer, "span_processor", None), "force_flush", None)
        if flush is not None:
            flush()

    @staticmethod
    def _close_incomplete_span(entry: _SpanEntry) -> None:
        if entry.token is None:
            return
        try_detach(entry.token)
        if entry.span.is_recording():
            entry.span.set_attribute(ERROR_TYPE, "_OTHER")
            entry.span.set_status(Status(StatusCode.ERROR, "Trace ended before span completion"))
            entry.span.end()

    def _record_trace_error(self, span: AgentsSpan[Any]) -> None:
        if not span.error:
            return
        workflow_entry = self._openai_trace_id_to_otel_workflow_entry.get(span.trace_id)
        if workflow_entry is not None and workflow_entry.error is None:
            workflow_entry.error = span.error

    def _resolve_parent_entry(self, span: AgentsSpan[Any]) -> Optional[_SpanEntry]:
        if span.parent_id:
            parent_entry = self._openai_span_id_to_otel_span_entry.get(span.parent_id)
            if parent_entry is not None:
                return parent_entry
        return self._openai_trace_id_to_otel_workflow_entry.get(span.trace_id)

    @staticmethod
    def _set_operation_name(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData],
    ) -> str:
        if isinstance(span_data, AgentSpanData):
            return GenAiOperationNameValues.INVOKE_AGENT.value
        if isinstance(span_data, (FunctionSpanData, HandoffSpanData)):
            return GenAiOperationNameValues.EXECUTE_TOOL.value
        if isinstance(span_data, GenerationSpanData):
            return (
                GenAiOperationNameValues.CHAT.value
                if any(OpenTelemetryTracingProcessor._get_value(item, "role") for item in (span_data.input or []))
                else GenAiOperationNameValues.TEXT_COMPLETION.value
            )
        return GenAiOperationNameValues.CHAT.value

    @staticmethod
    def _set_span_name(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData],
        operation_name: str,
    ) -> str:
        if isinstance(span_data, (AgentSpanData, FunctionSpanData)):
            return f"{operation_name} {span_data.name}"
        if isinstance(span_data, HandoffSpanData):
            return f"{operation_name} {OpenTelemetryTracingProcessor._get_handoff_tool_name(span_data)}"
        if isinstance(span_data, GenerationSpanData) and span_data.model:
            return f"{operation_name} {span_data.model}"
        return operation_name

    @staticmethod
    def _set_span_kind(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, HandoffSpanData, ResponseSpanData],
    ) -> SpanKind:
        if isinstance(span_data, (GenerationSpanData, ResponseSpanData)):
            return SpanKind.CLIENT
        return SpanKind.INTERNAL

    def _set_span_attributes(
        self,
        span: AgentsSpan[Any],
        agent_content: Optional[_AgentContent],
    ) -> tuple[dict[str, AttributeValue], Optional[_AgentContent]]:
        span_data = span.span_data
        if isinstance(span_data, GenerationSpanData):
            return self._set_generation_attributes(span_data)
        if isinstance(span_data, ResponseSpanData):
            return self._set_response_attributes(span_data)
        if isinstance(span_data, FunctionSpanData):
            return self._set_function_attributes(span_data), None
        if isinstance(span_data, HandoffSpanData):
            return self._set_handoff_attributes(span_data), None
        if isinstance(span_data, AgentSpanData):
            return self._set_agent_attributes(agent_content, span_data), None
        return {}, None

    @staticmethod
    def _set_generation_attributes(
        span_data: GenerationSpanData,
    ) -> tuple[dict[str, AttributeValue], _AgentContent]:
        attributes: dict[str, AttributeValue] = {}
        params = GenAIContextCapture.get_request_params()
        streamed_response = OpenTelemetryTracingProcessor._get_response_payload(span_data.output)
        model_config = {
            **OpenTelemetryTracingProcessor._get_response_request_config(streamed_response),
            **(span_data.model_config or {}),
            **params,
        }

        request_model = OpenTelemetryTracingProcessor._first_not_none(span_data.model, params.get("model"))
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, request_model)
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_PROVIDER_NAME,
            OpenTelemetryTracingProcessor._resolve_provider(request_model, model_config.get("base_url")),
        )
        OpenTelemetryTracingProcessor._set_request_attributes(attributes, model_config)

        output_items = span_data.output
        if streamed_response is not None:
            OpenTelemetryTracingProcessor._set_response_payload_attributes(attributes, streamed_response)
            OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_STREAM, True)
            output_items = OpenTelemetryTracingProcessor._get_value(streamed_response, "output")

        usage = OpenTelemetryTracingProcessor._first_not_none(
            span_data.usage, OpenTelemetryTracingProcessor._get_value(streamed_response, "usage")
        )
        OpenTelemetryTracingProcessor._set_usage_attributes(attributes, usage)
        default_finish_reason = OpenTelemetryTracingProcessor._get_finish_reason(streamed_response, attributes)

        system_instructions, input_messages = _GenAIMessageNormalizer.normalize_input_messages(span_data.input)
        output_messages = _GenAIMessageNormalizer.normalize_output_messages(output_items, default_finish_reason)
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content = _AgentContent(
            input_messages=input_messages or None,
            output_messages=output_messages or None,
            system_instructions=system_instructions or None,
            request_attributes=OpenTelemetryTracingProcessor._get_rollup_request_attributes(attributes),
        )
        return attributes, content

    @staticmethod
    def _set_response_attributes(span_data: ResponseSpanData) -> tuple[dict[str, AttributeValue], _AgentContent]:
        attributes: dict[str, AttributeValue] = {}
        response = span_data.response
        params = GenAIContextCapture.get_request_params()
        model_config = {**OpenTelemetryTracingProcessor._get_response_request_config(response), **params}

        response_model = OpenTelemetryTracingProcessor._get_value(response, "model")
        request_model = OpenTelemetryTracingProcessor._first_not_none(params.get("model"), response_model)
        OpenTelemetryTracingProcessor._set_response_payload_attributes(attributes, response)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, request_model)
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_PROVIDER_NAME,
            OpenTelemetryTracingProcessor._resolve_provider(request_model, model_config.get("base_url")),
        )
        OpenTelemetryTracingProcessor._set_request_attributes(attributes, model_config)

        usage = OpenTelemetryTracingProcessor._first_not_none(
            getattr(span_data, "usage", None), OpenTelemetryTracingProcessor._get_value(response, "usage")
        )
        OpenTelemetryTracingProcessor._set_usage_attributes(attributes, usage)

        system_instructions, input_messages = _GenAIMessageNormalizer.normalize_input_messages(
            getattr(span_data, "input", None)
        )
        response_instructions, _ = _GenAIMessageNormalizer.normalize_input_messages(
            {"role": "system", "content": OpenTelemetryTracingProcessor._get_value(response, "instructions")}
        )
        if response_instructions:
            system_instructions = response_instructions
        default_finish_reason = OpenTelemetryTracingProcessor._get_finish_reason(response, attributes)
        output_messages = _GenAIMessageNormalizer.normalize_output_messages(
            OpenTelemetryTracingProcessor._get_value(response, "output"), default_finish_reason
        )
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content = _AgentContent(
            input_messages=input_messages or None,
            output_messages=output_messages or None,
            system_instructions=system_instructions or None,
            request_attributes=OpenTelemetryTracingProcessor._get_rollup_request_attributes(attributes),
        )
        return attributes, content

    @staticmethod
    def _set_function_attributes(span_data: FunctionSpanData) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_NAME, span_data.name)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_TYPE, "function")
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_ID, GenAIContextCapture.get_tool_call().call_id
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_ARGUMENTS, to_tool_attribute_value(span_data.input)
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_RESULT, to_tool_attribute_value(span_data.output)
        )
        return attributes

    @staticmethod
    def _set_handoff_attributes(span_data: HandoffSpanData) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_NAME, OpenTelemetryTracingProcessor._get_handoff_tool_name(span_data)
        )
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_TOOL_TYPE, "function")
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_TOOL_CALL_ID, GenAIContextCapture.get_tool_call().call_id
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_TOOL_CALL_ARGUMENTS,
            to_tool_attribute_value({"from_agent": span_data.from_agent, "to_agent": span_data.to_agent}),
        )
        return attributes

    def _set_agent_attributes(
        self,
        content: Optional[_AgentContent],
        span_data: AgentSpanData,
    ) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        self._set_attribute(attributes, GEN_AI_AGENT_NAME, span_data.name)
        content = content or _AgentContent()
        for key, value in (content.request_attributes or {}).items():
            self._set_attribute(attributes, key, value)
        self._set_tool_definitions(attributes, span_data.tools)
        self._set_attribute(attributes, GEN_AI_OUTPUT_TYPE, self._get_agent_output_type(span_data.output_type))
        self._set_message_attributes(
            attributes,
            content.system_instructions or [],
            content.input_messages or [],
            content.output_messages or [],
        )
        return attributes

    @staticmethod
    def _set_request_attributes(attributes: dict[str, AttributeValue], model_config: Mapping[str, Any]) -> None:
        request_attributes = (
            (GEN_AI_REQUEST_TEMPERATURE, ("temperature",)),
            (GEN_AI_REQUEST_TOP_P, ("top_p",)),
            (GEN_AI_REQUEST_TOP_K, ("top_k",)),
            (GEN_AI_REQUEST_MAX_TOKENS, ("max_tokens", "max_output_tokens", "max_completion_tokens")),
            (GEN_AI_REQUEST_FREQUENCY_PENALTY, ("frequency_penalty",)),
            (GEN_AI_REQUEST_PRESENCE_PENALTY, ("presence_penalty",)),
            (GEN_AI_REQUEST_STOP_SEQUENCES, ("stop_sequences", "stop")),
            (GEN_AI_REQUEST_SEED, ("seed",)),
            (GEN_AI_REQUEST_CHOICE_COUNT, ("choice_count", "n")),
            (GEN_AI_REQUEST_STREAM, ("stream",)),
            (GEN_AI_OPENAI_REQUEST_SERVICE_TIER, ("service_tier",)),
        )
        for attribute, keys in request_attributes:
            value = OpenTelemetryTracingProcessor._first_not_none(*(model_config.get(key) for key in keys))
            if attribute == GEN_AI_REQUEST_STOP_SEQUENCES:
                value = OpenTelemetryTracingProcessor._to_string_sequence(value)
            OpenTelemetryTracingProcessor._set_attribute(attributes, attribute, value)

        base_url = model_config.get("base_url")
        if base_url:
            parsed_url = urlparse(str(base_url))
            OpenTelemetryTracingProcessor._set_attribute(attributes, SERVER_ADDRESS, parsed_url.hostname)
            OpenTelemetryTracingProcessor._set_attribute(attributes, SERVER_PORT, parsed_url.port)

        OpenTelemetryTracingProcessor._set_tool_definitions(attributes, model_config.get("tools"))
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_OUTPUT_TYPE, OpenTelemetryTracingProcessor._get_request_output_type(model_config)
        )

    @staticmethod
    def _set_response_payload_attributes(attributes: dict[str, AttributeValue], response: Any) -> None:
        response_id = OpenTelemetryTracingProcessor._get_value(response, "id")
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_RESPONSE_ID, None if response_id in ("__fake_id__", "") else response_id
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes, GEN_AI_RESPONSE_MODEL, OpenTelemetryTracingProcessor._get_value(response, "model")
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_OPENAI_RESPONSE_SERVICE_TIER,
            OpenTelemetryTracingProcessor._get_value(response, "service_tier"),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT,
            OpenTelemetryTracingProcessor._get_value(response, "system_fingerprint"),
        )

    @staticmethod
    def _set_usage_attributes(attributes: dict[str, AttributeValue], usage: Any) -> None:
        input_details = OpenTelemetryTracingProcessor._first_not_none(
            OpenTelemetryTracingProcessor._get_value(usage, "input_tokens_details"),
            OpenTelemetryTracingProcessor._get_value(usage, "prompt_tokens_details"),
        )
        output_details = OpenTelemetryTracingProcessor._first_not_none(
            OpenTelemetryTracingProcessor._get_value(usage, "output_tokens_details"),
            OpenTelemetryTracingProcessor._get_value(usage, "completion_tokens_details"),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_INPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                OpenTelemetryTracingProcessor._get_value(usage, "input_tokens"),
                OpenTelemetryTracingProcessor._get_value(usage, "prompt_tokens"),
            ),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_OUTPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                OpenTelemetryTracingProcessor._get_value(usage, "output_tokens"),
                OpenTelemetryTracingProcessor._get_value(usage, "completion_tokens"),
            ),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                OpenTelemetryTracingProcessor._get_value(input_details, "cached_tokens"),
                OpenTelemetryTracingProcessor._get_value(usage, "cache_read_input_tokens"),
            ),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                OpenTelemetryTracingProcessor._get_value(input_details, "cache_write_tokens"),
                OpenTelemetryTracingProcessor._get_value(usage, "cache_creation_input_tokens"),
            ),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                OpenTelemetryTracingProcessor._get_value(output_details, "reasoning_tokens"),
                OpenTelemetryTracingProcessor._get_value(usage, "reasoning_tokens"),
            ),
        )

    @staticmethod
    def _set_tool_definitions(attributes: dict[str, AttributeValue], tools: Any) -> None:
        definitions: list[dict[str, Any]] = []
        for tool in _GenAIMessageNormalizer._to_item_list(tools):
            if isinstance(tool, str):
                definitions.append({"type": "function", "name": tool})
                continue
            tool_data = _GenAIMessageNormalizer._as_mapping(tool)
            if tool_data is None:
                continue
            function = _GenAIMessageNormalizer._as_mapping(tool_data.get("function")) or {}
            definition: dict[str, Any] = {"type": tool_data.get("type") or "function"}
            for key, source_key in (("name", "name"), ("description", "description"), ("parameters", "parameters")):
                value = OpenTelemetryTracingProcessor._first_not_none(
                    tool_data.get(source_key), function.get(source_key)
                )
                if value is not None:
                    definition[key] = value
            definitions.append(definition)
        if definitions:
            attributes[GEN_AI_TOOL_DEFINITIONS] = serialize_to_json_string(definitions)

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
        if isinstance(value, (bool, bytes, float, int, str, list, tuple)):
            attributes[key] = value

    @staticmethod
    def _set_span_status(span: Span, error: Any) -> None:
        if not error:
            return

        message = str(error.get("message") or "OpenAI Agents span failed")
        data = error.get("data") if isinstance(error.get("data"), Mapping) else {}
        error_message = data.get("error")
        description = f"{message}: {error_message}" if error_message else message
        error_type = OpenTelemetryTracingProcessor._first_not_none(
            data.get("type"),
            data.get("error_type"),
            data.get("exception_type"),
            "_OTHER",
        )
        span.set_attribute(ERROR_TYPE, error_type)
        span.set_status(Status(StatusCode.ERROR, description))

    @staticmethod
    def _get_response_request_config(response: Any) -> dict[str, Any]:
        if response is None:
            return {}
        model_config: dict[str, Any] = {}
        for key in ("temperature", "top_p", "max_output_tokens", "top_logprobs", "tools", "text"):
            value = OpenTelemetryTracingProcessor._get_value(response, key)
            if value is not None:
                model_config[key] = value
        return model_config

    @staticmethod
    def _get_finish_reason(response: Any, attributes: Mapping[str, AttributeValue]) -> Optional[str]:
        incomplete_details = OpenTelemetryTracingProcessor._get_value(response, "incomplete_details")
        reason = OpenTelemetryTracingProcessor._get_value(incomplete_details, "reason")
        if reason:
            return {"max_output_tokens": "length", "content_filter": "content_filter"}.get(str(reason), str(reason))
        if OpenTelemetryTracingProcessor._get_value(response, "status") in ("failed", "cancelled"):
            return "error"
        max_tokens = attributes.get(GEN_AI_REQUEST_MAX_TOKENS)
        output_tokens = attributes.get(GEN_AI_USAGE_OUTPUT_TOKENS)
        if isinstance(max_tokens, int) and isinstance(output_tokens, int) and output_tokens >= max_tokens > 0:
            return "length"
        return None

    @staticmethod
    def _get_request_output_type(model_config: Mapping[str, Any]) -> Optional[str]:
        response_format = OpenTelemetryTracingProcessor._first_not_none(
            OpenTelemetryTracingProcessor._get_value(model_config.get("text"), "format"),
            model_config.get("response_format"),
        )
        format_type = OpenTelemetryTracingProcessor._first_not_none(
            OpenTelemetryTracingProcessor._get_value(response_format, "type"), response_format
        )
        if not isinstance(format_type, str):
            return None
        return {
            "text": GenAiOutputTypeValues.TEXT.value,
            "json": GenAiOutputTypeValues.JSON.value,
            "json_object": GenAiOutputTypeValues.JSON.value,
            "json_schema": GenAiOutputTypeValues.JSON.value,
        }.get(format_type)

    @staticmethod
    def _get_agent_output_type(output_type: Any) -> Optional[str]:
        if not output_type:
            return None
        if str(output_type) in ("str", "text"):
            return GenAiOutputTypeValues.TEXT.value
        return GenAiOutputTypeValues.JSON.value

    @staticmethod
    def _resolve_provider(model: Any, base_url: Any) -> str:
        candidates: list[str] = []
        if model and "/" in str(model):
            candidates.append(str(model).split("/", 1)[0])
        if base_url:
            hostname = urlparse(str(base_url)).hostname or ""
            for label in reversed(hostname.split(".")):
                candidates.extend(label.split("-"))
        for candidate in candidates:
            provider = PROVIDER_MAP.get(candidate.lower())
            if provider is not None:
                return provider
        return GenAiProviderNameValues.OPENAI.value

    @staticmethod
    def _get_rollup_request_attributes(attributes: Mapping[str, AttributeValue]) -> dict[str, AttributeValue]:
        rollup = {
            key: value
            for key, value in attributes.items()
            if key.startswith("gen_ai.request.") and key != GEN_AI_REQUEST_STREAM
        }
        provider = attributes.get(GEN_AI_PROVIDER_NAME)
        if provider is not None:
            rollup[GEN_AI_PROVIDER_NAME] = provider
        return rollup

    @staticmethod
    def _get_response_payload(output: Any) -> Optional[dict[str, Any]]:
        items = _GenAIMessageNormalizer._to_item_list(output)
        if len(items) != 1:
            return None
        payload = _GenAIMessageNormalizer._as_mapping(items[0])
        if payload is None or payload.get("object") != "response" or "output" not in payload:
            return None
        return payload

    @staticmethod
    def _get_handoff_tool_name(span_data: HandoffSpanData) -> str:
        tool_name = GenAIContextCapture.get_tool_call().name
        if tool_name:
            return str(tool_name)
        if not span_data.to_agent:
            return "handoff"
        return re.sub(r"[^a-zA-Z0-9_]", "_", f"transfer_to_{span_data.to_agent}").lower()

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
    def _first_not_none(*values: Any) -> Any:
        return next((value for value in values if value is not None), None)

    @staticmethod
    def _get_value(value: Any, name: str) -> Any:
        return value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)


class _GenAIMessageNormalizer:
    """Convert OpenAI Agents payloads into OTel GenAI semantic-convention messages."""

    @classmethod
    def normalize_input_messages(cls, items: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Normalize OpenAI Agents input payloads into GenAI semantic convention messages."""
        system_instructions: list[dict[str, Any]] = []
        conversation: list[dict[str, Any]] = []
        for item in cls._to_item_list(items):
            item_system, item_messages = cls._normalize_message(item, output=False)
            system_instructions.extend(item_system)
            conversation.extend(item_messages)
        return system_instructions, conversation

    @classmethod
    def normalize_output_messages(cls, items: Any, default_finish_reason: Optional[str] = None) -> list[dict[str, Any]]:
        """Normalize OpenAI Agents output payloads into GenAI semantic convention messages."""
        messages: list[dict[str, Any]] = []
        for item in cls._to_item_list(items):
            _, item_messages = cls._normalize_message(item, output=True, default_finish_reason=default_finish_reason)
            messages.extend(item_messages)
        return messages

    @classmethod
    def _normalize_message(  # pylint: disable=too-many-branches
        cls,
        item: Any,
        output: bool,
        default_finish_reason: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        system_instructions: list[dict[str, Any]] = []
        messages: list[dict[str, Any]] = []
        item_data = cls._as_mapping(item)

        if item_data is None:
            parts = cls._to_message_parts(item)
            if parts:
                message = {"role": "assistant" if output else "user", "parts": parts}
                if output:
                    message["finish_reason"] = default_finish_reason or "stop"
                messages.append(message)
            return system_instructions, messages

        item_type = item_data.get("type")
        role = item_data.get("role")

        if item_type and item_type.endswith("_call"):
            part = {
                "type": "tool_call",
                "id": item_data.get("call_id") or item_data.get("id") or "",
                "name": item_data.get("name") or item_type,
                "arguments": cls._parse_arguments(
                    item_data.get("arguments") or item_data.get("action") or item_data.get("query") or {}
                ),
            }
            message = {"role": "assistant", "parts": [part]}
            if output:
                message["finish_reason"] = "tool_call"
            messages.append(message)
            return system_instructions, messages

        if item_type == "function_call_output":
            part = {
                "type": "tool_call_response",
                "id": item_data.get("call_id") or item_data.get("id") or "",
                "response": item_data.get("output", ""),
            }
            message = {"role": "tool", "parts": [part]}
            if output:
                message["finish_reason"] = "stop"
            messages.append(message)
            return system_instructions, messages

        if item_type == "reasoning":
            reasoning = cls._to_reasoning_parts(item_data)
            if reasoning:
                message = {"role": "assistant", "parts": reasoning}
                if output:
                    message["finish_reason"] = cls._normalize_finish_reason(item_data, False)
                messages.append(message)
            return system_instructions, messages

        parts: list[dict[str, Any]] = []
        if role == "tool":
            parts.append(
                {
                    "type": "tool_call_response",
                    "id": item_data.get("tool_call_id") or item_data.get("call_id") or "",
                    "response": item_data.get("content", ""),
                }
            )
        else:
            parts.extend(cls._to_message_parts(item_data.get("content")))

        refusal = item_data.get("refusal")
        if refusal:
            parts.append({"type": "text", "content": str(refusal)})

        tool_call_parts = cls._to_tool_call_parts(item_data.get("tool_calls"))
        parts.extend(tool_call_parts)

        if role in ("system", "developer"):
            system_instructions.extend(parts)
            return system_instructions, messages

        if not role:
            role = "assistant" if output else "user"
        message = {"role": role, "parts": parts}
        if output:
            message["finish_reason"] = cls._normalize_finish_reason(
                item_data, bool(tool_call_parts), default_finish_reason
            )
        messages.append(message)
        return system_instructions, messages

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
            "length": "length",
            "content_filter": "content_filter",
        }.get(str(raw_reason), str(raw_reason))

    @classmethod
    def _to_message_parts(cls, content: Any) -> list[dict[str, Any]]:
        if isinstance(content, BaseModel):
            content = content.model_dump()
        if isinstance(content, Sequence) and not isinstance(content, (str, bytes)):
            content = [block.model_dump() if isinstance(block, BaseModel) else block for block in content]
        return [cls._normalize_part(part) for part in content_to_parts(content)]

    @staticmethod
    def _normalize_part(part: dict[str, Any]) -> dict[str, Any]:
        part_type = part.get("type")
        if part_type == "refusal":
            return {"type": "text", "content": str(part.get("refusal") or part.get("content") or "")}
        if part_type in ("input_image", "output_image", "input_file", "input_audio"):
            modality = "audio" if part_type == "input_audio" else ("file" if "file" in part_type else "image")
            uri = part.get("image_url") or part.get("file_url") or part.get("file_id")
            if isinstance(uri, str) and uri:
                return {"type": "uri", "modality": modality, "uri": uri}
            data = part.get("data") or part.get("file_data") or ""
            return {
                "type": "blob",
                "modality": modality,
                "mime_type": part.get("mime_type") or part.get("filename") or f"{modality}/*",
                "content": data,
            }
        return part

    @classmethod
    def _to_tool_call_parts(cls, tool_calls: Any) -> list[dict[str, Any]]:
        parts: list[dict[str, Any]] = []
        for raw_tool_call in cls._to_item_list(tool_calls):
            tool_call = cls._as_mapping(raw_tool_call) or {}
            function = cls._as_mapping(tool_call.get("function")) or {}
            parts.append(
                {
                    "type": "tool_call",
                    "id": tool_call.get("id") or tool_call.get("call_id") or "",
                    "name": function.get("name") or tool_call.get("name") or "",
                    "arguments": cls._parse_arguments(function.get("arguments", tool_call.get("arguments", {}))),
                }
            )
        return parts

    @classmethod
    def _to_reasoning_parts(cls, item: Mapping[str, Any]) -> list[dict[str, Any]]:
        parts: list[dict[str, Any]] = []
        for source_key in ("summary", "content"):
            for summary_item in cls._to_item_list(item.get(source_key)):
                summary = cls._as_mapping(summary_item)
                content = summary.get("text") if summary else summary_item
                if content:
                    parts.append({"type": "reasoning", "content": str(content)})
            if parts:
                break
        return parts

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
    def _as_mapping(value: Any) -> Optional[dict[str, Any]]:
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, BaseModel):
            return value.model_dump()
        return None

    @staticmethod
    def _parse_arguments(arguments: Any) -> Any:
        if not isinstance(arguments, str):
            return arguments
        try:
            return json.loads(arguments)
        except (TypeError, ValueError):
            return arguments
