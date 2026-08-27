# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from contextvars import Token
from dataclasses import dataclass
from typing import Any, Optional, Union

from agents.tracing import Span as AgentsSpan
from agents.tracing import Trace as AgentsTrace
from agents.tracing import TracingProcessor
from agents.tracing.span_data import AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData
from pydantic import BaseModel
from typing_extensions import override

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
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
    GEN_AI_WORKFLOW_NAME,
    GenAiOperationNameValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, set_span_in_context
from opentelemetry.util.types import AttributeValue

_logger = logging.getLogger(__name__)


@dataclass
class _SpanEntry:
    span: Span
    token: Optional[Token] = None
    agent_content: Optional["_AgentContent"] = None


@dataclass
class _AgentContent:
    input_messages: Optional[list[dict[str, Any]]] = None
    output_messages: Optional[list[dict[str, Any]]] = None
    system_instructions: Optional[list[dict[str, Any]]] = None
    request_model: Optional[str] = None


class OpenTelemetryTracingProcessor(TracingProcessor):
    """Translate OpenAI Agents SDK tracing callbacks into OpenTelemetry spans."""

    def __init__(self, tracer: Tracer, force_flush: Optional[Callable[[], Any]] = None) -> None:
        self._tracer = tracer
        self._force_flush = force_flush
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
        entry.span.set_status(Status(StatusCode.OK))
        entry.span.end()

    @override
    def on_span_start(self, span: AgentsSpan[Any]) -> None:
        parent_entry = self._resolve_parent_entry(span)
        span_data = span.span_data
        if not isinstance(span_data, (AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData)):
            if parent_entry is not None:
                self._openai_span_id_to_otel_span_entry.put(
                    span.span_id,
                    _SpanEntry(span=parent_entry.span, agent_content=parent_entry.agent_content),
                )
            return

        agent_content = parent_entry.agent_content if parent_entry is not None else None
        if isinstance(span_data, AgentSpanData):
            agent_content = _AgentContent()

        operation = self._set_operation_name(span_data)
        attributes = {
            GEN_AI_OPERATION_NAME: operation,
            GEN_AI_PROVIDER_NAME: GenAiProviderNameValues.OPENAI.value,
        }
        otel_span = self._tracer.start_span(
            self._set_span_name(span_data, operation),
            context=set_span_in_context(parent_entry.span) if parent_entry is not None else None,
            kind=self._set_span_kind(span_data),
            attributes=attributes,
        )
        token = context.attach(set_span_in_context(otel_span))
        self._openai_span_id_to_otel_span_entry.put(
            span.span_id,
            _SpanEntry(span=otel_span, token=token, agent_content=agent_content),
        )

    @override
    def on_span_end(self, span: AgentsSpan[Any]) -> None:
        entry = self._openai_span_id_to_otel_span_entry.pop(span.span_id)
        span_data = span.span_data
        if not isinstance(span_data, (AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData)):
            return
        if entry is None or entry.token is None:
            return

        try_detach(entry.token)
        otel_span = entry.span

        try:
            attributes, content = self._set_span_attributes(span, entry.agent_content)
            if isinstance(span_data, GenerationSpanData):
                operation = self._set_operation_name(span_data)
                attributes[GEN_AI_OPERATION_NAME] = operation
                otel_span.update_name(self._set_span_name(span_data, operation))
            if content is not None and entry.agent_content is not None:
                agent_content = entry.agent_content
                agent_content.input_messages = self._first_not_none(
                    agent_content.input_messages, content.input_messages
                )
                agent_content.system_instructions = self._first_not_none(
                    agent_content.system_instructions, content.system_instructions
                )
                agent_content.request_model = self._first_not_none(agent_content.request_model, content.request_model)
                agent_content.output_messages = self._first_not_none(
                    content.output_messages, agent_content.output_messages
                )
            otel_span.set_attributes(attributes)

            if isinstance(span_data, ResponseSpanData):
                response_model = attributes.get(GEN_AI_RESPONSE_MODEL)
                if response_model:
                    otel_span.update_name(f"{GenAiOperationNameValues.CHAT.value} {response_model}")

            self._set_span_status(otel_span, span.error)
        except Exception as error:  # pylint: disable=broad-exception-caught
            _logger.warning("Failed to enrich OpenAI Agents span %s: %s", span.span_id, error)
            self._set_span_status(otel_span, span.error)
        finally:
            otel_span.end()

    @override
    def shutdown(self) -> None:
        open_entries = list(reversed(self._openai_span_id_to_otel_span_entry.pop_all()))
        open_entries.extend(reversed(self._openai_trace_id_to_otel_workflow_entry.pop_all()))
        for entry in open_entries:
            if entry.token is None:
                continue
            try_detach(entry.token)
            if entry.span.is_recording():
                entry.span.set_attribute(ERROR_TYPE, "_OTHER")
                entry.span.set_status(Status(StatusCode.ERROR, "Trace ended before span completion"))
                entry.span.end()

    @override
    def force_flush(self) -> None:
        if self._force_flush is not None:
            self._force_flush()

    def _resolve_parent_entry(self, span: AgentsSpan[Any]) -> Optional[_SpanEntry]:
        if span.parent_id:
            parent_entry = self._openai_span_id_to_otel_span_entry.get(span.parent_id)
            if parent_entry is not None:
                return parent_entry
        return self._openai_trace_id_to_otel_workflow_entry.get(span.trace_id)

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
                if any(OpenTelemetryTracingProcessor._get_value(item, "role") for item in (span_data.input or []))
                else GenAiOperationNameValues.TEXT_COMPLETION.value
            )
        return GenAiOperationNameValues.CHAT.value

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

    @staticmethod
    def _set_span_kind(
        span_data: Union[AgentSpanData, FunctionSpanData, GenerationSpanData, ResponseSpanData],
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
        if isinstance(span_data, AgentSpanData):
            return self._set_agent_attributes(agent_content, span_data), None
        return {}, None

    @staticmethod
    def _set_generation_attributes(
        span_data: GenerationSpanData,
    ) -> tuple[dict[str, AttributeValue], _AgentContent]:
        attributes: dict[str, AttributeValue] = {}
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, span_data.model)

        model_config = dict(span_data.model_config or {})
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
                OpenTelemetryTracingProcessor._first_not_none(*(model_config.get(key) for key in keys)),
            )

        usage = span_data.usage or {}
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_INPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                usage.get("input_tokens"),
                usage.get("prompt_tokens"),
            ),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_OUTPUT_TOKENS,
            OpenTelemetryTracingProcessor._first_not_none(
                usage.get("output_tokens"),
                usage.get("completion_tokens"),
            ),
        )

        system_instructions, input_messages = _GenAIMessageNormalizer.normalize_input_messages(span_data.input)
        output_messages = _GenAIMessageNormalizer.normalize_output_messages(span_data.output)
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content = _AgentContent(
            input_messages=input_messages or None,
            output_messages=output_messages or None,
            system_instructions=system_instructions or None,
            request_model=span_data.model,
        )
        return attributes, content

    @staticmethod
    def _set_response_attributes(span_data: ResponseSpanData) -> tuple[dict[str, AttributeValue], _AgentContent]:
        attributes: dict[str, AttributeValue] = {}
        response = span_data.response
        response_id = getattr(response, "id", None)
        response_model = getattr(response, "model", None)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_ID, response_id)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_MODEL, response_model)
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_REQUEST_MODEL, response_model)

        usage = getattr(span_data, "usage", None) or getattr(response, "usage", None)
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_INPUT_TOKENS,
            OpenTelemetryTracingProcessor._get_value(usage, "input_tokens"),
        )
        OpenTelemetryTracingProcessor._set_attribute(
            attributes,
            GEN_AI_USAGE_OUTPUT_TOKENS,
            OpenTelemetryTracingProcessor._get_value(usage, "output_tokens"),
        )

        system_instructions, input_messages = _GenAIMessageNormalizer.normalize_input_messages(
            getattr(span_data, "input", None)
        )
        response_instructions, _ = _GenAIMessageNormalizer.normalize_input_messages(
            {"role": "system", "content": getattr(response, "instructions", None)}
        )
        if response_instructions:
            system_instructions = response_instructions
        output_messages = _GenAIMessageNormalizer.normalize_output_messages(getattr(response, "output", None))
        OpenTelemetryTracingProcessor._set_message_attributes(
            attributes, system_instructions, input_messages, output_messages
        )
        finish_reasons = [message["finish_reason"] for message in output_messages if message.get("finish_reason")]
        OpenTelemetryTracingProcessor._set_attribute(attributes, GEN_AI_RESPONSE_FINISH_REASONS, finish_reasons or None)

        content = _AgentContent(
            input_messages=input_messages or None,
            output_messages=output_messages or None,
            system_instructions=system_instructions or None,
            request_model=response_model,
        )
        return attributes, content

    @staticmethod
    def _set_function_attributes(span_data: FunctionSpanData) -> dict[str, AttributeValue]:
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

    def _set_agent_attributes(
        self,
        content: Optional[_AgentContent],
        span_data: AgentSpanData,
    ) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {}
        self._set_attribute(attributes, GEN_AI_AGENT_NAME, span_data.name)
        content = content or _AgentContent()
        self._set_attribute(attributes, GEN_AI_REQUEST_MODEL, content.request_model)
        self._set_message_attributes(
            attributes,
            content.system_instructions or [],
            content.input_messages or [],
            content.output_messages or [],
        )
        return attributes

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
        error_type = OpenTelemetryTracingProcessor._first_not_none(
            data.get("type"),
            data.get("error_type"),
            data.get("exception_type"),
            "_OTHER",
        )
        span.set_attribute(ERROR_TYPE, error_type)
        span.set_status(Status(StatusCode.ERROR, description))

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
    def normalize_output_messages(cls, items: Any) -> list[dict[str, Any]]:
        """Normalize OpenAI Agents output payloads into GenAI semantic convention messages."""
        messages: list[dict[str, Any]] = []
        for item in cls._to_item_list(items):
            _, item_messages = cls._normalize_message(item, output=True)
            messages.extend(item_messages)
        return messages

    @classmethod
    def _normalize_message(  # pylint: disable=too-many-branches
        cls,
        item: Any,
        output: bool,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        system_instructions: list[dict[str, Any]] = []
        messages: list[dict[str, Any]] = []
        item_data = cls._as_mapping(item)

        if item_data is None:
            parts = cls._to_message_parts(item)
            if parts:
                message = {"role": "assistant" if output else "user", "parts": parts}
                if output:
                    message["finish_reason"] = "stop"
                messages.append(message)
            return system_instructions, messages

        item_type = item_data.get("type")
        role = item_data.get("role")

        if item_type == "function_call":
            part = {
                "type": "tool_call",
                "id": item_data.get("call_id") or item_data.get("id") or "",
                "name": item_data.get("name") or "",
                "arguments": cls._parse_arguments(item_data.get("arguments", {})),
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

        tool_call_parts = cls._to_tool_call_parts(item_data.get("tool_calls"))
        parts.extend(tool_call_parts)

        if role in ("system", "developer"):
            system_instructions.extend(parts)
            return system_instructions, messages

        if not role:
            role = "assistant" if output else "user"
        message = {"role": role, "parts": parts}
        if output:
            message["finish_reason"] = cls._normalize_finish_reason(item_data, bool(tool_call_parts))
        messages.append(message)
        return system_instructions, messages

    @staticmethod
    def _normalize_finish_reason(item: Mapping[str, Any], has_tool_calls: bool) -> str:
        if has_tool_calls:
            return "tool_call"
        raw_reason = item.get("finish_reason") or item.get("stop_reason")
        if raw_reason is None:
            return "stop"
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
        return content_to_parts(content)

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
        for summary_item in cls._to_item_list(item.get("summary")):
            summary = cls._as_mapping(summary_item)
            content = summary.get("text") if summary else summary_item
            if content:
                parts.append({"type": "reasoning", "content": str(content)})
        if not parts and item.get("content"):
            parts.append({"type": "reasoning", "content": str(item["content"])})
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
