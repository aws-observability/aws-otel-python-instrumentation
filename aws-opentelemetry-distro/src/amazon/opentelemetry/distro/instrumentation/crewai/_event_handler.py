# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import inspect
import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    PROVIDER_MAP,
    serialize_to_json_string,
)
from opentelemetry import context, trace
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_MESSAGE
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind, Status, StatusCode

if TYPE_CHECKING:
    from crewai.agents.agent_builder.base_agent import BaseAgent
    from crewai.crew import Crew
    from crewai.events.base_events import BaseEvent
    from crewai.events.types.agent_events import (
        AgentExecutionCompletedEvent,
        AgentExecutionErrorEvent,
        AgentExecutionStartedEvent,
    )
    from crewai.events.types.crew_events import (
        CrewKickoffCompletedEvent,
        CrewKickoffFailedEvent,
        CrewKickoffStartedEvent,
    )
    from crewai.events.types.llm_events import LLMCallCompletedEvent, LLMCallFailedEvent, LLMCallStartedEvent
    from crewai.events.types.tool_usage_events import ToolUsageFinishedEvent, ToolUsageStartedEvent
    from crewai.llm import LLM
    from crewai.tools.tool_usage import ToolUsage
    from crewai.types.usage_metrics import UsageMetrics

_LOG = logging.getLogger(__name__)


@dataclass
class _SpanEntry:
    span: trace.Span
    token: Any


class _EventBusEmitWrapper:
    """Wrapper for crewai_event_bus.emit to run our event handler synchronously."""

    def __init__(self, event_handler: "OpenTelemetryEventHandler"):
        self._event_handler = event_handler

    def __call__(self, wrapped, instance, args, kwargs) -> Any:
        from crewai.events.base_events import BaseEvent  # pylint: disable=import-outside-toplevel

        result = wrapped(*args, **kwargs)
        event = args[1] if len(args) > 1 else kwargs.get("event")
        if isinstance(event, BaseEvent):
            source = args[0] if args else kwargs.get("source")
            self._event_handler._handle_event(source, event)
        return result


class _LLMToolCallCompletedEventPatch:
    # ports fix from: https://github.com/crewAIInc/crewAI/pull/4880
    # tldr:
    # LLMCallCompletedEvent is not emitted when tool_calls are returned.
    # CrewAI's _handle_non_streaming_response returns tool_calls without emitting
    # LLMCallCompletedEvent, corrupting the event scope stack and breaking started_event_id.

    def __call__(self, wrapped, instance, args, kwargs) -> Any:
        if inspect.iscoroutinefunction(wrapped):
            return self._async_call(wrapped, instance, args, kwargs)
        result = wrapped(*args, **kwargs)
        self._emit_if_tool_calls(instance, result, args, kwargs)
        return result

    async def _async_call(self, wrapped, instance, args, kwargs) -> Any:
        result = await wrapped(*args, **kwargs)
        self._emit_if_tool_calls(instance, result, args, kwargs)
        return result

    @staticmethod
    def _emit_if_tool_calls(instance, result, args, kwargs):
        if isinstance(result, list) and result and hasattr(result[0], "function"):
            try:
                from crewai.events.types.llm_events import LLMCallType  # pylint: disable=import-outside-toplevel

                instance._handle_emit_call_events(  # pylint: disable=protected-access
                    response=result,
                    call_type=LLMCallType.TOOL_CALL,
                    from_task=kwargs.get("from_task") or (args[3] if len(args) > 3 else None),
                    from_agent=kwargs.get("from_agent") or (args[4] if len(args) > 4 else None),
                )
            except Exception:  # pylint: disable=broad-exception-caught
                _LOG.debug("Failed to emit LLMCallCompletedEvent for tool_calls")


class OpenTelemetryEventHandler:

    def __init__(self, tracer: trace.Tracer) -> None:
        # pylint: disable=import-outside-toplevel
        from crewai.events.types.agent_events import (
            AgentExecutionCompletedEvent,
            AgentExecutionErrorEvent,
            AgentExecutionStartedEvent,
        )
        from crewai.events.types.crew_events import (
            CrewKickoffCompletedEvent,
            CrewKickoffFailedEvent,
            CrewKickoffStartedEvent,
        )
        from crewai.events.types.llm_events import LLMCallCompletedEvent, LLMCallFailedEvent, LLMCallStartedEvent
        from crewai.events.types.tool_usage_events import ToolUsageFinishedEvent, ToolUsageStartedEvent

        self._tracer = tracer
        self._lock = threading.Lock()
        # a map of every event's id to its span. If the event does not 
        # create a span, then it's mapped to the span created by its nearest ancestor event
        self._event_id_to_span_entry_map: Dict[str, _SpanEntry] = {}
        self._event_type_handlers: Dict[type, Any] = {
            CrewKickoffStartedEvent: self._on_crew_start,
            CrewKickoffCompletedEvent: self._on_crew_completed,
            CrewKickoffFailedEvent: self._on_crew_failed,
            AgentExecutionStartedEvent: self._on_agent_start,
            AgentExecutionCompletedEvent: self._on_agent_completed,
            AgentExecutionErrorEvent: self._on_agent_failed,
            ToolUsageStartedEvent: self._on_tool_start,
            ToolUsageFinishedEvent: self._on_tool_finished,
            LLMCallStartedEvent: self._on_llm_start,
            LLMCallCompletedEvent: self._on_llm_completed,
            LLMCallFailedEvent: self._on_llm_failed,
        }

    def _handle_event(self, source: Any, event: "BaseEvent") -> None:
        handler = self._event_type_handlers.get(type(event))
        if handler:
            handler(source, event)
        else:
            event_id = getattr(event, "event_id", None)
            parent_event_id = getattr(event, "parent_event_id", None)
            if event_id and parent_event_id:
                parent_entry = self._get_entry(parent_event_id)
                if parent_entry:
                    with self._lock:
                        self._event_id_to_span_entry_map[event_id] = parent_entry

    def _on_crew_start(self, source: "Crew", event: "CrewKickoffStartedEvent") -> None:
        crew_name = getattr(source, "name", None)
        span_name = f"crew_kickoff {crew_name}" if crew_name else "crew_kickoff"
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.INVOKE_AGENT.value,
        }
        if crew_name:
            attributes[GEN_AI_AGENT_NAME] = crew_name
        if hasattr(source, "id"):
            attributes[GEN_AI_AGENT_ID] = str(source.id)

        agents = getattr(source, "agents", [])
        if agents:
            all_tools = []
            for agent in agents:
                all_tools.extend(getattr(agent, "tools", []) or [])
            if all_tools:
                tool_defs = self._extract_tool_definitions(all_tools)
                if tool_defs:
                    attributes[GEN_AI_TOOL_DEFINITIONS] = serialize_to_json_string(tool_defs)

        self._start_span(span_name, event.event_id, attributes, event.parent_event_id)

    def _on_crew_completed(
        self, source: "Crew", event: "CrewKickoffCompletedEvent"  # pylint: disable=unused-argument
    ) -> None:
        self._end_span(event.started_event_id)

    def _on_crew_failed(
        self, source: "Crew", event: "CrewKickoffFailedEvent"
    ) -> None:  # pylint: disable=unused-argument
        self._on_error_span(event.started_event_id, getattr(event, "error", None))

    def _on_agent_start(
        self, source: "BaseAgent", event: "AgentExecutionStartedEvent"
    ) -> None:  # pylint: disable=unused-argument
        agent = getattr(event, "agent", None)
        agent_role = getattr(agent, "role", None) if agent else None
        span_name = (
            f"{GenAiOperationNameValues.INVOKE_AGENT.value} {agent_role}"
            if agent_role
            else GenAiOperationNameValues.INVOKE_AGENT.value
        )
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.INVOKE_AGENT.value,
        }
        if agent:
            if agent_role:
                attributes[GEN_AI_AGENT_NAME] = agent_role
            if hasattr(agent, "id"):
                attributes[GEN_AI_AGENT_ID] = str(agent.id)
            goal = getattr(agent, "goal", None)
            if goal:
                attributes[GEN_AI_AGENT_DESCRIPTION] = goal
            backstory = getattr(agent, "backstory", None)
            if backstory:
                attributes[GEN_AI_SYSTEM_INSTRUCTIONS] = backstory

            llm = getattr(agent, "llm", None)
            provider, model = self._extract_provider_and_model(llm)
            if provider:
                attributes[GEN_AI_PROVIDER_NAME] = provider
            if model:
                attributes[GEN_AI_REQUEST_MODEL] = model
            if llm:
                temperature = getattr(llm, "temperature", None)
                if temperature is not None:
                    attributes[GEN_AI_REQUEST_TEMPERATURE] = temperature
                max_tokens = getattr(llm, "max_tokens", None)
                if max_tokens is not None:
                    attributes[GEN_AI_REQUEST_MAX_TOKENS] = max_tokens

        self._start_span(span_name, event.event_id, attributes, event.parent_event_id)

    def _on_agent_completed(
        self, source: "BaseAgent", event: "AgentExecutionCompletedEvent"
    ) -> None:  # pylint: disable=unused-argument
        self._end_span(event.started_event_id)

    def _on_agent_failed(
        self, source: "BaseAgent", event: "AgentExecutionErrorEvent"
    ) -> None:  # pylint: disable=unused-argument
        self._on_error_span(event.started_event_id, getattr(event, "error", None))

    def _on_tool_start(self, source: "ToolUsage", event: "ToolUsageStartedEvent") -> None:
        tool_name = event.tool_name
        span_name = (
            f"{GenAiOperationNameValues.EXECUTE_TOOL.value} {tool_name}"
            if tool_name
            else GenAiOperationNameValues.EXECUTE_TOOL.value
        )
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.EXECUTE_TOOL.value,
            GEN_AI_TOOL_TYPE: "function",
        }
        if tool_name:
            attributes[GEN_AI_TOOL_NAME] = tool_name

        tools = getattr(source, "tools", None)
        if tools and tool_name:
            for tool in tools:
                if getattr(tool, "name", None) == tool_name:
                    desc = getattr(tool, "description", None)
                    if desc:
                        attributes[GEN_AI_TOOL_DESCRIPTION] = desc
                    break

        agent = getattr(source, "agent", None)
        if agent:
            llm = getattr(agent, "llm", None)
            provider, model = self._extract_provider_and_model(llm)
            if provider:
                attributes[GEN_AI_PROVIDER_NAME] = provider
            if model:
                attributes[GEN_AI_REQUEST_MODEL] = model

        if event.tool_args:
            attributes[GEN_AI_TOOL_CALL_ARGUMENTS] = serialize_to_json_string(event.tool_args)

        self._start_span(span_name, event.event_id, attributes, event.parent_event_id)

    def _on_tool_finished(
        self, source: "ToolUsage", event: "ToolUsageFinishedEvent"
    ) -> None:  # pylint: disable=unused-argument
        attrs: Dict[str, Any] = {}
        output = getattr(event, "output", None)
        if output is not None:
            attrs[GEN_AI_TOOL_CALL_RESULT] = serialize_to_json_string(output)
        self._end_span(event.started_event_id, attrs)

    def _on_llm_start(self, source: "LLM", event: "LLMCallStartedEvent") -> None:
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.CHAT.value,
        }

        provider, model_name = self._extract_provider_and_model(source)
        if provider:
            attributes[GEN_AI_PROVIDER_NAME] = provider
        if model_name:
            attributes[GEN_AI_REQUEST_MODEL] = model_name

        span_name = (
            f"{GenAiOperationNameValues.CHAT.value} {model_name}" if model_name else GenAiOperationNameValues.CHAT.value
        )

        messages = event.messages
        if messages:
            if isinstance(messages, str):
                messages = [{"role": "user", "content": messages}]
            system_instructions = [m for m in messages if m.get("role") == "system"]
            non_system_messages = [m for m in messages if m.get("role") != "system"]
            if system_instructions:
                parts = [self._to_text_part(m.get("content", "")) for m in system_instructions]
                attributes[GEN_AI_SYSTEM_INSTRUCTIONS] = serialize_to_json_string(parts)
            if non_system_messages:
                attributes[GEN_AI_INPUT_MESSAGES] = serialize_to_json_string(
                    [self._to_chat_message(m) for m in non_system_messages]
                )

        self._start_span(span_name, event.event_id, attributes, event.parent_event_id)

    def _on_llm_completed(self, source: "LLM", event: "LLMCallCompletedEvent") -> None:
        attrs: Dict[str, Any] = {}

        from crewai.events.types.llm_events import LLMCallType  # pylint: disable=import-outside-toplevel

        finish_reason = "tool_calls" if event.call_type == LLMCallType.TOOL_CALL else "stop"

        response = event.response
        if response is not None:
            if isinstance(response, str):
                attrs[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(
                    [self._to_output_message({"role": "assistant", "content": response}, finish_reason)]
                )
            elif isinstance(response, list) and response and isinstance(response[0], dict):
                attrs[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(
                    [self._to_output_message(m, finish_reason) for m in response]
                )
            elif isinstance(response, list) and response and hasattr(response[0], "function"):
                tool_calls = []
                for tc in response:
                    tool_calls.append(
                        {
                            "id": getattr(tc, "id", None),
                            "type": "function",
                            "function": {
                                "name": getattr(tc.function, "name", None),
                                "arguments": getattr(tc.function, "arguments", None),
                            },
                        }
                    )
                attrs[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(
                    [{"role": "assistant", "parts": tool_calls, "finish_reason": finish_reason}]
                )

        _, model_name = self._extract_provider_and_model(source)
        if model_name:
            attrs[GEN_AI_RESPONSE_MODEL] = model_name

        usage: "UsageMetrics" = source.get_token_usage_summary()
        if usage.prompt_tokens > 0:
            attrs[GEN_AI_USAGE_INPUT_TOKENS] = usage.prompt_tokens
        if usage.completion_tokens > 0:
            attrs[GEN_AI_USAGE_OUTPUT_TOKENS] = usage.completion_tokens

        self._end_span(event.started_event_id, attrs)

    def _on_llm_failed(self, source: Any, event: "LLMCallFailedEvent") -> None:  # pylint: disable=unused-argument
        self._on_error_span(event.started_event_id, getattr(event, "error", None))

    def _get_entry(self, event_id: Optional[str]) -> Optional[_SpanEntry]:
        if not event_id:
            return None
        with self._lock:
            return self._event_id_to_span_entry_map.get(event_id)

    def _start_span(
        self,
        name: str,
        event_id: str,
        attributes: Optional[Dict[str, Any]] = None,
        parent_event_id: Optional[str] = None,
    ) -> None:
        parent_ctx = None
        parent_entry = self._get_entry(parent_event_id)
        if parent_entry:
            parent_ctx = trace.set_span_in_context(parent_entry.span)

        span = self._tracer.start_span(name, kind=SpanKind.INTERNAL, attributes=attributes, context=parent_ctx)
        token = context.attach(trace.set_span_in_context(span))
        with self._lock:
            self._event_id_to_span_entry_map[event_id] = _SpanEntry(span=span, token=token)

    def _end_span(self, started_event_id: Optional[str], attributes: Optional[Dict[str, Any]] = None) -> None:
        if not started_event_id:
            return
        with self._lock:
            entry = self._event_id_to_span_entry_map.pop(started_event_id, None)
        if not entry:
            return
        if attributes:
            for key, value in attributes.items():
                if value is not None:
                    entry.span.set_attribute(key, value)
        entry.span.set_status(Status(StatusCode.OK))
        entry.span.end()
        context.detach(entry.token)

    def _on_error_span(self, started_event_id: Optional[str], error: Optional[str] = None) -> None:
        if not started_event_id:
            return
        with self._lock:
            entry = self._event_id_to_span_entry_map.pop(started_event_id, None)
        if not entry:
            return
        entry.span.set_status(Status(StatusCode.ERROR, error))
        if error:
            entry.span.set_attribute(ERROR_MESSAGE, error)
        entry.span.end()
        context.detach(entry.token)

    @staticmethod
    def _extract_provider_and_model(llm: Any) -> Tuple[Optional[str], Optional[str]]:
        if not llm:
            return None, None
        model = getattr(llm, "model", None)
        if not model:
            return None, None
        if "/" in model:
            prefix, _, model_part = model.partition("/")
            provider_name = PROVIDER_MAP.get(prefix.lower(), prefix)
            return provider_name, model_part
        provider = getattr(llm, "provider", None)
        if provider:
            return PROVIDER_MAP.get(provider.lower(), provider), model
        return None, model

    @staticmethod
    def _to_text_part(content: str) -> Dict[str, str]:
        return {"type": "text", "content": content}

    @staticmethod
    def _to_chat_message(msg: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "role": msg.get("role", "user"),
            "parts": [{"type": "text", "content": msg.get("content", "")}],
        }

    @staticmethod
    def _to_output_message(msg: Dict[str, Any], finish_reason: str = "stop") -> Dict[str, Any]:
        return {
            "role": msg.get("role", "assistant"),
            "parts": [{"type": "text", "content": msg.get("content", "")}],
            "finish_reason": finish_reason,
        }

    @staticmethod
    def _extract_tool_definitions(tools: Any) -> list:
        defs = []
        for tool in tools:
            tool_def: Dict[str, Any] = {"type": "function"}
            if name := getattr(tool, "name", None):
                tool_def["name"] = name
            if desc := getattr(tool, "description", None):
                tool_def["description"] = desc
            args_schema = getattr(tool, "args_schema", None)
            if args_schema is not None:
                try:
                    tool_def["parameters"] = args_schema.model_json_schema()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            defs.append(tool_def)
        return defs
