# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

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


class _EventBusEmitWrapper:
    """Wrapper for crewai_event_bus.emit to run our event handler synchronously."""

    def __init__(self, event_handler: "OpenTelemetryEventHandler"):
        self._event_handler = event_handler

    def __call__(self, wrapped, instance, args, kwargs) -> Any:
        result = wrapped(*args, **kwargs)
        event = args[1] if len(args) > 1 else kwargs.get("event")
        if event:
            source = args[0] if args else kwargs.get("source")
            self._event_handler._handle_event(source, event)
        return result


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
        self._event_id_to_span: Dict[str, Tuple[trace.Span, Any]] = {}
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

    def _handle_event(self, source: Union["Crew", "BaseAgent", "ToolUsage", "LLM"], event: "BaseEvent") -> None:
        handler = self._event_type_handlers.get(type(event))
        if handler:
            handler(source, event)

    def _on_crew_start(self, source: "Crew", event: "CrewKickoffStartedEvent") -> None:
        # The span name "crew_kickoff {crew_name}" does not conform to any current OTel semantic
        # conventions. This is because CrewAI's workflow can contain multiple agents but there currently
        # does not exist any semantic convention naming schema to capture this system.

        crew_name = getattr(source, "name", None)
        span_name = f"crew_kickoff {crew_name}" if crew_name else "crew_kickoff"
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.INVOKE_AGENT.value,
        }
        if crew_name:
            attributes[GEN_AI_AGENT_NAME] = crew_name
        if hasattr(source, "id"):
            attributes[GEN_AI_AGENT_ID] = str(source.id)

        # As of OTel semconv v1.39.0, there are no semantic conventions that support
        # multi-agent systems. We intentionally do not set gen_ai.provider.name or
        # gen_ai.request.model here because a Crew can contain multiple agents with different
        # providers/models. Per-agent provider/model info is captured in child invoke_agent spans.
        # TODO: Revisit span attributes when OTel semconv adds multi-agent system support.
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
        self, source: "Crew", event: "CrewKickoffFailedEvent"  # pylint: disable=unused-argument
    ) -> None:
        self._fail_span(event.started_event_id, getattr(event, "error", None))

    def _on_agent_start(self, source: "BaseAgent", event: "AgentExecutionStartedEvent") -> None:
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
        self, source: "BaseAgent", event: "AgentExecutionCompletedEvent"  # pylint: disable=unused-argument
    ) -> None:
        self._end_span(event.started_event_id)

    def _on_agent_failed(
        self, source: "BaseAgent", event: "AgentExecutionErrorEvent"  # pylint: disable=unused-argument
    ) -> None:
        self._fail_span(event.started_event_id, getattr(event, "error", None))

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

        agent = getattr(event, "agent", None)
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
        self, source: "ToolUsage", event: "ToolUsageFinishedEvent"  # pylint: disable=unused-argument
    ) -> None:
        attrs: Dict[str, Any] = {}
        output = getattr(event, "output", None)
        if output is not None:
            attrs[GEN_AI_TOOL_CALL_RESULT] = serialize_to_json_string(output)
        self._end_span(event.started_event_id, attrs)

    def _on_llm_start(self, source: "LLM", event: "LLMCallStartedEvent") -> None:
        model = getattr(source, "model", None) or "unknown"
        span_name = f"{GenAiOperationNameValues.CHAT.value} {model}"
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: GenAiOperationNameValues.CHAT.value,
        }

        provider, model_name = self._extract_provider_and_model(source)
        if provider:
            attributes[GEN_AI_PROVIDER_NAME] = provider
        if model_name:
            attributes[GEN_AI_REQUEST_MODEL] = model_name

        messages = event.messages
        if messages:
            if isinstance(messages, str):
                messages = [{"role": "user", "content": messages}]
            attributes[GEN_AI_INPUT_MESSAGES] = serialize_to_json_string(messages)

        self._start_span(span_name, event.event_id, attributes, event.parent_event_id)

    def _on_llm_completed(self, source: "LLM", event: "LLMCallCompletedEvent") -> None:
        attrs: Dict[str, Any] = {}

        response = event.response
        if response is not None:
            if isinstance(response, str):
                attrs[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string([{"role": "assistant", "content": response}])
            elif isinstance(response, list):
                attrs[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(response)

        usage: "UsageMetrics" = source.get_token_usage_summary()
        if usage.prompt_tokens > 0:
            attrs[GEN_AI_USAGE_INPUT_TOKENS] = usage.prompt_tokens
        if usage.completion_tokens > 0:
            attrs[GEN_AI_USAGE_OUTPUT_TOKENS] = usage.completion_tokens

        self._end_span(event.started_event_id, attrs)

    def _on_llm_failed(self, source: Any, event: "LLMCallFailedEvent") -> None:  # pylint: disable=unused-argument
        self._fail_span(event.started_event_id, getattr(event, "error", None))

    def _start_span(
        self,
        name: str,
        event_id: str,
        attributes: Optional[Dict[str, Any]] = None,
        parent_event_id: Optional[str] = None,
    ) -> None:
        parent_ctx = None
        if parent_event_id:
            entry = self._event_id_to_span.get(parent_event_id)
            if entry:
                parent_ctx = trace.set_span_in_context(entry[0])

        span = self._tracer.start_span(name, kind=SpanKind.INTERNAL, attributes=attributes, context=parent_ctx)
        token = context.attach(trace.set_span_in_context(span))
        self._event_id_to_span[event_id] = (span, token)

    def _end_span(self, started_event_id: Optional[str], set_attrs: Optional[Dict[str, Any]] = None) -> None:
        if not started_event_id:
            return
        entry = self._event_id_to_span.pop(started_event_id, None)
        if not entry:
            return
        span, token = entry
        if set_attrs:
            for key, value in set_attrs.items():
                if value is not None:
                    span.set_attribute(key, value)
        span.set_status(Status(StatusCode.OK))
        span.end()
        context.detach(token)

    def _fail_span(self, started_event_id: Optional[str], error: Optional[str] = None) -> None:
        if not started_event_id:
            return
        entry = self._event_id_to_span.pop(started_event_id, None)
        if not entry:
            return
        span, token = entry
        span.set_status(Status(StatusCode.ERROR, error))
        if error:
            span.set_attribute(ERROR_MESSAGE, error)
        span.end()
        context.detach(token)

    @staticmethod
    def _extract_provider_and_model(llm: Any) -> Tuple[Optional[str], Optional[str]]:
        if not llm:
            return None, None
        model = getattr(llm, "model", None)
        if not model:
            return None, None
        provider = getattr(llm, "provider", None)
        if provider:
            return PROVIDER_MAP.get(provider.lower(), provider), model
        if "/" in model:
            prefix, _, model_part = model.partition("/")
            provider_name = PROVIDER_MAP.get(prefix.lower())
            return (provider_name or prefix), model_part
        return None, model

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
