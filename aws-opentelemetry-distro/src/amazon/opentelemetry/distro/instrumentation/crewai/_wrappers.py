# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional, Tuple

from amazon.opentelemetry.distro.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_PROVIDER_NAME,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
)
from opentelemetry import context, trace
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
)
from opentelemetry.trace import SpanKind, Status, StatusCode

if TYPE_CHECKING:
    from crewai.agent import Agent
    from crewai.crew import Crew
    from crewai.llm import LLM
    from crewai.task import Task
    from crewai.tools.base_tool import BaseTool
    from pydantic import BaseModel

_OPERATION_INVOKE_AGENT = "invoke_agent"
_OPERATION_EXECUTE_TOOL = "execute_tool"


class _BaseWrapper(ABC):
    """Base wrapper class for CrewAI instrumentation."""

    # see: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    # under gen_ai.provider.name
    _PROVIDER_MAP = {
        "bedrock": "aws.bedrock",
        "aws": "aws.bedrock",
        "openai": "openai",
        "anthropic": "anthropic",
        "claude": "anthropic",
        "azure": "azure.ai.openai",
        "azure_openai": "azure.ai.openai",
        "google": "gcp.vertex_ai",
        "gemini": "gcp.gemini",
        "cohere": "cohere",
        "mistral": "mistral_ai",
        "groq": "groq",
        "deepseek": "deepseek",
        "perplexity": "perplexity",
    }

    def __init__(self, tracer: Optional[trace.Tracer] = None) -> None:
        self._tracer = tracer or trace.get_tracer(__name__)

    def __call__(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> Any:
        if context.get_value(context._SUPPRESS_INSTRUMENTATION_KEY):
            return wrapped(*args, **kwargs)

        pre_call_state = self._before_call(instance, args, kwargs)  # pylint: disable=assignment-from-no-return

        with self._tracer.start_as_current_span(
            self._get_span_name(instance, args, kwargs),
            kind=SpanKind.INTERNAL,
            attributes=self._get_attributes(instance, args, kwargs),
        ) as span:
            try:
                result = wrapped(*args, **kwargs)
                self._on_success(span, result, instance, pre_call_state)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as exc:  # pylint: disable=broad-exception-caught
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.set_attribute(ERROR_TYPE, type(exc).__name__)
                span.record_exception(exc)
                raise

    def _set_agent_llm_attributes(self, attributes: Dict[str, Any], agent: Any) -> None:
        if not agent:
            return
        llm = getattr(agent, "llm", None)
        provider, model = self._extract_provider_and_model(llm)
        if provider:
            attributes[GEN_AI_PROVIDER_NAME] = provider
        if model:
            attributes[GEN_AI_REQUEST_MODEL] = model

    def _extract_provider_and_model(self, llm: Optional["LLM"]) -> Tuple[Optional[str], Optional[str]]:
        if not llm:
            return None, None

        model = getattr(llm, "model", None)
        if not model:
            return None, None

        provider = getattr(llm, "provider", None)
        if provider:
            provider_name = self._PROVIDER_MAP.get(provider.lower(), provider)
            return provider_name, model

        if "/" in model:
            prefix, _, model_part = model.partition("/")
            provider_name = self._PROVIDER_MAP.get(prefix.lower())
            if provider_name:
                return provider_name, model_part
            return prefix, model_part

        return None, model

    @staticmethod
    def _serialize_to_json(value: Any, max_depth: int = 10) -> str:
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

    @abstractmethod
    def _get_span_name(self, instance: Any, args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
        pass

    @abstractmethod
    def _get_attributes(self, instance: Any, args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        pass

    def _before_call(  # pylint: disable=no-self-use
        self, instance: Any, args: Tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> Any:
        """Hook called before wrapped function execution. Returns state to pass to _on_success."""

    def _on_success(self, span: trace.Span, result: Any, instance: Any = None, pre_call_state: Any = None) -> None:
        """Hook called on successful execution."""


class _CrewKickoffWrapper(_BaseWrapper):
    # wraps Crew.kickoff which is responsible for starting the agentic workflow.
    # see:
    # https://github.com/crewAIInc/crewAI/blob/06d953bf46c636ff9f2d64f45574493d05fb7771/lib/crewai/src/crewai/crew.py#L676-L679
    # Note: The span name "crew_kickoff {crew_name}" does not conform to any current OTel semantic
    # conventions. This is because CrewAI's workflow can contain multiple agents but there currently
    # does not exist any semantic convention naming schema to capture this system.

    def _get_span_name(self, instance: "Crew", args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
        crew_name = getattr(instance, "name", None)
        return f"crew_kickoff {crew_name}" if crew_name else "crew_kickoff"

    def _get_attributes(self, instance: "Crew", args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        # Note: As of OTel semconv v1.39.0, there are no semantic conventions that support
        # multi-agent systems. We intentionally do not set gen_ai.provider.name or
        # gen_ai.request.model here because a Crew can contain multiple agents with different
        # providers/models. Per-agent provider/model info is captured in child invoke_agent spans.
        # TODO: Revisit span attributes when OTel semconv adds multi-agent system support.
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: _OPERATION_INVOKE_AGENT,
        }

        crew_name = getattr(instance, "name", None)
        if crew_name:
            attributes[GEN_AI_AGENT_NAME] = crew_name
        if hasattr(instance, "id"):
            attributes[GEN_AI_AGENT_ID] = str(instance.id)

        agents = getattr(instance, "agents", [])
        if agents:
            all_tools = []
            for agent in agents:
                all_tools.extend(getattr(agent, "tools", []) or [])
            if all_tools:
                tool_defs = self._extract_tool_definitions(all_tools)
                if tool_defs:
                    attributes[GEN_AI_TOOL_DEFINITIONS] = self._serialize_to_json(tool_defs)

        return attributes

    @staticmethod
    def _extract_tool_definitions(tools: Any) -> list:
        defs = []
        for tool in tools:
            tool_def: Dict[str, Any] = {"type": "function"}
            if name := getattr(tool, "name", None):
                tool_def["name"] = name
            if desc := getattr(tool, "description", None):
                tool_def["description"] = desc
            args_schema: Optional[type["BaseModel"]] = getattr(tool, "args_schema", None)
            if args_schema is not None:
                try:
                    tool_def["parameters"] = args_schema.model_json_schema()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            defs.append(tool_def)
        return defs


class _TaskExecuteCoreWrapper(_BaseWrapper):
    # wraps Task._execute_core which is responsible for running a single task
    # with its assigned agent.
    # see:
    # https://github.com/crewAIInc/crewAI/blob/06d953bf46c636ff9f2d64f45574493d05fb7771/lib/crewai/src/crewai/task.py#L604-L608

    def _get_span_name(self, instance: "Task", args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
        agent: Optional[Agent] = args[0] if args else kwargs.get("agent")
        agent_role = getattr(agent, "role", None) if agent else None
        return f"{_OPERATION_INVOKE_AGENT} {agent_role}" if agent_role else _OPERATION_INVOKE_AGENT

    def _get_attributes(self, instance: "Task", args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        agent: Optional[Agent] = args[0] if args else kwargs.get("agent")
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: _OPERATION_INVOKE_AGENT,
        }

        if agent:
            agent_role = getattr(agent, "role", None)
            if agent_role:
                attributes[GEN_AI_AGENT_NAME] = agent_role
            if hasattr(agent, "id"):
                attributes[GEN_AI_AGENT_ID] = str(agent.id)

            goal = getattr(agent, "goal", None)
            if goal:
                attributes[GEN_AI_AGENT_DESCRIPTION] = goal

            self._set_agent_llm_attributes(attributes, agent)

            llm = getattr(agent, "llm", None)
            if llm:
                temperature = getattr(llm, "temperature", None)
                if temperature is not None:
                    attributes[GEN_AI_REQUEST_TEMPERATURE] = temperature
                max_tokens = getattr(llm, "max_tokens", None)
                if max_tokens is not None:
                    attributes[GEN_AI_REQUEST_MAX_TOKENS] = max_tokens

            backstory = getattr(agent, "backstory", None)
            if backstory:
                attributes[GEN_AI_SYSTEM_INSTRUCTIONS] = backstory

        return attributes


class _BaseToolRunWrapper(_BaseWrapper):
    # Wraps BaseTool.run which is the base execution point for all tool calls.
    # see:
    # https://github.com/crewAIInc/crewAI/blob/6bb1b178a10139160575cccc4ce1be62800b28c0/lib/crewai/src/crewai/tools/base_tool.py#L157

    def _get_span_name(self, instance: "BaseTool", args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
        tool_name = getattr(instance, "name", None)
        return f"{_OPERATION_EXECUTE_TOOL} {tool_name}" if tool_name else _OPERATION_EXECUTE_TOOL

    def _get_attributes(
        self, instance: "BaseTool", args: Tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> Dict[str, Any]:
        attributes: Dict[str, Any] = {
            GEN_AI_OPERATION_NAME: _OPERATION_EXECUTE_TOOL,
            GEN_AI_TOOL_TYPE: "function",
        }

        tool_name = getattr(instance, "name", None)
        if tool_name:
            attributes[GEN_AI_TOOL_NAME] = tool_name

        tool_desc = getattr(instance, "description", None)
        if tool_desc:
            attributes[GEN_AI_TOOL_DESCRIPTION] = tool_desc

        # Capture tool arguments from args/kwargs
        tool_args = args[0] if args else kwargs
        if tool_args:
            attributes[GEN_AI_TOOL_CALL_ARGUMENTS] = self._serialize_to_json(tool_args)

        return attributes

    def _on_success(self, span: trace.Span, result: Any, instance: Any = None, pre_call_state: Any = None) -> None:
        if result is not None:
            span.set_attribute(GEN_AI_TOOL_CALL_RESULT, self._serialize_to_json(result))
