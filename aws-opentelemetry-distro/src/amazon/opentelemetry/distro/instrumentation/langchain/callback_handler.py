# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from contextvars import Token
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    PROVIDER_MAP,
    serialize_to_json_string,
    try_detach,
)
from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROMPT,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import SpanKind, set_span_in_context
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util.types import AttributeValue

if TYPE_CHECKING:
    from langchain_core.agents import AgentAction, AgentFinish
    from langchain_core.messages import BaseMessage
    from langchain_core.outputs import LLMResult

_logger = logging.getLogger(__name__)

LANGGRAPH_STEP_SPAN_ATTR = "langgraph.step"
LANGGRAPH_NODE_SPAN_ATTR = "langgraph.node"

# We use "invoke_model" instead of the OTel semconv "chat" for span names because "chat" is
# a bit ambiguous it could refer to the user's chat session or the agent's conversation. We feel
# that "invoke_model" makes it clear the span represents the agent calling
# the underlying model. The gen_ai.operation.name attribute still uses the semconv value.
INVOKE_MODEL = "invoke_model"


class _BaseCallbackManagerInitWrapper:
    """Wrapper for BaseCallbackManager.__init__ to inject OpenTelemetry callback handler."""

    def __init__(self, callback_handler: "OpenTelemetryCallbackHandler"):
        self.callback_handler = callback_handler

    def __call__(self, wrapped, instance, args, kwargs) -> None:
        wrapped(*args, **kwargs)
        if not hasattr(instance, "inheritable_handlers") or not hasattr(instance, "handlers"):
            _logger.debug("Missing handler lists on %s, skipping OTel callback injection.", type(instance).__name__)
            return
        for handler in instance.inheritable_handlers:
            if isinstance(handler, OpenTelemetryCallbackHandler):
                return
        # OTel handler must be first so that the
        # span context is properly propagated to downstream spans
        instance.inheritable_handlers.insert(0, self.callback_handler)
        instance.handlers.insert(0, self.callback_handler)


class OpenTelemetryCallbackHandler(BaseCallbackHandler):
    # Ensures the OTel callback is executed synchronously and not in an async thread.
    # This is to ensure that we are ALWAYS setting this instrumentation's spans as the current span in context to make
    # sure we propagate the trace to downstream
    # https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/callbacks/manager.py#L381-L390
    run_inline = True

    def __init__(self, tracer, should_suppress_internal_chains: bool = True):
        super().__init__()
        self.tracer = tracer
        self.should_suppress_internal_chains = should_suppress_internal_chains
        self.run_id_to_span_map: dict[UUID, tuple[Span, Token]] = {}

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return

        model_id: str | None = kwargs.get("invocation_params", {}).get("model_id")
        model_name: str | None = model_id or self._get_name_from_callback(serialized, **kwargs)
        provider: str | None = self._extract_llm_provider(serialized, kwargs)
        system_instructions, conversation = self._format_lc_messages(messages)
        span_name: str = f"{INVOKE_MODEL} {model_name}" if model_name else INVOKE_MODEL

        span: Span = self._start_span(run_id, parent_run_id, span_name)

        self._set_langgraph_span_attributes(span, metadata)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        self._set_span_attribute(span, GEN_AI_INPUT_MESSAGES, serialize_to_json_string(conversation))

        if system_instructions:
            self._set_span_attribute(span, GEN_AI_SYSTEM_INSTRUCTIONS, serialize_to_json_string(system_instructions))
        self._set_llm_request_span_attributes(
            span, kwargs, serialized=serialized.get("kwargs", {}), model_name=model_name
        )

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return

        model_id: str | None = kwargs.get("invocation_params", {}).get("model_id")
        model_name: str | None = model_id or self._get_name_from_callback(serialized, **kwargs)
        provider: str | None = self._extract_llm_provider(serialized, kwargs)
        span_name: str = (
            f"{GenAiOperationNameValues.TEXT_COMPLETION.value} {model_name}"
            if model_name
            else GenAiOperationNameValues.TEXT_COMPLETION.value
        )
        span: Span = self._start_span(run_id, parent_run_id, span_name)

        self._set_langgraph_span_attributes(span, metadata)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.TEXT_COMPLETION.value)
        self._set_span_attribute(span, GEN_AI_PROMPT, serialize_to_json_string(prompts))
        self._set_llm_request_span_attributes(
            span, kwargs, serialized=serialized.get("kwargs", {}), model_name=model_name
        )

    def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY) or run_id not in self.run_id_to_span_map:
            return

        span, _ = self.run_id_to_span_map[run_id]
        llm_output: dict | None = response.llm_output
        usage: dict = (llm_output.get("token_usage") or llm_output.get("usage") or {}) if llm_output else {}
        model: str | None = (llm_output.get("model_name") or llm_output.get("model_id")) if llm_output else None
        response_id: str | None = llm_output.get("id") if llm_output else None
        input_tokens: int | None = (
            usage.get("prompt_tokens") or usage.get("input_token_count") or usage.get("input_tokens")
        )
        output_tokens: int | None = (
            usage.get("completion_tokens") or usage.get("generated_token_count") or usage.get("output_tokens")
        )

        if response.generations:
            self._set_span_attribute(
                span, GEN_AI_OUTPUT_MESSAGES, serialize_to_json_string(self._format_lc_llm_output(response.generations))
            )

        self._set_span_attribute(span, GEN_AI_RESPONSE_MODEL, model)
        self._set_span_attribute(span, GEN_AI_RESPONSE_ID, response_id)
        self._set_span_attribute(span, GEN_AI_USAGE_INPUT_TOKENS, input_tokens)
        self._set_span_attribute(span, GEN_AI_USAGE_OUTPUT_TOKENS, output_tokens)

        self._end_span(run_id)

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    def on_chain_start(
        self,
        serialized: dict[str, Any],
        inputs: dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return

        name: str | None = self._get_name_from_callback(serialized, **kwargs)
        if self._should_skip_chain(serialized, name, metadata):
            return

        # AgentExecutor is the legacy LangChain agent node, lc_agent_name metadata was added in
        # langchain >= 1.2.4 this is only set when a custom name is given to the agent,
        # otherwise if no name is given it defaults to "LangGraph".
        # langgraph_node check ensures we only match against agent nodes, not unwanted
        # internal nodes.
        is_agent_chain: bool = bool(name) and (
            "AgentExecutor" in name or name == "LangGraph" or name == (metadata or {}).get("lc_agent_name")
        )

        provider: str | None = self._extract_llm_provider(serialized, kwargs)
        agent_name: str | None = (metadata.get("lc_agent_name") if metadata else None) or (
            name if is_agent_chain else None
        )
        operation: str = GenAiOperationNameValues.INVOKE_AGENT.value if is_agent_chain else "chain"
        span_name: str = f"{operation} {name}" if name else operation

        span: Span = self._start_span(run_id, parent_run_id, span_name)

        self._set_langgraph_span_attributes(span, metadata)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        if is_agent_chain:
            self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        self._set_span_attribute(span, GEN_AI_AGENT_NAME, agent_name)

    def on_chain_end(self, outputs: dict[str, Any], *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        self._end_span(run_id)

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return

        name: str | None = self._get_name_from_callback(serialized, **kwargs)
        provider: str | None = self._extract_llm_provider(serialized, kwargs)
        tool_call_id: str | None = serialized.get("id")
        description: str | None = serialized.get("description")
        span_name: str = (
            f"{GenAiOperationNameValues.EXECUTE_TOOL.value} {name}"
            if name
            else GenAiOperationNameValues.EXECUTE_TOOL.value
        )
        span: Span = self._start_span(run_id, parent_run_id, span_name)

        self._set_langgraph_span_attributes(span, metadata)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)
        self._set_span_attribute(span, GEN_AI_TOOL_NAME, name)
        self._set_span_attribute(span, GEN_AI_TOOL_TYPE, "function")
        self._set_span_attribute(span, GEN_AI_TOOL_DESCRIPTION, description)
        self._set_span_attribute(span, GEN_AI_TOOL_CALL_ID, tool_call_id)
        self._set_span_attribute(span, GEN_AI_TOOL_CALL_ARGUMENTS, serialize_to_json_string(input_str))

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        span, _ = self.run_id_to_span_map[run_id]
        self._set_span_attribute(span, GEN_AI_TOOL_CALL_RESULT, serialize_to_json_string(output))
        self._end_span(run_id)

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    def on_agent_action(self, action: AgentAction, *, run_id: UUID, **kwargs: Any) -> None:
        if run_id in self.run_id_to_span_map:
            span, _ = self.run_id_to_span_map[run_id]

            self._set_span_attribute(
                span, GEN_AI_TOOL_CALL_ARGUMENTS, serialize_to_json_string(getattr(action, "tool_input", None))
            )
            self._set_span_attribute(span, GEN_AI_TOOL_NAME, getattr(action, "tool", None))
            self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)

    def on_agent_finish(self, finish: AgentFinish, *, run_id: UUID, **kwargs: Any) -> None:
        if run_id in self.run_id_to_span_map:
            span, _ = self.run_id_to_span_map[run_id]
            self._set_span_attribute(
                span,
                GEN_AI_TOOL_CALL_RESULT,
                serialize_to_json_string(finish.return_values.get("output")),
            )

    def on_agent_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    @staticmethod
    def _format_lc_messages(messages: list[list[BaseMessage]]) -> tuple[list[dict], list[dict]]:
        # converts langchain messages to OTel format conversation and system instructions format based on
        # the following schemas -
        # https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-input-messages.json
        # https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-output-messages.json
        # https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-system-instructions.json
        #
        # example LangChain input based on:
        # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/messages/human.py#L31
        # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/messages/system.py#L24
        # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/messages/tool.py#L53
        #
        #   [[
        #     SystemMessage(type="system", content="You are a helpful assistant."),
        #     HumanMessage(type="human", content="What is the weather in Paris?"),
        #     AIMessage(type="ai", content="Let me check.", tool_calls=[
        #         {"name": "get_weather", "args": {"city": "Paris"}, "id": "call_abc123", "type": "tool_call"}
        #     ]),
        #   ]]
        #
        #
        # example OTel output
        #
        #   system_instructions:
        #     [{"type": "text", "content": "You are a helpful assistant."}]
        #
        #   conversation:
        #     [
        #       {"role": "user", "parts": [{"type": "text", "content": "What is the weather in Paris?"}]},
        #       {"role": "assistant", "parts": [
        #           {"type": "text", "content": "Let me check."},
        #           {"type": "tool_call", "id": "call_abc123", "name": "get_weather", "arguments": {...}},
        #       ]},
        #       {"role": "tool", "parts": [
        #           {"type": "tool_call_response", "id": "call_abc123", "response": "72°F and sunny"},
        #       ]},
        #     ]
        role_map: dict[str, str] = {"human": "user", "ai": "assistant", "system": "system", "tool": "tool"}
        system_instructions: list[dict] = []
        conversation: list[dict] = []
        batch: list[BaseMessage]

        for batch in messages:
            msg: BaseMessage
            for msg in batch:
                role: str = role_map.get(msg.type, msg.type)
                parts: list[dict] = []
                content: str | list[str | dict] = msg.content
                tool_call_id: str | None = getattr(msg, "tool_call_id", None)
                if role == "tool" and tool_call_id:
                    parts.append(
                        {
                            "type": "tool_call_response",
                            "id": tool_call_id,
                            "response": str(content) if content else "",
                        }
                    )
                elif content:
                    parts.append({"type": "text", "content": str(content)})
                tool_calls: list[dict] = getattr(msg, "tool_calls", None) or []
                for tc in tool_calls:
                    parts.append(
                        {
                            "type": "tool_call",
                            "id": tc.get("id", ""),
                            "name": tc.get("name", ""),
                            "arguments": tc.get("args", {}),
                        }
                    )
                if role == "system":
                    system_instructions.extend(parts)
                else:
                    conversation.append({"role": role, "parts": parts})
        return system_instructions, conversation

    @staticmethod
    def _format_lc_llm_output(generations: list) -> list[dict]:
        # converts the result of LLM to OTel output messages format
        # https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-output-messages.json
        #
        # example LangChain input based on:
        # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/outputs/llm_result.py#L19
        #
        #   [[ChatGeneration(
        #       message=AIMessage(content="The weather is sunny.", tool_calls=[...]),
        #       generation_info={"finish_reason": "end_turn"},
        #   )]]
        #
        # example OTel output:
        #
        #   [{"role": "assistant", "parts": [
        #       {"type": "text", "content": "The weather is sunny."},
        #       {"type": "tool_call", "id": "call_abc", "name": "get_weather", "arguments": {...}},
        #   ], "finish_reason": "stop"}]
        finish_reason_map: dict[str, str] = {
            "stop": "stop",
            "end_turn": "stop",
            "tool_use": "tool_call",
            "max_tokens": "length",
            "length": "length",
            "content_filter": "content_filter",
        }
        formatted: list[dict] = []
        for batch in generations:
            for gen in batch:
                msg: BaseMessage | None = getattr(gen, "message", None)
                if msg is not None:
                    _, msgs = OpenTelemetryCallbackHandler._format_lc_messages([[msg]])
                    # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/outputs/chat_generation.py#L28
                    # https://github.com/langchain-ai/langchain/blob/7a4cc3ec321c4ded3681b57456df365936139333/libs/core/langchain_core/messages/ai.py#L195
                    raw_reason: str | None = (getattr(gen, "generation_info", None) or {}).get("finish_reason") or (
                        getattr(msg, "response_metadata", None) or {}
                    ).get("stop_reason")
                    finish_reason: str = finish_reason_map.get(raw_reason, raw_reason) if raw_reason else "stop"
                    for msg_dict in msgs:
                        msg_dict["finish_reason"] = finish_reason
                    formatted.extend(msgs)
                else:
                    text: str = getattr(gen, "text", "")
                    if text:
                        formatted.append(
                            {
                                "role": "assistant",
                                "parts": [{"type": "text", "content": text}],
                                "finish_reason": "stop",
                            }
                        )
        return formatted

    @staticmethod
    def _set_langgraph_span_attributes(span: Span, metadata: Optional[dict]) -> None:
        if not metadata:
            return
        OpenTelemetryCallbackHandler._set_span_attribute(span, LANGGRAPH_STEP_SPAN_ATTR, metadata.get("langgraph_step"))
        OpenTelemetryCallbackHandler._set_span_attribute(span, LANGGRAPH_NODE_SPAN_ATTR, metadata.get("langgraph_node"))

    def _set_llm_request_span_attributes(
        self, span: Span, kwargs: dict, serialized: Optional[dict] = None, model_name: Optional[str] = None
    ):
        config = serialized or {}
        model = model_name
        if not model:
            for model_tag in ("model", "model_name", "model_id", "base_model_id"):
                if (model := kwargs.get(model_tag)) is not None:
                    break
                if (model := (kwargs.get("invocation_params") or {}).get(model_tag)) is not None:
                    break
                if (model := config.get(model_tag)) is not None:
                    break
        if model:
            self._set_span_attribute(span, GEN_AI_REQUEST_MODEL, model)
            self._set_span_attribute(span, GEN_AI_RESPONSE_MODEL, model)

        params: dict[str, Any] = (
            kwargs.get("invocation_params", {}).get("params") or kwargs.get("invocation_params") or kwargs
        )
        self._set_span_attribute(
            span,
            GEN_AI_REQUEST_MAX_TOKENS,
            params.get("max_tokens") or params.get("max_new_tokens") or config.get("max_tokens"),
        )
        self._set_span_attribute(
            span, GEN_AI_REQUEST_TEMPERATURE, params.get("temperature") or config.get("temperature")
        )
        self._set_span_attribute(span, GEN_AI_REQUEST_TOP_P, params.get("top_p") or config.get("top_p"))

    def _should_skip_chain(
        self, serialized: dict[str, Any], name: Optional[str], metadata: Optional[dict] = None
    ) -> bool:
        if not self.should_suppress_internal_chains:
            return False

        # on_chain_start/end callbacks will contain internal chain types showing the
        # internal agent orchestration workflow which can cause a lot of noisy spans
        # except for chains with "AgentExecutor or LangGraph" in the name as
        # those are used for invoke_agent spans:
        # - "runnable": internal orchestration, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/runnables/base.py
        # - "prompts": string formatting, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/prompts
        # - "output_parser": text parsing, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/output_parsers
        skippable_namespaces = {"runnable", "prompts", "output_parser"}

        # legacy agent for supporting classic langchain >= 0.3.21, < 1.0.0
        if name and (name.startswith("Runnable") or name.endswith("OutputParser") or name.endswith("PromptTemplate")):
            return "AgentExecutor" not in name

        if serialized and (ids := serialized.get("id")):
            if any(ns in ids for ns in skippable_namespaces):
                return True

        # In langchain >= 1.0.0, the agent creation logic changed to depend on langgraph.
        # We suppress internal nodes that have langgraph metadata, except for nodes that
        # contain the agent name metadata as those are used for invoke_agent spans.
        if metadata and any(k.startswith("langgraph_") for k in metadata):
            is_agent = "lc_agent_name" in metadata or (name and ("LangGraph" == name or "AgentExecutor" in name))
            return not is_agent
        return False

    def _handle_error(self, error: BaseException, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        if run_id not in self.run_id_to_span_map:
            return
        span, _ = self.run_id_to_span_map[run_id]
        span.set_status(Status(StatusCode.ERROR, str(error)))
        span.set_attribute(ERROR_TYPE, type(error).__qualname__)
        self._end_span(run_id)

    def _start_span(
        self,
        run_id: UUID,
        parent_run_id: Optional[UUID],
        span_name: str,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> Span:
        if parent_run_id and parent_run_id in self.run_id_to_span_map:
            parent_span, _ = self.run_id_to_span_map[parent_run_id]
            span = self.tracer.start_span(span_name, context=set_span_in_context(parent_span), kind=kind)
        else:
            span = self.tracer.start_span(span_name, kind=kind)

        token = context.attach(set_span_in_context(span))
        self.run_id_to_span_map[run_id] = (span, token)
        return span

    def _end_span(self, run_id: UUID) -> None:
        if run_id not in self.run_id_to_span_map:
            return
        span, token = self.run_id_to_span_map.pop(run_id)
        try_detach(token)
        span.end()

    @staticmethod
    def _get_name_from_callback(serialized: dict[str, Any], **kwargs: Any) -> Optional[str]:
        if serialized:
            if name := serialized.get("kwargs", {}).get("name"):
                return name
            if name := serialized.get("name"):
                return name
            if ids := serialized.get("id"):
                return ids[-1]
        return kwargs.get("name")

    @staticmethod
    def _extract_llm_provider(serialized: dict[str, Any], kwargs: dict[str, Any]) -> Optional[str]:
        inv_type = kwargs.get("invocation_params", {}).get("_type", "")
        if inv_type:
            prefix = inv_type.split("-")[0].lower()
            if provider := PROVIDER_MAP.get(prefix):
                return provider

        model = kwargs.get("invocation_params", {}).get("model_id") or kwargs.get("model_id")
        if model and "/" in model:
            prefix = model.split("/")[0].lower()
            if provider := PROVIDER_MAP.get(prefix):
                return provider

        if ids := (serialized or {}).get("id", []):
            for part in ids:
                if provider := PROVIDER_MAP.get(part.lower()):
                    return provider

        return None

    @staticmethod
    def _set_span_attribute(span: Span, name: str, value: Optional[AttributeValue]):
        if value is not None and value != "":
            span.set_attribute(name, value)
