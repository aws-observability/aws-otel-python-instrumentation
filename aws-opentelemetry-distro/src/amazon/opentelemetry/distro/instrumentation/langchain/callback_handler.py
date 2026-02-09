# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

from contextvars import Token
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

from amazon.opentelemetry.distro.instrumentation.common.utils import PROVIDER_MAP, serialize_to_json
from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
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


@dataclass
class SpanHolder:
    span: Span
    token: Token
    children: list[UUID]
    parent_run_id: Optional[UUID] = None


class _BaseCallbackManagerInitWrapper:
    """Wrapper for BaseCallbackManager.__init__ to inject OpenTelemetry callback handler."""

    def __init__(self, callback_handler: "OpenTelemetryCallbackHandler"):
        self.callback_handler = callback_handler

    def __call__(self, wrapped, instance, args, kwargs) -> None:
        wrapped(*args, **kwargs)
        for handler in instance.inheritable_handlers:
            if isinstance(handler, OpenTelemetryCallbackHandler):
                return
        instance.add_handler(self.callback_handler, True)


class OpenTelemetryCallbackHandler(BaseCallbackHandler):
    def __init__(self, tracer, should_suppress_internal_chains: bool = True):
        super().__init__()
        self.tracer = tracer
        self.should_suppress_internal_chains = should_suppress_internal_chains
        self.span_mapping: dict[UUID, SpanHolder] = {}
        self.skipped_runs: dict[UUID, Optional[UUID]] = {}

    @staticmethod
    def _set_span_attribute(span: Span, name: str, value: Optional[AttributeValue]):
        if value is not None and value != "":
            span.set_attribute(name, value)

    def _set_request_params(self, span: Span, kwargs: dict):
        model = None
        for model_tag in ("model", "model_name", "model_id", "base_model_id"):
            if (model := kwargs.get(model_tag)) is not None:
                break
            if (model := (kwargs.get("invocation_params") or {}).get(model_tag)) is not None:
                break

        if model:
            self._set_span_attribute(span, GEN_AI_REQUEST_MODEL, model)
            self._set_span_attribute(span, GEN_AI_RESPONSE_MODEL, model)

        params: dict[str, Any] = (
            kwargs.get("invocation_params", {}).get("params") or kwargs.get("invocation_params") or kwargs
        )
        self._set_span_attribute(
            span, GEN_AI_REQUEST_MAX_TOKENS, params.get("max_tokens") or params.get("max_new_tokens")
        )
        self._set_span_attribute(span, GEN_AI_REQUEST_TEMPERATURE, params.get("temperature"))
        self._set_span_attribute(span, GEN_AI_REQUEST_TOP_P, params.get("top_p"))

    # map of skipped chain's id to its actual parent span. When a chain is skipped, its children
    # still need to find the correct parent span, so we store the already-resolved parent here.
    def _resolve_parent_span(self, parent_run_id: Optional[UUID]) -> Optional[UUID]:
        if parent_run_id and parent_run_id in self.skipped_runs:
            return self.skipped_runs[parent_run_id]
        if parent_run_id and parent_run_id in self.span_mapping:
            return parent_run_id
        return None

    def _should_skip_chain(
        self, serialized: dict[str, Any], name: Optional[str], metadata: Optional[dict] = None
    ) -> bool:
        if not self.should_suppress_internal_chains:
            return False

        # on_chain_start/end callbacks will contain internal chain types showing the
        # internal agent orchestration workflow which can cause a lot of noisy spans
        # except for chains with "AgentExecutor" in the name as those are used for invoke_agent spans:
        # - "runnable": internal orchestration, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/runnables/base.py
        # - "prompts": string formatting, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/prompts
        # - "output_parser": text parsing, see:
        #   https://github.com/langchain-ai/langchain/blob/80e09feec/libs/core/langchain_core/output_parsers
        skippable_namespaces = {"runnable", "prompts", "output_parser"}

        # legacy agent for supporting langchain >= 0.3.21, < 1.0.0
        if name and (name.startswith("Runnable") or name.endswith("OutputParser") or name.endswith("PromptTemplate")):
            return "AgentExecutor" not in name

        # fallback to namespace as another check
        if serialized and (ids := serialized.get("id")):
            if any(ns in ids for ns in skippable_namespaces):
                return True

        # In langchain >= 1.0.0, the agent creation logic changed to depend on langgraph.
        # We suppress internal nodes that have langgraph metadata, except for nodes with
        # "agent" in the name as those are used for invoke_agent spans.
        if metadata and any(k.startswith("langgraph_") for k in metadata):
            return not (name and "agent" in name.lower())
        return False

    def _end_span(self, run_id: UUID) -> None:
        state = self.span_mapping.get(run_id)
        if not state:
            return
        for child_id in state.children:
            if child_state := self.span_mapping.get(child_id):
                context.detach(child_state.token)
                child_state.span.end()
                del self.span_mapping[child_id]
        context.detach(state.token)
        state.span.end()
        del self.span_mapping[run_id]

    def _create_span(
        self,
        run_id: UUID,
        parent_run_id: Optional[UUID],
        span_name: str,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> Span:
        parent = self._resolve_parent_span(parent_run_id)

        if parent and parent in self.span_mapping:
            parent_ctx = set_span_in_context(self.span_mapping[parent].span)
            span = self.tracer.start_span(span_name, context=parent_ctx, kind=kind)
            self.span_mapping[parent].children.append(run_id)
        else:
            span = self.tracer.start_span(span_name, kind=kind)

        token = context.attach(set_span_in_context(span))
        self.span_mapping[run_id] = SpanHolder(span, token, [], parent)
        return span

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

    def _handle_error(self, error: BaseException, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        if run_id not in self.span_mapping:
            return
        span = self.span_mapping[run_id].span
        span.set_status(Status(StatusCode.ERROR, str(error)))
        span.set_attribute(ERROR_TYPE, type(error).__qualname__)
        self._end_span(run_id)

    def _find_ancestor_agent_span(self, run_id: Optional[UUID]) -> Optional[Span]:
        # OTel semantic conventions recommend gen_ai.request.model, gen_ai.request.temperature, and
        # gen_ai.provider.name on invoke_agent spans. However on_chain_start callback doesn't have access to the underlying
        # LLM configuration. We propagate these attributes when the child LLM span callback fires and
        # set these attributes on the parent agent span since it is still open during the callback.
        # We have to walk up the span hierarchy to find the nearest invoke_agent ancestor.
        # In practice, the depth is typically 1-2 levels, so this is not a costly operation.
        visited = set()
        current = self._resolve_parent_span(run_id)
        while current and current not in visited:
            visited.add(current)
            if current not in self.span_mapping:
                break
            holder = self.span_mapping[current]
            if "invoke_agent" in holder.span.name:
                return holder.span
            current = holder.parent_run_id
        return None

    def _propagate_to_parent_agent_span(self, parent_run_id: Optional[UUID], attrs: dict[str, Any]) -> None:
        agent_span = self._find_ancestor_agent_span(parent_run_id)
        if not agent_span:
            return
        for name, value in attrs.items():
            self._set_span_attribute(agent_span, name, value)

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
        model_id = kwargs.get("invocation_params", {}).get("model_id")
        name = model_id or self._get_name_from_callback(serialized, **kwargs)
        span_name = f"{GenAiOperationNameValues.CHAT.value} {name}" if name else GenAiOperationNameValues.CHAT.value
        span = self._create_span(run_id, parent_run_id, span_name)
        provider = self._extract_provider(serialized, kwargs)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        if "kwargs" in serialized:
            self._set_request_params(span, serialized["kwargs"])
        model = (serialized.get("kwargs") or {}).get("model") or (serialized.get("kwargs") or {}).get("model_name")
        temperature = (serialized.get("kwargs") or {}).get("temperature")
        self._propagate_to_parent_agent_span(
            parent_run_id,
            {
                GEN_AI_PROVIDER_NAME: provider,
                GEN_AI_REQUEST_MODEL: model or name,
                GEN_AI_REQUEST_TEMPERATURE: temperature,
            },
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
        model_id = kwargs.get("invocation_params", {}).get("model_id")
        name = model_id or self._get_name_from_callback(serialized, **kwargs)
        span_name = (
            f"{GenAiOperationNameValues.TEXT_COMPLETION.value} {name}"
            if name
            else GenAiOperationNameValues.TEXT_COMPLETION.value
        )
        span = self._create_span(run_id, parent_run_id, span_name)
        provider = self._extract_provider(serialized, kwargs)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, provider)
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.TEXT_COMPLETION.value)
        self._set_request_params(span, kwargs)
        temperature = kwargs.get("invocation_params", {}).get("temperature")
        self._propagate_to_parent_agent_span(
            parent_run_id,
            {GEN_AI_PROVIDER_NAME: provider, GEN_AI_REQUEST_MODEL: name, GEN_AI_REQUEST_TEMPERATURE: temperature},
        )

    def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY) or run_id not in self.span_mapping:
            return
        span = self.span_mapping[run_id].span
        if response.llm_output:
            model = response.llm_output.get("model_name") or response.llm_output.get("model_id")
            if model:
                self._set_span_attribute(span, GEN_AI_RESPONSE_MODEL, model)
            if response.llm_output.get("id"):
                self._set_span_attribute(span, GEN_AI_RESPONSE_ID, response.llm_output["id"])
            usage = response.llm_output.get("token_usage") or response.llm_output.get("usage") or {}
            input_tokens = usage.get("prompt_tokens") or usage.get("input_token_count") or usage.get("input_tokens")
            output_tokens = (
                usage.get("completion_tokens") or usage.get("generated_token_count") or usage.get("output_tokens")
            )
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
        name = self._get_name_from_callback(serialized, **kwargs)
        if self._should_skip_chain(serialized, name, metadata):
            self.skipped_runs[run_id] = self._resolve_parent_span(parent_run_id)
            return

        is_agent = name and "agent" in name.lower()
        if is_agent:
            span_name = (
                f"{GenAiOperationNameValues.INVOKE_AGENT.value} {name}"
                if name
                else GenAiOperationNameValues.INVOKE_AGENT.value
            )
        else:
            span_name = f"chain {name}" if name else "chain"

        span = self._create_span(run_id, parent_run_id, span_name)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, self._extract_provider(serialized, kwargs))

        if is_agent:
            self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            if name:
                self._set_span_attribute(span, GEN_AI_AGENT_NAME, name)
        if metadata and metadata.get("agent_name"):
            self._set_span_attribute(span, GEN_AI_AGENT_NAME, metadata["agent_name"])

    def on_chain_end(self, outputs: dict[str, Any], *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        self.skipped_runs.pop(run_id, None)
        self._end_span(run_id)

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self.skipped_runs.pop(run_id, None)
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
        name = self._get_name_from_callback(serialized, **kwargs)
        span_name = (
            f"{GenAiOperationNameValues.EXECUTE_TOOL.value} {name}"
            if name
            else GenAiOperationNameValues.EXECUTE_TOOL.value
        )
        span = self._create_span(run_id, parent_run_id, span_name)
        self._set_span_attribute(span, GEN_AI_PROVIDER_NAME, self._extract_provider(serialized, kwargs))
        self._set_span_attribute(span, GEN_AI_TOOL_CALL_ARGUMENTS, serialize_to_json(input_str))
        if serialized.get("id"):
            self._set_span_attribute(span, GEN_AI_TOOL_CALL_ID, serialized["id"])
        if serialized.get("description"):
            self._set_span_attribute(span, GEN_AI_TOOL_DESCRIPTION, serialized["description"])
        if name:
            self._set_span_attribute(span, GEN_AI_TOOL_NAME, name)
        self._set_span_attribute(span, GEN_AI_TOOL_TYPE, "function")
        self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        if context.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return
        span = self.span_mapping[run_id].span
        self._set_span_attribute(span, GEN_AI_TOOL_CALL_RESULT, serialize_to_json(output))
        self._end_span(run_id)

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    def on_agent_action(self, action: AgentAction, *, run_id: UUID, **kwargs: Any) -> None:
        if run_id in self.span_mapping:
            span = self.span_mapping[run_id].span
            self._set_span_attribute(
                span, GEN_AI_TOOL_CALL_ARGUMENTS, serialize_to_json(getattr(action, "tool_input", None))
            )
            self._set_span_attribute(span, GEN_AI_TOOL_NAME, getattr(action, "tool", None))
            self._set_span_attribute(span, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)

    def on_agent_finish(self, finish: AgentFinish, *, run_id: UUID, **kwargs: Any) -> None:
        if run_id in self.span_mapping:
            self._set_span_attribute(
                self.span_mapping[run_id].span,
                GEN_AI_TOOL_CALL_RESULT,
                serialize_to_json(finish.return_values.get("output")),
            )

    def on_agent_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._handle_error(error, run_id, **kwargs)

    @staticmethod
    def _extract_provider(serialized: dict[str, Any], kwargs: dict[str, Any]) -> Optional[str]:
        """Extract provider name from serialized data or kwargs."""
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
