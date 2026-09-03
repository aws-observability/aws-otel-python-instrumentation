# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass, field
from inspect import isawaitable, iscoroutinefunction
from typing import Any, Optional, TypeVar

from openai import NotGiven, Omit
from wrapt import ObjectProxy

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import get_value
from opentelemetry import context as otel_context
from opentelemetry.context import Context

_GEN_AI_CAPTURING_CONTEXT_KEY = otel_context.create_key("aws_otel_openai_agents_capturing_context")
_CapturingContextT = TypeVar("_CapturingContextT", bound="GenAICapturingContext")
_logger = logging.getLogger(__name__)


def set_gen_ai_capturing_context_in_context(
    capture: "GenAICapturingContext",
    context: Optional[Context] = None,
) -> Context:
    ctx = otel_context.set_value(_GEN_AI_CAPTURING_CONTEXT_KEY, capture, context=context)
    return ctx


def get_gen_ai_capturing_context(
    capturing_context_type: type[_CapturingContextT],
    context: Optional[Context] = None,
) -> Optional[_CapturingContextT]:
    capture = otel_context.get_value(_GEN_AI_CAPTURING_CONTEXT_KEY, context)
    return capture if isinstance(capture, capturing_context_type) else None


class _ResponseCapturingStream(ObjectProxy):
    """Preserve the completion stream interface while capturing each response chunk."""

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        chunk = next(self.__wrapped__)
        try:
            GenAICapturingContext.capture_model_response_attributes(chunk)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI stream response attributes", exc_info=True)
        return chunk

    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> Any:
        chunk = await self.__wrapped__.__anext__()
        try:
            GenAICapturingContext.capture_model_response_attributes(chunk)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI stream response attributes", exc_info=True)
        return chunk


class GenAICapturingContext:
    """Capture GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    @classmethod
    def for_model(cls, model_impl: Optional[str] = None) -> "GenAICapturingContext":
        return ModelCapturingContext(model_impl=model_impl)

    @classmethod
    def for_tool(cls) -> "GenAICapturingContext":
        return ToolCapturingContext()

    @classmethod
    def capture_openai_request_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Responses create methods to capture request attributes."""
        try:
            cls.capture_model_request_attributes(instance, kwargs)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI request attributes", exc_info=True)
        return wrapped(*args, **kwargs)

    @classmethod
    def capture_openai_completion_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Chat Completions create methods to capture request and response attributes."""
        current_model_context = get_gen_ai_capturing_context(ModelCapturingContext)
        if current_model_context is None or current_model_context.model_impl == "litellm":
            return wrapped(*args, **kwargs)
        try:
            cls.capture_model_request_attributes(instance, kwargs)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI request attributes", exc_info=True)
        response = wrapped(*args, **kwargs)
        try:
            if isawaitable(response):
                return cls._capture_acompletion_response_attributes(response, kwargs.get("stream"))
            return cls._capture_completion_response_attributes(response, kwargs.get("stream"))
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to prepare GenAI response capture", exc_info=True)
            return response

    @classmethod
    def capture_litellm_completion_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap LiteLLM completion and acompletion to capture request and response attributes."""
        # LiteLLM's acompletion calls completion(..., acompletion=True); capture the request only once.
        if kwargs.get("acompletion") and not iscoroutinefunction(wrapped):
            return wrapped(*args, **kwargs)
        if get_gen_ai_capturing_context(ModelCapturingContext) is None:
            return wrapped(*args, **kwargs)
        try:
            cls.capture_model_request_attributes(instance, kwargs)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI request attributes", exc_info=True)
        response = wrapped(*args, **kwargs)
        try:
            if isawaitable(response):
                return cls._capture_acompletion_response_attributes(response, kwargs.get("stream"))
            return cls._capture_completion_response_attributes(response, kwargs.get("stream"))
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to prepare GenAI response capture", exc_info=True)
            return response

    @classmethod
    def _capture_completion_response_attributes(cls, response: Any, stream: Any) -> Any:
        if stream and (hasattr(response, "__iter__") or hasattr(response, "__aiter__")):
            try:
                return _ResponseCapturingStream(response)
            except Exception:  # pylint: disable=broad-exception-caught
                _logger.debug("Failed to wrap GenAI response stream", exc_info=True)
                return response
        try:
            cls.capture_model_response_attributes(response)
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI response attributes", exc_info=True)
        return response

    @classmethod
    async def _capture_acompletion_response_attributes(cls, response: Any, stream: Any) -> Any:
        return cls._capture_completion_response_attributes(await response, stream)

    @classmethod
    def capture_tool_call_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Agents tool-call helpers to capture the tool name and call ID attributes."""
        try:
            tool_call = kwargs.get("tool_call") or (args[0] if args else None)
            name = get_value(tool_call, "name")
            call_id = get_value(tool_call, "call_id")
            current_tool_context = get_gen_ai_capturing_context(ToolCapturingContext)
            if current_tool_context is not None and (name or call_id):
                current_tool_context.name = name
                current_tool_context.call_id = call_id
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Failed to capture GenAI tool attributes", exc_info=True)
        return wrapped(*args, **kwargs)

    @classmethod
    def capture_model_request_attributes(cls, instance: Any, kwargs: Any) -> None:
        current_model_context = get_gen_ai_capturing_context(ModelCapturingContext)
        if current_model_context is None:
            return
        current_model_context.request_params = {
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
            current_model_context.request_params["base_url"] = str(base_url)

    @classmethod
    def capture_model_response_attributes(cls, response: Any) -> None:
        current_model_context = get_gen_ai_capturing_context(ModelCapturingContext)
        if current_model_context is None:
            return
        response_params: dict[str, Any] = {}
        for key in (
            "finish_reason",
            "finish_reasons",
            "id",
            "incomplete_reason",
            "model",
            "service_tier",
            "status",
            "system_fingerprint",
        ):
            value = get_value(response, key)
            if value is not None:
                response_params[key] = value
        choices = []
        for choice in get_value(response, "choices") or []:
            finish_reason = get_value(choice, "finish_reason")
            if finish_reason is not None:
                choices.append({"finish_reason": finish_reason})
        if choices:
            response_params["choices"] = choices
        incomplete_reason = get_value(get_value(response, "incomplete_details"), "reason")
        if incomplete_reason is not None:
            response_params["incomplete_details"] = {"reason": incomplete_reason}
        usage = get_value(response, "usage")
        if usage is not None:
            usage_params = {
                key: value
                for key in (
                    "cache_creation_input_tokens",
                    "cache_read_input_tokens",
                    "cache_write_input_tokens",
                    "completion_tokens",
                    "input_tokens",
                    "output_tokens",
                    "prompt_tokens",
                    "reasoning_tokens",
                )
                if (value := get_value(usage, key)) is not None
            }
            for key in (
                "completion_tokens_details",
                "input_tokens_details",
                "output_tokens_details",
                "prompt_tokens_details",
            ):
                details = get_value(usage, key)
                detail_params = {
                    detail_key: value
                    for detail_key in ("cached_tokens", "cache_write_tokens", "reasoning_tokens")
                    if (value := get_value(details, detail_key)) is not None
                }
                if detail_params:
                    usage_params[key] = detail_params
            if usage_params:
                response_params["usage"] = usage_params
        if response_params:
            current_model_context.responses.append(response_params)


INVALID_GEN_AI_CAPTURING_CONTEXT = GenAICapturingContext()


@dataclass
class ModelCapturingContext(GenAICapturingContext):
    model_impl: Optional[str] = None
    request_params: dict[str, Any] = field(default_factory=dict)
    responses: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ToolCapturingContext(GenAICapturingContext):
    name: Optional[str] = None
    call_id: Optional[str] = None
