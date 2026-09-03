# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from dataclasses import dataclass, field
from inspect import isawaitable, iscoroutinefunction
from typing import Any, Optional

from openai import NotGiven, Omit
from wrapt import ObjectProxy

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import get_value


@dataclass
class ModelCapturingContext:
    model_impl: Optional[str] = None
    request_params: dict[str, Any] = field(default_factory=dict)
    responses: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ToolCapturingContext:
    name: Optional[str] = None
    call_id: Optional[str] = None


class _ResponseCapturingStream(ObjectProxy):
    """Preserve the completion stream interface while capturing each response chunk."""

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        chunk = next(self.__wrapped__)
        GenAIContextCapture.capture_model_response_attributes(chunk)
        return chunk

    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> Any:
        chunk = await self.__wrapped__.__anext__()
        GenAIContextCapture.capture_model_response_attributes(chunk)
        return chunk


class GenAIContextCapture:
    """Capture GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    _current_model_context: ContextVar[Optional[ModelCapturingContext]] = ContextVar(
        "aws_otel_openai_agents_current_model_context", default=None
    )
    _current_tool_context: ContextVar[Optional[ToolCapturingContext]] = ContextVar(
        "aws_otel_openai_agents_current_tool_context", default=None
    )

    @classmethod
    def capture_openai_request_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Responses create methods to capture request attributes."""
        cls.capture_model_request_attributes(instance, kwargs)
        return wrapped(*args, **kwargs)

    @classmethod
    def capture_openai_completion_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Chat Completions create methods to capture request and response attributes."""
        current_model_context = cls.get_current_model_context()
        if current_model_context is None or current_model_context.model_impl == "litellm":
            return wrapped(*args, **kwargs)
        cls.capture_model_request_attributes(instance, kwargs)
        response = wrapped(*args, **kwargs)
        if isawaitable(response):
            return cls._capture_acompletion_response_attributes(response, kwargs.get("stream"))
        return cls._capture_completion_response_attributes(response, kwargs.get("stream"))

    @classmethod
    def capture_litellm_completion_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap LiteLLM completion and acompletion to capture request and response attributes."""
        # LiteLLM's acompletion calls completion(..., acompletion=True); capture the request only once.
        if kwargs.get("acompletion") and not iscoroutinefunction(wrapped):
            return wrapped(*args, **kwargs)
        if cls.get_current_model_context() is None:
            return wrapped(*args, **kwargs)
        cls.capture_model_request_attributes(instance, kwargs)
        response = wrapped(*args, **kwargs)
        if isawaitable(response):
            return cls._capture_acompletion_response_attributes(response, kwargs.get("stream"))
        return cls._capture_completion_response_attributes(response, kwargs.get("stream"))

    @classmethod
    def _capture_completion_response_attributes(cls, response: Any, stream: Any) -> Any:
        if stream and (hasattr(response, "__iter__") or hasattr(response, "__aiter__")):
            return _ResponseCapturingStream(response)
        cls.capture_model_response_attributes(response)
        return response

    @classmethod
    async def _capture_acompletion_response_attributes(cls, response: Any, stream: Any) -> Any:
        return cls._capture_completion_response_attributes(await response, stream)

    @classmethod
    def capture_tool_call_attributes(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap OpenAI Agents tool-call helpers to capture the tool name and call ID attributes."""
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = get_value(tool_call, "name")
        call_id = get_value(tool_call, "call_id")
        current_tool_context = cls.get_current_tool_context()
        if current_tool_context is not None and (name or call_id):
            current_tool_context.name = name
            current_tool_context.call_id = call_id
        return wrapped(*args, **kwargs)

    @classmethod
    def capture_model_request_attributes(cls, instance: Any, kwargs: Any) -> None:
        current_model_context = cls.get_current_model_context()
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
        current_model_context = cls.get_current_model_context()
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

    @classmethod
    def reset_current_model_context(cls, model_impl: Optional[str] = None) -> None:
        cls._current_model_context.set(ModelCapturingContext(model_impl=model_impl))

    @classmethod
    def get_current_model_context(cls) -> Optional[ModelCapturingContext]:
        return cls._current_model_context.get()

    @classmethod
    def reset_current_tool_context(cls) -> None:
        cls._current_tool_context.set(ToolCapturingContext())

    @classmethod
    def get_current_tool_context(cls) -> Optional[ToolCapturingContext]:
        return cls._current_tool_context.get()
