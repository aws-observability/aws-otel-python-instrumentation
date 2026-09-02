# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class _ModelInvocation:
    request_params: Optional[dict[str, Any]] = None
    response: Any = None


@dataclass
class _ToolCall:
    name: Optional[str] = None
    call_id: Optional[str] = None


class GenAIContextCapture:
    """Capture GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    _model_invocation: ContextVar[Optional[_ModelInvocation]] = ContextVar(
        "aws_otel_openai_agents_model_invocation", default=None
    )
    _tool_call: ContextVar[Optional[_ToolCall]] = ContextVar("aws_otel_openai_agents_tool_call", default=None)

    @classmethod
    def record_request(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        cls._capture_model_request_params(instance, kwargs)
        return wrapped(*args, **kwargs)

    @classmethod
    async def record_litellm_invocation(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        cls._capture_model_request_params(instance, kwargs)
        response = await wrapped(*args, **kwargs)
        cls._capture_litellm_response(response)
        return response

    @classmethod
    def _capture_litellm_response(cls, response: Any) -> None:
        invocation = cls._model_invocation.get() or _ModelInvocation()
        cls._model_invocation.set(_ModelInvocation(request_params=invocation.request_params, response=response))

    @classmethod
    def _capture_model_request_params(cls, instance: Any, kwargs: Any) -> None:
        params = {
            key: value
            for key, value in kwargs.items()
            if value is not None
            and key
            in {
                "base_url",
                "choice_count",
                "frequency_penalty",
                "max_completion_tokens",
                "max_output_tokens",
                "max_tokens",
                "model",
                "n",
                "parallel_tool_calls",
                "presence_penalty",
                "reasoning",
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
        base_url = getattr(getattr(instance, "_client", None), "base_url", None)
        if base_url is not None:
            params["base_url"] = str(base_url)
        cls._model_invocation.set(_ModelInvocation(request_params=params))

    @classmethod
    def record_tool_call(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = getattr(tool_call, "name", None)
        call_id = getattr(tool_call, "call_id", None)
        if name or call_id:
            cls._tool_call.set(_ToolCall(name=name, call_id=call_id))
        return wrapped(*args, **kwargs)

    @classmethod
    def get_model_invocation(cls) -> _ModelInvocation:
        return cls._model_invocation.get() or _ModelInvocation()

    @classmethod
    def get_tool_call(cls) -> _ToolCall:
        return cls._tool_call.get() or _ToolCall()

    @classmethod
    def reset_model_invocation(cls) -> None:
        cls._model_invocation.set(None)

    @classmethod
    def reset_tool_call(cls) -> None:
        cls._tool_call.set(None)
