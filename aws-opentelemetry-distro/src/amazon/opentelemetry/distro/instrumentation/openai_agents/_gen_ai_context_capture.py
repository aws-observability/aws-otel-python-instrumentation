# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class _ToolCall:
    name: Optional[str] = None
    call_id: Optional[str] = None


class GenAIContextCapture:
    """Capture GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    _request_params: ContextVar[Optional[dict[str, Any]]] = ContextVar(
        "aws_otel_openai_agents_request_params", default=None
    )
    _tool_call: ContextVar[Optional[_ToolCall]] = ContextVar("aws_otel_openai_agents_tool_call", default=None)

    @classmethod
    def record_request(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        params = {
            key: value
            for key, value in kwargs.items()
            if value is not None
            and key
            in {
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
        cls._request_params.set(params)
        return wrapped(*args, **kwargs)

    @classmethod
    def record_tool_call(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = getattr(tool_call, "name", None)
        call_id = getattr(tool_call, "call_id", None)
        if name or call_id:
            cls._tool_call.set(_ToolCall(name=name, call_id=call_id))
        return wrapped(*args, **kwargs)

    @classmethod
    def get_request_params(cls) -> dict[str, Any]:
        return cls._request_params.get() or {}

    @classmethod
    def get_tool_call(cls) -> _ToolCall:
        return cls._tool_call.get() or _ToolCall()

    @classmethod
    def reset_request_params(cls) -> None:
        cls._request_params.set(None)

    @classmethod
    def reset_tool_call(cls) -> None:
        cls._tool_call.set(None)
