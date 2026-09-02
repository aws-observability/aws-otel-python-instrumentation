# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import first_not_none, get_value


@dataclass
class _ModelInvocation:
    request_params: Optional[dict[str, Any]] = None
    response: Optional[dict[str, Any]] = None


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
        if cls._model_invocation.get() is None:
            return await wrapped(*args, **kwargs)
        cls._capture_model_request_params(instance, kwargs)
        response = await wrapped(*args, **kwargs)
        if kwargs.get("stream") and hasattr(response, "__aiter__"):
            return cls._capture_litellm_stream(response)
        cls._capture_litellm_response(response)
        return response

    @classmethod
    def _capture_litellm_response(cls, response: Any) -> None:
        invocation = cls._model_invocation.get()
        if invocation is None:
            return
        params = invocation.response or {}
        for key in ("id", "model", "service_tier", "system_fingerprint", "status"):
            value = get_value(response, key)
            if value is not None:
                params[key] = value
        incomplete_reason = first_not_none(
            get_value(response, "incomplete_reason"),
            get_value(get_value(response, "incomplete_details"), "reason"),
        )
        if incomplete_reason is not None:
            params["incomplete_reason"] = incomplete_reason
        finish_reasons = [
            reason
            for reason in dict.fromkeys(
                (params.get("finish_reasons") or [])
                + (get_value(response, "finish_reasons") or [])
                + [get_value(response, "finish_reason")]
                + [get_value(choice, "finish_reason") for choice in get_value(response, "choices") or []]
            )
            if reason is not None
        ]
        if finish_reasons:
            params["finish_reasons"] = finish_reasons
        usage = get_value(response, "usage")
        if usage is not None:
            model_dump = getattr(usage, "model_dump", None)
            params["usage"] = model_dump(exclude_none=True) if callable(model_dump) else dict(usage)
        invocation.response = params or None

    @classmethod
    async def _capture_litellm_stream(cls, stream: Any) -> Any:
        try:
            async for chunk in stream:
                cls._capture_litellm_response(chunk)
                yield chunk
        finally:
            close = getattr(stream, "aclose", None)
            if callable(close):
                await close()

    @classmethod
    def _capture_model_request_params(cls, instance: Any, kwargs: Any) -> None:
        invocation = cls._model_invocation.get()
        if invocation is None:
            return
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
        base_url = getattr(getattr(instance, "_client", None), "base_url", None)
        if base_url is not None:
            params["base_url"] = str(base_url)
        invocation.request_params = params

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
    def start_model_invocation(cls) -> None:
        cls._model_invocation.set(_ModelInvocation())

    @classmethod
    def get_tool_call(cls) -> _ToolCall:
        return cls._tool_call.get() or _ToolCall()

    @classmethod
    def reset_model_invocation(cls) -> None:
        cls._model_invocation.set(None)

    @classmethod
    def reset_tool_call(cls) -> None:
        cls._tool_call.set(None)
