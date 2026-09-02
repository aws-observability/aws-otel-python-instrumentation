# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Optional

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import get_value


@dataclass
class _ModelRequestResponse:
    request_params: Optional[dict[str, Any]] = None
    response_params: Optional[dict[str, Any]] = None

    def clear(self) -> None:
        self.request_params = None
        self.response_params = None


@dataclass(frozen=True)
class ModelRequestResponseContext:
    token: Token
    request_response: _ModelRequestResponse


@dataclass
class _ToolCall:
    name: Optional[str] = None
    call_id: Optional[str] = None


class GenAIContextCapture:
    """Capture GenAI call details that the OpenAI Agents tracing callbacks do not expose."""

    _model_request_response: ContextVar[Optional[_ModelRequestResponse]] = ContextVar(
        "aws_otel_openai_agents_model_request_response", default=None
    )
    _tool_call: ContextVar[Optional[_ToolCall]] = ContextVar("aws_otel_openai_agents_tool_call", default=None)

    @classmethod
    def record_request(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        cls._capture_openai_request_params(instance, kwargs)
        return wrapped(*args, **kwargs)

    @classmethod
    async def record_litellm_invocation(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        request_response = cls._model_request_response.get()
        if request_response is None:
            return await wrapped(*args, **kwargs)

        cls._capture_request_params(request_response, instance, kwargs)
        response = await wrapped(*args, **kwargs)
        if kwargs.get("stream") and hasattr(response, "__aiter__"):
            return _CapturedAsyncStream(response, request_response)
        cls._capture_litellm_response_params(request_response, response)
        return response

    @classmethod
    def _capture_litellm_response_params(cls, request_response: _ModelRequestResponse, response: Any) -> None:
        response_params = request_response.response_params or {}
        for key in ("id", "model", "service_tier", "system_fingerprint", "status"):
            value = get_value(response, key)
            if value is not None:
                response_params[key] = value

        incomplete_reason = get_value(response, "incomplete_reason") or get_value(
            get_value(response, "incomplete_details"), "reason"
        )
        if incomplete_reason is not None:
            response_params["incomplete_reason"] = incomplete_reason

        finish_reasons = response_params.setdefault("finish_reasons", [])
        for finish_reason in cls._to_list(get_value(response, "finish_reasons")):
            if finish_reason is not None and finish_reason not in finish_reasons:
                finish_reasons.append(finish_reason)
        for choice in cls._to_list(get_value(response, "choices")):
            finish_reason = get_value(choice, "finish_reason")
            if finish_reason is not None and finish_reason not in finish_reasons:
                finish_reasons.append(finish_reason)
        if not finish_reasons:
            response_params.pop("finish_reasons", None)

        usage = cls._to_mapping(get_value(response, "usage"))
        if usage:
            response_params["usage"] = cls._merge_mappings(response_params.get("usage"), usage)

        if response_params:
            request_response.response_params = response_params

    @classmethod
    def _capture_openai_request_params(cls, instance: Any, kwargs: Any) -> None:
        request_response = cls._model_request_response.get()
        if request_response is not None:
            cls._capture_request_params(request_response, instance, kwargs)

    @staticmethod
    def _capture_request_params(request_response: _ModelRequestResponse, instance: Any, kwargs: Any) -> None:
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
        request_response.request_params = params

    @staticmethod
    def _to_list(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, (str, bytes)):
            return [value]
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

    @classmethod
    def _to_mapping(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Mapping):
            source = value
        else:
            model_dump = getattr(value, "model_dump", None)
            if not callable(model_dump):
                return {}
            source = model_dump(exclude_none=True)
        result = {}
        for key, item in source.items():
            normalized = cls._to_metadata_value(item)
            if normalized is not None:
                result[str(key)] = normalized
        return result

    @classmethod
    def _to_metadata_value(cls, value: Any) -> Any:
        if isinstance(value, (bool, bytes, float, int, str)):
            return value
        if isinstance(value, Mapping) or callable(getattr(value, "model_dump", None)):
            return cls._to_mapping(value)
        if isinstance(value, (list, tuple)):
            return [normalized for item in value if (normalized := cls._to_metadata_value(item)) is not None]
        return None

    @classmethod
    def _merge_mappings(cls, existing: Any, new: Mapping[str, Any]) -> dict[str, Any]:
        merged = dict(existing) if isinstance(existing, Mapping) else {}
        for key, value in new.items():
            if isinstance(value, Mapping):
                merged[key] = cls._merge_mappings(merged.get(key), value)
            else:
                merged[key] = value
        return merged

    @classmethod
    def record_tool_call(cls, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        tool_call = kwargs.get("tool_call") or (args[0] if args else None)
        name = getattr(tool_call, "name", None)
        call_id = getattr(tool_call, "call_id", None)
        if name or call_id:
            cls._tool_call.set(_ToolCall(name=name, call_id=call_id))
        return wrapped(*args, **kwargs)

    @classmethod
    def start_model_request_response(cls) -> ModelRequestResponseContext:
        request_response = _ModelRequestResponse()
        return ModelRequestResponseContext(
            token=cls._model_request_response.set(request_response),
            request_response=request_response,
        )

    @classmethod
    def end_model_request_response(cls, context: ModelRequestResponseContext) -> None:
        context.request_response.clear()
        try:
            cls._model_request_response.reset(context.token)
        except (RuntimeError, ValueError):
            # Shutdown may close an unfinished span from a different async context.
            pass

    @classmethod
    def get_model_request_response(cls) -> _ModelRequestResponse:
        return cls._model_request_response.get() or _ModelRequestResponse()

    @classmethod
    def get_tool_call(cls) -> _ToolCall:
        return cls._tool_call.get() or _ToolCall()

    @classmethod
    def reset_model_request_response(cls) -> None:
        request_response = cls._model_request_response.get()
        if request_response is not None:
            request_response.clear()
        cls._model_request_response.set(None)

    @classmethod
    def reset_tool_call(cls) -> None:
        cls._tool_call.set(None)


class _CapturedAsyncStream:
    """Proxy a LiteLLM stream while retaining only response telemetry metadata."""

    def __init__(self, stream: Any, request_response: _ModelRequestResponse) -> None:
        self._stream = stream
        self._iterator = stream.__aiter__()
        self._request_response = request_response
        self._closed = False

    def __aiter__(self) -> "_CapturedAsyncStream":
        return self

    async def __anext__(self) -> Any:
        try:
            chunk = await self._iterator.__anext__()
        except (StopAsyncIteration, asyncio.CancelledError):
            await self.aclose()
            raise
        GenAIContextCapture._capture_litellm_response_params(self._request_response, chunk)
        return chunk

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._iterator, "aclose", None) or getattr(self._stream, "aclose", None)
        if callable(close):
            result = close()
            if hasattr(result, "__await__"):
                await result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)
