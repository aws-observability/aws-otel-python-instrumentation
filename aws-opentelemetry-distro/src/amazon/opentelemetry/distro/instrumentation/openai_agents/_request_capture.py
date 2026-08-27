# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from typing import Any, Optional

_CURRENT_REQUEST_PARAMS: ContextVar[Optional[dict[str, Any]]] = ContextVar(
    "aws_otel_openai_agents_current_request_params", default=None
)


def record_request(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
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
    _CURRENT_REQUEST_PARAMS.set(params)
    return wrapped(*args, **kwargs)


def get_request_params() -> dict[str, Any]:
    return _CURRENT_REQUEST_PARAMS.get() or {}


def reset_request_params() -> None:
    _CURRENT_REQUEST_PARAMS.set(None)
