# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import urllib.request
from typing import Any

os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")

# TODO: Update this version and schema revision when ADOT's OTel dependency versions are bumped.
# Keep these schema constants in sync with contract-tests/tests/test/amazon/gen_ai/otel_schema.py.
_OTEL_SEMCONV_VERSION = "v1.43.0"
# semantic-conventions-genai does not publish version tags. This revision's manifest declares the v1.43.0 dependency
# used by opentelemetry-semantic-conventions 0.65b0.
_OTEL_GEN_AI_SCHEMA_REVISION = "647791f1ad23fd7c427dce4a984b3efee40961fc"
_OTEL_GEN_AI_SCHEMA_BASE = (
    "https://raw.githubusercontent.com/open-telemetry/semantic-conventions-genai/"
    f"{_OTEL_GEN_AI_SCHEMA_REVISION}/model/gen-ai"
)
_SCHEMA_FETCH_TIMEOUT_SECONDS = 10
_SCHEMA_CACHE: dict = {}


def validate_otel_schema(data, schema_url: str) -> None:
    import jsonschema

    if schema_url not in _SCHEMA_CACHE:
        with urllib.request.urlopen(schema_url, timeout=_SCHEMA_FETCH_TIMEOUT_SECONDS) as resp:
            _SCHEMA_CACHE[schema_url] = json.loads(resp.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_url])


def validate_otel_genai_schema(data: list, schema_name: str) -> None:
    validate_otel_schema(data, f"{_OTEL_GEN_AI_SCHEMA_BASE}/{schema_name}.json")


def call_mock_llm(provider: str, **kwargs: Any) -> None:  # pylint: disable=too-many-locals
    config = {
        "openai": {
            "model": "gpt-5.6-sol",
            "temperature": 1.0,
            "top_k": None,
            "max_tokens": 100,
        },
        "anthropic": {
            "model": "claude-fable-5",
            "temperature": 1.0,
            "top_k": 250,
            "max_tokens": 100,
        },
        "bedrock": {
            "model": "anthropic.claude-fable-5",
            "temperature": 0.7,
            "top_k": 250,
            "max_tokens": 100,
        },
    }.get(provider)
    if config is None:
        raise ValueError(f"Unsupported LLM provider: {provider}")

    model = kwargs.get("model", config["model"])
    temperature = kwargs.get("temperature", config["temperature"])
    top_k = kwargs.get("top_k", config["top_k"])
    max_tokens = kwargs.get("max_tokens", config["max_tokens"])

    if provider == "openai":
        import httpx
        from openai import OpenAI

        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                request=request,
                json={
                    "id": "chatcmpl-mock",
                    "object": "chat.completion",
                    "created": 1234567890,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "Hello, World!"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
                },
            )
        )
        with httpx.Client(transport=transport) as http_client:
            OpenAI(api_key="fake-key", http_client=http_client).chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello"}],
                temperature=temperature,
            )
        return

    if provider == "anthropic":
        from inspect import signature

        from anthropic import Anthropic, DefaultHttpxClient, _base_client

        anthropic_httpx = getattr(_base_client, "httpx2", None) or _base_client.httpx
        transport = anthropic_httpx.MockTransport(
            lambda request: anthropic_httpx.Response(
                200,
                request=request,
                json={
                    "id": "msg_mock",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [{"type": "text", "text": "Hello, World!"}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 20},
                },
            )
        )
        request = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": "Hello"}],
        }
        with DefaultHttpxClient(transport=transport) as http_client:
            client = Anthropic(api_key="fake-key", http_client=http_client)
            supported_parameters = signature(client.messages.create).parameters
            if "temperature" in supported_parameters:
                request["temperature"] = temperature
            if top_k is not None and "top_k" in supported_parameters:
                request["top_k"] = top_k
            client.messages.create(**request)
        return

    if provider == "bedrock":
        from botocore.session import get_session
        from botocore.stub import Stubber

        client = get_session().create_client(
            "bedrock-runtime",
            region_name="us-east-1",
            aws_access_key_id="fake-key",
            aws_secret_access_key="fake-key",
        )
        request = {
            "modelId": model,
            "messages": [{"role": "user", "content": [{"text": "Hello"}]}],
            "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
        }
        if top_k is not None:
            request["additionalModelRequestFields"] = {"top_k": top_k}
        response = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Hello, World!"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
            "metrics": {"latencyMs": 1},
        }
        with Stubber(client) as stubber:
            stubber.add_response("converse", response, request)
            client.converse(**request)
