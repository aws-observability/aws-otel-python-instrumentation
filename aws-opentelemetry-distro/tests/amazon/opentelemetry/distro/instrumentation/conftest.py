# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import urllib.request
from typing import Any, Callable

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


def call_mock_llm(
    provider: str, invoke_llm_callback: Callable[[Any], None], **kwargs: Any
) -> None:  # pylint: disable=too-many-locals
    # These intentionally fake model names keep the tests independent of SDK model catalogs.
    config = {
        "openai": {
            "model": "gpt-5.6-sol",
        },
        "anthropic": {
            "model": "claude-fable-5",
        },
        "bedrock": {
            "model": "anthropic.claude-fable-5",
        },
        "litellm": {
            "model": "gpt-5.6-sol",
        },
    }.get(provider)
    if config is None:
        raise ValueError(f"Unsupported LLM provider: {provider}")

    is_async = kwargs.get("is_async", False)
    model = kwargs.get("model", config["model"])

    if provider in ("openai", "litellm"):
        import asyncio

        import httpx
        from openai import AsyncOpenAI, OpenAI

        if provider == "litellm" and is_async:
            raise NotImplementedError("Async LiteLLM mock calls are not supported")

        def openai_response(request):
            return httpx.Response(
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

        transport = httpx.MockTransport(openai_response)
        if is_async:
            http_client = httpx.AsyncClient(transport=transport)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                invoke_llm_callback(AsyncOpenAI(api_key="fake-key", http_client=http_client))
            finally:
                loop.run_until_complete(http_client.aclose())
                asyncio.set_event_loop(None)
                loop.close()
            return

        with httpx.Client(transport=transport) as http_client:
            if provider == "litellm":
                import litellm

                previous_client_session = litellm.client_session
                litellm.client_session = http_client
                try:
                    invoke_llm_callback(None)
                finally:
                    litellm.client_session = previous_client_session
            else:
                invoke_llm_callback(OpenAI(api_key="fake-key", http_client=http_client))
        return

    if provider == "anthropic":
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
        with DefaultHttpxClient(transport=transport) as http_client:
            invoke_llm_callback(Anthropic(api_key="fake-key", http_client=http_client))
        return

    if provider == "bedrock":
        if kwargs.get("is_litellm"):
            import asyncio
            from unittest.mock import patch

            import httpx
            from litellm.llms.custom_httpx.http_handler import AsyncHTTPHandler

            response = {
                "output": {"message": {"role": "assistant", "content": [{"text": "Hello, World!"}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
                "metrics": {"latencyMs": 1},
            }

            def bedrock_response(request):
                return httpx.Response(
                    200,
                    request=request,
                    headers={"x-amzn-requestid": "bedrock-request-id"},
                    json=response,
                )

            mock_http_client = httpx.AsyncClient(transport=httpx.MockTransport(bedrock_response))
            http_handler = AsyncHTTPHandler()
            owned_http_client = http_handler.client
            http_handler.client = mock_http_client

            async def close_clients():
                await owned_http_client.aclose()
                await mock_http_client.aclose()

            try:
                with patch(
                    "litellm.llms.bedrock.chat.converse_handler.get_async_httpx_client",
                    return_value=http_handler,
                ):
                    invoke_llm_callback(None)
            finally:
                asyncio.run(close_clients())
            return

        from botocore.session import get_session
        from botocore.stub import Stubber

        client = get_session().create_client(
            "bedrock-runtime",
            region_name="us-east-1",
            aws_access_key_id="fake-key",
            aws_secret_access_key="fake-key",
        )
        response = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Hello, World!"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
            "metrics": {"latencyMs": 1},
        }
        with Stubber(client) as stubber:
            stubber.add_response("converse", response)
            invoke_llm_callback(client)
