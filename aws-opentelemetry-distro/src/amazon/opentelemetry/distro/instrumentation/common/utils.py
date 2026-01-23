# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Any

# see: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
# under gen_ai.provider.name
PROVIDER_MAP = {
    "bedrock": "aws.bedrock",
    "aws": "aws.bedrock",
    "openai": "openai",
    "anthropic": "anthropic",
    "claude": "anthropic",
    "azure": "azure.ai.openai",
    "azure_openai": "azure.ai.openai",
    "google": "gcp.vertex_ai",
    "gemini": "gcp.gemini",
    "cohere": "cohere",
    "mistral": "mistral_ai",
    "groq": "groq",
    "deepseek": "deepseek",
    "perplexity": "perplexity",
}


def serialize_to_json(value: Any, max_depth: int = 3) -> str:
    """Serialize a value to JSON string with depth truncation."""

    def _truncate(obj: Any, depth: int) -> Any:
        if depth <= 0:
            return "..."
        if isinstance(obj, dict):
            return {k: _truncate(v, depth - 1) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_truncate(item, depth - 1) for item in obj]
        return obj

    try:
        return json.dumps(_truncate(value, max_depth))
    except (TypeError, ValueError):
        return str(value)
