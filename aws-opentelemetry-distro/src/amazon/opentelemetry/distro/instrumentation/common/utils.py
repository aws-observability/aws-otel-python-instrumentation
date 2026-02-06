# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Any

from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import GenAiProviderNameValues

PROVIDER_MAP = {
    "bedrock": GenAiProviderNameValues.AWS_BEDROCK.value,
    "aws": GenAiProviderNameValues.AWS_BEDROCK.value,
    "openai": GenAiProviderNameValues.OPENAI.value,
    "anthropic": GenAiProviderNameValues.ANTHROPIC.value,
    "claude": GenAiProviderNameValues.ANTHROPIC.value,
    "azure": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "azure_openai": GenAiProviderNameValues.AZURE_AI_OPENAI.value,
    "google": GenAiProviderNameValues.GCP_VERTEX_AI.value,
    "gemini": GenAiProviderNameValues.GCP_GEMINI.value,
    "cohere": GenAiProviderNameValues.COHERE.value,
    "mistral": GenAiProviderNameValues.MISTRAL_AI.value,
    "groq": GenAiProviderNameValues.GROQ.value,
    "deepseek": GenAiProviderNameValues.DEEPSEEK.value,
    "perplexity": GenAiProviderNameValues.PERPLEXITY.value,
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
