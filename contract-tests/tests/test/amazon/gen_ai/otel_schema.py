# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import urllib.request
from typing import Any

import jsonschema

_OTEL_GEN_AI_SCHEMA_COMMIT = "67dff024110be5bd9f318006e733f4078e0f4c97"
_OTEL_GEN_AI_SCHEMA_BASE = (
    "https://raw.githubusercontent.com/open-telemetry/semantic-conventions-genai/"
    f"{_OTEL_GEN_AI_SCHEMA_COMMIT}/model/gen-ai"
)
_SCHEMA_CACHE: dict = {}


def validate_otel_genai_schema(data: Any, schema_name: str) -> None:
    schema_url = f"{_OTEL_GEN_AI_SCHEMA_BASE}/{schema_name}.json"
    if schema_url not in _SCHEMA_CACHE:
        with urllib.request.urlopen(schema_url) as response:
            _SCHEMA_CACHE[schema_url] = json.loads(response.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_url])
