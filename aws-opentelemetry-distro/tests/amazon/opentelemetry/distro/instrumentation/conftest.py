# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import urllib.request

os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")

# The OTel GenAI semantic conventions (and their JSON Schemas) moved out of
# opentelemetry.io into the dedicated open-telemetry/semantic-conventions-genai
# repository; the old opentelemetry.io/docs/specs/semconv/gen-ai/*.json URLs now
# 404. The relocated repo does not yet publish tagged releases, so we pin to the
# raw files on the default branch.
_OTEL_SCHEMA_BASE = "https://raw.githubusercontent.com/open-telemetry/semantic-conventions-genai/main/docs"
_SCHEMA_CACHE: dict = {}


def validate_otel_schema(data, schema_url: str) -> None:
    import jsonschema

    if schema_url not in _SCHEMA_CACHE:
        with urllib.request.urlopen(schema_url) as resp:
            _SCHEMA_CACHE[schema_url] = json.loads(resp.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_url])


def validate_otel_genai_schema(data: list, schema_name: str) -> None:
    validate_otel_schema(data, f"{_OTEL_SCHEMA_BASE}/gen-ai/{schema_name}.json")
