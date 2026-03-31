# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import urllib.request

import jsonschema

_OTEL_SCHEMA_BASE = "https://opentelemetry.io/docs/specs/semconv"
_SCHEMA_CACHE: dict = {}


def validate_otel_schema(data, schema_url: str) -> None:
    if schema_url not in _SCHEMA_CACHE:
        with urllib.request.urlopen(schema_url) as resp:
            _SCHEMA_CACHE[schema_url] = json.loads(resp.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_url])


def validate_otel_genai_schema(data: list, schema_name: str) -> None:
    validate_otel_schema(data, f"{_OTEL_SCHEMA_BASE}/gen-ai/{schema_name}.json")
