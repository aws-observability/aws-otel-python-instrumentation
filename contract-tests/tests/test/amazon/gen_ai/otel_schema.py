# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import urllib.request
from typing import Any

import jsonschema

# TODO: Update this version and schema revision when ADOT's OTel dependency versions are bumped.
# Keep these schema constants in sync with
# aws-opentelemetry-distro/tests/amazon/opentelemetry/distro/instrumentation/conftest.py.
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


def validate_otel_genai_schema(data: Any, schema_name: str) -> None:
    schema_url = f"{_OTEL_GEN_AI_SCHEMA_BASE}/{schema_name}.json"
    if schema_url not in _SCHEMA_CACHE:
        with urllib.request.urlopen(schema_url, timeout=_SCHEMA_FETCH_TIMEOUT_SECONDS) as response:
            _SCHEMA_CACHE[schema_url] = json.loads(response.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_url])
