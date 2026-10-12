# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import urllib.request
from typing import Any

import jsonschema

# TODO: Update this version and schema revision when ADOT's OTel dependency versions are bumped.
# Keep these schema constants in sync with
# aws-opentelemetry-distro/tests/amazon/opentelemetry/distro/instrumentation/conftest.py.
_OTEL_SEMCONV_VERSION = "v1.44.0"
# semantic-conventions-genai does not publish version tags. This revision's manifest declares the v1.44.0 dependency
# used by opentelemetry-semantic-conventions 0.66b0.
_OTEL_GEN_AI_SCHEMA_REVISION = "b694ec35855d8eccfacd5b09e4b72a808b363038"
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
