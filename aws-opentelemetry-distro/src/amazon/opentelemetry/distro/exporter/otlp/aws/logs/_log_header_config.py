# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from functools import lru_cache
from typing import NamedTuple, Optional

from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_LOGS_HEADERS


class OtlpLogHeaderSetting(NamedTuple):
    log_group: Optional[str]
    log_stream: Optional[str]
    namespace: Optional[str]

    def is_valid(self) -> bool:
        """Return whether both CloudWatch Logs destination headers are present."""
        return self.log_group is not None and self.log_stream is not None


@lru_cache(maxsize=1)
def fetch_otlp_logs_header() -> OtlpLogHeaderSetting:
    """Parse and cache the CloudWatch destination and EMF namespace headers."""
    logs_headers = os.environ.get(OTEL_EXPORTER_OTLP_LOGS_HEADERS)
    if not logs_headers:
        return OtlpLogHeaderSetting(None, None, None)

    log_group = None
    log_stream = None
    namespace = None
    for pair in logs_headers.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            if key == "x-aws-log-group" and value:
                log_group = value
            elif key == "x-aws-log-stream" and value:
                log_stream = value
            elif key == "x-aws-metric-namespace" and value:
                namespace = value
    return OtlpLogHeaderSetting(log_group, log_stream, namespace)
