# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Final


class _SpanMetrics:
    SCOPE_NAME: Final = "cloudwatch.plugin.otel.span-metrics"

    CALLS_NAME: Final = "traces.span.metrics.calls"
    DURATION_NAME: Final = "traces.span.metrics.duration"
    DURATION_UNIT: Final = "s"
    DURATION_BUCKET_BOUNDARIES: Final = [
        0.002,
        0.004,
        0.006,
        0.008,
        0.01,
        0.05,
        0.1,
        0.2,
        0.4,
        0.8,
        1.0,
        1.4,
        2.0,
        5.0,
        10.0,
        15.0,
    ]
    NANOS_PER_SECOND: Final = 1_000_000_000.0

    SPAN_NAME: Final = "span.name"
    SPAN_KIND: Final = "span.kind"
    STATUS_CODE: Final = "status.code"
    SCHEMA: Final = "aws.otel.span.metrics.schema"
    SCHEMA_VERSION: Final = "v1"
    LIB_VERSION: Final = "aws.otel.extension.lib.version"
