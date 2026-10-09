# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

OTEL_EXPORTER_OTLP_SIGV4_SERVICE = "OTEL_EXPORTER_OTLP_SIGV4_SERVICE"
"""
.. envvar:: OTEL_EXPORTER_OTLP_SIGV4_SERVICE

The :envvar:`OTEL_EXPORTER_OTLP_SIGV4_SERVICE` sets the AWS signing service for
the OTLP SigV4 exporters. Signal-specific service variables take higher precedence.
This variable is optional. When unset, the exporters default to ``xray`` for spans,
``monitoring`` for metrics, and ``logs`` for logs.
"""

OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE = "OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE"
"""
.. envvar:: OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE

Same as :envvar:`OTEL_EXPORTER_OTLP_SIGV4_SERVICE` but only for the span
exporter. If both are present, this takes higher precedence.
Default: xray
"""

OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE = "OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE"
"""
.. envvar:: OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE

Same as :envvar:`OTEL_EXPORTER_OTLP_SIGV4_SERVICE` but only for the metrics
exporter. If both are present, this takes higher precedence.
Default: monitoring
"""

OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE = "OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE"
"""
.. envvar:: OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE

Same as :envvar:`OTEL_EXPORTER_OTLP_SIGV4_SERVICE` but only for the log
exporter. If both are present, this takes higher precedence.
Default: logs
"""
