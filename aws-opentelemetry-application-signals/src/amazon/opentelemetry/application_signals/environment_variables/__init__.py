# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

OTEL_AWS_APPLICATION_SIGNALS_ENABLED = "OTEL_AWS_APPLICATION_SIGNALS_ENABLED"
"""
.. envvar:: OTEL_AWS_APPLICATION_SIGNALS_ENABLED

Enables AWS Application Signals. When enabled, the distro will generate
Application Signals metrics from spans and propagate attributes for
service/dependency metric correlation.
Default: "false"
"""

OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED = "OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED"
"""
.. envvar:: OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED

Enables runtime metrics collection for Application Signals (CPU, memory, GC, threads).
Only takes effect when OTEL_AWS_APPLICATION_SIGNALS_ENABLED is also "true".
Default: "true"
"""

OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT = "OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT"
"""
.. envvar:: OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT

The endpoint to export Application Signals metrics to.
Default: "http://localhost:4316/v1/metrics" (http/protobuf) or "localhost:4315" (grpc)
"""

# Deprecated aliases
OTEL_AWS_APP_SIGNALS_ENABLED = "OTEL_AWS_APP_SIGNALS_ENABLED"
"""Deprecated: Use OTEL_AWS_APPLICATION_SIGNALS_ENABLED instead."""

OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT = "OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT"
"""Deprecated: Use OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT instead."""
