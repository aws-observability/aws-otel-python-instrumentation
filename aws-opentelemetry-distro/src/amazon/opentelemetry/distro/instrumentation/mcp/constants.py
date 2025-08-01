# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
MCP (Model Context Protocol) Constants for OpenTelemetry instrumentation.

This module defines constants and configuration variables used by the MCP instrumentor.
"""


class MCPTraceContext:
    """Constants for MCP distributed tracing context propagation."""

    TRACEPARENT_HEADER = "traceparent"
    """
    W3C Trace Context traceparent header name.
    Used for propagating trace context in MCP request metadata.
    """

    TRACE_FLAGS_SAMPLED = "01"
    """
    W3C Trace Context flags indicating the trace is sampled.
    """

    TRACEPARENT_VERSION = "00"
    """
    W3C Trace Context version identifier.
    """


class MCPEnvironmentVariables:
    """Environment variable names for MCP instrumentation configuration."""

    SERVER_NAME = "MCP_INSTRUMENTATION_SERVER_NAME"
    """
    Environment variable to override the default MCP server name.
    Default value: "mcp server"
    """
