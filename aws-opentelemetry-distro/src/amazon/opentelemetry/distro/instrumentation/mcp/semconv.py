# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
MCP (Model Context Protocol) Semantic Conventions for OpenTelemetry.

This module defines semantic conventions for MCP instrumentation following
OpenTelemetry standards for consistent telemetry data.
"""


class MCPAttributes:
    """MCP-specific span attributes for OpenTelemetry instrumentation."""

    # MCP Operation Type Attributes
    MCP_INITIALIZE = "mcp.initialize"
    """
    Boolean attribute indicating this span represents an MCP initialize operation.
    Set to True when the span tracks session initialization between client and server.
    """

    MCP_LIST_TOOLS = "mcp.list_tools"
    """
    Boolean attribute indicating this span represents an MCP list tools operation.
    Set to True when the span tracks discovery of available tools on the server.
    """

    MCP_CALL_TOOL = "mcp.call_tool"
    """
    Boolean attribute indicating this span represents an MCP call tool operation.
    Set to True when the span tracks execution of a specific tool.
    """

    # MCP Tool Information
    MCP_TOOL_NAME = "mcp.tool.name"
    """
    The name of the MCP tool being called.
    Example: "echo", "search", "calculator"
    """

    # AWS-specific Remote Service Attributes
    AWS_REMOTE_SERVICE = "aws.remote.service"
    """
    The name of the remote MCP service being called.
    Default: "mcp server" (can be overridden via MCP_INSTRUMENTATION_SERVER_NAME env var)
    """

    AWS_REMOTE_OPERATION = "aws.remote.operation"
    """
    The specific MCP operation being performed.
    Values: "Initialize", "ListTool", or the specific tool name for call operations
    """


class MCPSpanNames:
    """Standard span names for MCP operations."""

    # Client-side span names
    CLIENT_SEND_REQUEST = "client.send_request"
    """
    Span name for client-side MCP request operations.
    Used for all outgoing MCP requests (initialize, list tools, call tool).
    """

    CLIENT_INITIALIZE = "mcp.initialize"
    """
    Span name for client-side MCP initialization requests.
    """

    CLIENT_LIST_TOOLS = "mcp.list_tools"
    """
    Span name for client-side MCP list tools requests.
    """

    @staticmethod
    def client_call_tool(tool_name: str) -> str:
        """
        Generate span name for client-side MCP tool call requests.

        Args:
            tool_name: Name of the tool being called

        Returns:
            Formatted span name like "mcp.call_tool.echo", "mcp.call_tool.search"
        """
        return f"mcp.call_tool.{tool_name}"

    # Server-side span names
    TOOLS_INITIALIZE = "tools/initialize"
    """
    Span name for server-side MCP initialization handling.
    Tracks server processing of client initialization requests.
    """

    TOOLS_LIST = "tools/list"
    """
    Span name for server-side MCP list tools handling.
    Tracks server processing of tool discovery requests.
    """

    @staticmethod
    def tools_call(tool_name: str) -> str:
        """
        Generate span name for server-side MCP tool call handling.

        Args:
            tool_name: Name of the tool being called

        Returns:
            Formatted span name like "tools/echo", "tools/search"
        """
        return f"tools/{tool_name}"


class MCPOperations:
    """Standard operation names for MCP semantic conventions."""

    INITIALIZE = "Initialize"
    """Operation name for MCP session initialization."""

    LIST_TOOL = "ListTool"
    """Operation name for MCP tool discovery."""

    UNKNOWN_OPERATION = "UnknownOperation"
    """Fallback operation name for unrecognized MCP operations."""


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
