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
    MCP_INITIALIZE = "notifications/initialize"
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


class MCPSpanNames:
    """Standard span names for MCP operations."""

    # Client-side span names
    CLIENT_SEND_REQUEST = "client.send_request"
    """
    Span name for client-side MCP request operations.
    Used for all outgoing MCP requests (initialize, list tools, call tool).
    """

    CLIENT_INITIALIZE = "notifications/initialize"
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

    INITIALIZE = "Notifications/Initialize"
    """Operation name for MCP session initialization."""

    LIST_TOOL = "ListTool"
    """Operation name for MCP tool discovery."""

    UNKNOWN_OPERATION = "UnknownOperation"
    """Fallback operation name for unrecognized MCP operations."""
