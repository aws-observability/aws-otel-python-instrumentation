# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from fastmcp import FastMCP

# Create FastMCP server instance
mcp = FastMCP("Simple MCP Server")


@mcp.tool(name="echo", description="Echo the provided text")
def echo(text: str) -> str:
    """Echo the provided text"""
    return f"Echo: {text}"


if __name__ == "__main__":
    mcp.run()
