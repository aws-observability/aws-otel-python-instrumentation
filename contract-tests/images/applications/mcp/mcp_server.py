# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from fastmcp import FastMCP

# Create FastMCP server instance
mcp = FastMCP("Simple MCP Server")


@mcp.tool(name="echo", description="Echo the provided text")
def echo(text: str) -> str:
    """Echo the provided text"""
    return f"Echo: {text}"


@mcp.resource(uri="file://sample.txt", name="Sample Resource")
def sample_resource() -> str:
    """Sample MCP resource"""
    return "This is a sample resource content"


@mcp.prompt(name="greeting", description="Generate a greeting message")
def greeting_prompt(name: str = "World") -> str:
    """Generate a personalized greeting"""
    return f"Hello, {name}! Welcome to our MCP server."

if __name__ == "__main__":
    mcp.run()
