# MCP Instrumentor

OpenTelemetry instrumentation for Model Context Protocol (MCP).

## Installation

Included in AWS OpenTelemetry Distro:

```bash
pip install aws-opentelemetry-distro
```

## Usage

Automatically enabled with:

```bash
opentelemetry-instrument python your_mcp_app.py
```

## Configuration

- `MCP_INSTRUMENTATION_SERVER_NAME`: Override default server name (default: "mcp server")

## Spans Created

- **Client**: 
  - Initialize: `mcp.initialize`
  - List Tools: `mcp.list_tools`
  - Call Tool: `mcp.call_tool.{tool_name}`
- **Server**: `tools/initialize`, `tools/list`, `tools/{tool_name}`