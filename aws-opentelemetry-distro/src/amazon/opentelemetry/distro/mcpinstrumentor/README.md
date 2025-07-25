# MCP Instrumentor

OpenTelemetry MCP instrumentation package for AWS Distro.

## Installation

```bash
pip install amazon-opentelemetry-distro-mcpinstrumentor
```

## Usage

```python
from mcpinstrumentor import MCPInstrumentor

MCPInstrumentor().instrument()
```

## Configuration

### Environment Variables

- `MCP_SERVICE_NAME`: Sets the service name for MCP client spans. Defaults to "Generic MCP Server" if not set.

```bash
export MCP_SERVICE_NAME="My Custom MCP Server"
```

## Features

- Automatic instrumentation of MCP client and server requests
- Distributed tracing support with trace context propagation
- Configurable service naming via environment variables