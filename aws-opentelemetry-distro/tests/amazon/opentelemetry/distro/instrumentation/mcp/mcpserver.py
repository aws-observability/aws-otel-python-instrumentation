# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys

from amazon.opentelemetry.distro.instrumentation.mcp import McpInstrumentor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

tracer_provider = TracerProvider()
if len(sys.argv) > 1:
    otlp_port = sys.argv[1]
    span_exporter = OTLPSpanExporter(f"http://localhost:{otlp_port}/v1/traces")
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))

McpInstrumentor().instrument(tracer_provider=tracer_provider)

from mcp.server.fastmcp import FastMCP  # noqa: E402

server = FastMCP()


@server.tool()
def hello(name: str) -> str:
    """Say hello."""
    return f"Hello, {name}!"


@server.tool()
def failing_tool() -> str:
    """A tool that always fails."""
    raise ValueError("Tool execution failed")


@server.prompt()
def greeting_prompt(name: str) -> str:
    """Generate a greeting prompt."""
    return f"Please greet {name} warmly."


@server.resource("test://example")
def example_resource() -> str:
    """An example resource."""
    return "Example resource content"


try:
    if len(sys.argv) > 2:
        import uvicorn  # noqa: E402

        http_port = int(sys.argv[2])
        transport = sys.argv[3] if len(sys.argv) > 3 else "http"
        if transport == "sse":
            uvicorn.run(server.sse_app(), host="127.0.0.1", port=http_port, log_level="critical")
        else:
            uvicorn.run(server.streamable_http_app(), host="127.0.0.1", port=http_port, log_level="critical")
    else:
        server.run(transport="stdio")
finally:
    tracer_provider.shutdown()
