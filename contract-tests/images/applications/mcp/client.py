# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class MCPHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # pylint: disable=invalid-name
        if self.path == "/mcp/echo":
            asyncio.run(self._call_mcp_tool("echo", {"text": "Hello from HTTP request!"}))
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    @staticmethod
    async def _call_mcp_tool(tool_name, arguments):
        server_env = {
            "OTEL_PYTHON_DISTRO": "aws_distro",
            "OTEL_PYTHON_CONFIGURATOR": "aws_configurator",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", ""),
            "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
            "OTEL_TRACES_SAMPLER": "always_on",
            "OTEL_METRICS_EXPORTER": "none",
            "OTEL_LOGS_EXPORTER": "none",
        }
        server_params = StdioServerParameters(
            command="opentelemetry-instrument", args=["python3", "mcp_server.py"], env=server_env
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)
                return result


if __name__ == "__main__":
    print("Ready")
    server = HTTPServer(("0.0.0.0", 8080), MCPHandler)
    server.serve_forever()
