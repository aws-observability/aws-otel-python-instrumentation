# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import PromptReference
from pydantic import AnyUrl


class MCPHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # pylint: disable=invalid-name
        if "call_tool" in self.path:
            asyncio.run(self._call_mcp_server("call_tool"))
        elif "list_tools" in self.path:
            asyncio.run(self._call_mcp_server("list_tools"))
        elif "list_prompts" in self.path:
            asyncio.run(self._call_mcp_server("list_prompts"))
        elif "list_resources" in self.path:
            asyncio.run(self._call_mcp_server("list_resources"))
        elif "read_resource" in self.path:
            asyncio.run(self._call_mcp_server("read_resource"))
        elif "get_prompt" in self.path:
            asyncio.run(self._call_mcp_server("get_prompt"))
        elif "complete" in self.path:
            asyncio.run(self._call_mcp_server("complete"))
        elif "set_logging_level" in self.path:
            asyncio.run(self._call_mcp_server("set_logging_level"))
        elif "ping" in self.path:
            asyncio.run(self._call_mcp_server("ping"))
        else:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.end_headers()

    @staticmethod
    async def _call_mcp_server(action, *args):
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
                result = None
                if action == "list_tools":
                    result = await session.list_tools()
                elif action == "call_tool":
                    result = await session.call_tool("echo", {"text": "Hello from HTTP request!"})
                elif action == "list_prompts":
                    result = await session.list_prompts()
                elif action == "list_resources":
                    result = await session.list_resources()
                elif action == "read_resource":
                    result = await session.read_resource(AnyUrl("file://sample.txt"))
                elif action == "get_prompt":
                    result = await session.get_prompt("greeting", {"name": "Test User"})
                elif action == "complete":
                    prompt_ref = PromptReference(type="ref/prompt", name="greeting")
                    result = await session.complete(ref=prompt_ref, argument={"name": "completion_test"})
                elif action == "set_logging_level":
                    result = await session.set_logging_level("info")
                elif action == "ping":
                    result = await session.send_ping()
                
                return result


if __name__ == "__main__":
    print("Ready")
    server = HTTPServer(("0.0.0.0", 8080), MCPHandler)
    server.serve_forever()
