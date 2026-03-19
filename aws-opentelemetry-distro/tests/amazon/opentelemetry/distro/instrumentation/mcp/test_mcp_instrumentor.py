# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import signal
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from threading import Thread
from unittest import TestCase

from collector import OTLPServer, Telemetry

from amazon.opentelemetry.distro.instrumentation.mcp import McpInstrumentor
from opentelemetry.baggage.propagation import W3CBaggagePropagator

try:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
except ImportError:
    HTTPXClientInstrumentor = None
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.proto.trace.v1.trace_pb2 import Span as ProtoSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROMPT,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.semconv._incubating.attributes.mcp_attributes import (
    MCP_METHOD_NAME,
    MCP_PROTOCOL_VERSION,
    MCP_RESOURCE_URI,
    MCP_SESSION_ID,
    McpMethodNameValues,
)
from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_RESPONSE_STATUS_CODE
from opentelemetry.semconv.attributes.client_attributes import CLIENT_ADDRESS, CLIENT_PORT
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import NETWORK_TRANSPORT, NetworkTransportValues
from opentelemetry.trace import SpanKind, StatusCode, get_tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


class McpInstrumentorTestBase(TestCase):

    def setUp(self):
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.propagator = CompositePropagator(
            [W3CBaggagePropagator(), AwsXRayPropagator(), TraceContextTextMapPropagator()]
        )
        self.instrumentor = McpInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider, propagators=self.propagator)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self.span_exporter.clear()

    @staticmethod
    def _get_free_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @staticmethod
    def _wait_for_server(port, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(0.2)
        raise TimeoutError(f"Server on port {port} not ready after {timeout}s")

    def _get_attr(self, span, key):
        if hasattr(span.attributes, "get"):
            return span.attributes.get(key)
        for attr in span.attributes:
            if attr.key == key:
                return attr.value.string_value or attr.value.int_value
        return None

    def _assert_span_attrs(self, span, expected):
        for key, val in expected.items():
            actual_val = self._get_attr(span, key)
            self.assertIsNotNone(actual_val, f"Missing attribute: {key}")
            self.assertEqual(actual_val, val, f"Value mismatch for {key}")

    @staticmethod
    def _get_streamable_http_client():
        try:
            from mcp.client.streamable_http import streamable_http_client  # pylint: disable=import-outside-toplevel

            return streamable_http_client
        except ImportError:
            from mcp.client.streamable_http import streamablehttp_client  # pylint: disable=import-outside-toplevel

            return streamablehttp_client

    @staticmethod
    def _get_span(spans, name):
        return next(s for s in spans if s.name == name)


@unittest.skipIf(sys.version_info < (3, 10), "mcp requires Python >= 3.10")
class TestMcpInstrumentor(McpInstrumentorTestBase):

    def setUp(self):
        super().setUp()
        self.server_telemetry = Telemetry()
        self.otlp_server = OTLPServer(("localhost", 0), self.server_telemetry)
        self.collector_thread = Thread(target=self.otlp_server.serve_forever, daemon=True)
        self.collector_thread.start()

    def tearDown(self):
        self.server_telemetry.clear()
        self.otlp_server.shutdown()
        self.collector_thread.join(timeout=1)
        super().tearDown()

    def test_instrumentor_initialization(self):
        self.assertIsNotNone(self.instrumentor._client_wrapper)
        self.assertIsNotNone(self.instrumentor._server_wrapper)

    def test_mcp_tools(self):
        for transport in ["stdio", "http", "sse"]:
            with self.subTest(transport=transport):

                async def run_client(session):
                    await session.initialize()
                    await session.call_tool("hello", {"name": "World"})

                client_spans, _ = self._run_transport_test(run_client, transport, "mcp tools/call hello")

                tool_span = self._get_span(client_spans, "mcp tools/call hello")
                self._assert_span_attrs(
                    tool_span,
                    {
                        MCP_METHOD_NAME: McpMethodNameValues.TOOLS_CALL.value,
                        GEN_AI_TOOL_NAME: "hello",
                        GEN_AI_OPERATION_NAME: GenAiOperationNameValues.EXECUTE_TOOL.value,
                        GEN_AI_TOOL_CALL_ARGUMENTS: json.dumps({"name": "World"}),
                    },
                )
                self.assertIn("Hello, World", tool_span.attributes.get(GEN_AI_TOOL_CALL_RESULT))

    def test_mcp_tool_error(self):
        for transport in ["stdio", "http", "sse"]:
            with self.subTest(transport=transport):

                async def run_client(session):
                    await session.initialize()
                    await session.call_tool("failing_tool", {})

                client_spans, server_spans = self._run_transport_test(
                    run_client, transport, "mcp tools/call failing_tool"
                )

                tool_span = self._get_span(client_spans, "mcp tools/call failing_tool")
                self._assert_span_attrs(
                    tool_span,
                    {
                        MCP_METHOD_NAME: McpMethodNameValues.TOOLS_CALL.value,
                        GEN_AI_TOOL_NAME: "failing_tool",
                        GEN_AI_OPERATION_NAME: GenAiOperationNameValues.EXECUTE_TOOL.value,
                    },
                )
                self.assertIn("error", tool_span.attributes.get(GEN_AI_TOOL_CALL_RESULT, "").lower())
                self.assertEqual(tool_span.attributes.get(ERROR_TYPE), "tool_error")
                self.assertEqual(tool_span.status.status_code, StatusCode.ERROR)

                server_tool_span = self._get_span(server_spans, "mcp tools/call failing_tool")
                self.assertEqual(server_tool_span.kind, ProtoSpan.SpanKind.SPAN_KIND_SERVER)

    def test_mcp_prompt(self):
        for transport in ["stdio", "http", "sse"]:
            with self.subTest(transport=transport):

                async def run_client(session):
                    await session.initialize()
                    await session.get_prompt("greeting_prompt", {"name": "Alice"})

                client_spans, _ = self._run_transport_test(run_client, transport, "mcp prompts/get greeting_prompt")

                prompt_span = self._get_span(client_spans, "mcp prompts/get greeting_prompt")
                self.assertEqual(prompt_span.attributes.get(GEN_AI_PROMPT), "greeting_prompt")

    def test_mcp_resource(self):
        for transport in ["stdio", "http", "sse"]:
            with self.subTest(transport=transport):

                async def run_client(session):
                    await session.initialize()
                    await session.read_resource("test://example")

                client_spans, _ = self._run_transport_test(run_client, transport, "mcp resources/read test://example")

                resource_span = self._get_span(client_spans, "mcp resources/read test://example")
                self.assertEqual(resource_span.attributes.get(MCP_RESOURCE_URI), "test://example")

    def test_mcp_error_nonexistent_resource(self):
        from mcp.shared.exceptions import McpError  # pylint: disable=import-outside-toplevel

        async def run_client(session):
            await session.initialize()
            await session.read_resource("nonexistent://resource")

        self.span_exporter.clear()
        with self.assertRaises((McpError, BaseException)):
            asyncio.run(self._run_stdio_client(run_client))

        client_spans = self.span_exporter.get_finished_spans()
        resource_span = self._get_span(client_spans, "mcp resources/read nonexistent://resource")
        self.assertEqual(resource_span.attributes.get(ERROR_TYPE), "McpError")
        self.assertEqual(resource_span.status.status_code, StatusCode.ERROR)
        self.assertIsNotNone(resource_span.attributes.get(RPC_RESPONSE_STATUS_CODE))

    def _run_transport_test(self, callback, transport, operation_span_name):
        self.span_exporter.clear()
        self.server_telemetry.clear()

        if transport == "stdio":
            asyncio.run(self._run_stdio_client(callback))
            expected_network = NetworkTransportValues.PIPE.value
            is_http = False
        else:
            asyncio.run(self._run_http_client(callback, transport=transport))
            expected_network = NetworkTransportValues.TCP.value
            is_http = True

        client_spans = self.span_exporter.get_finished_spans()
        server_spans = self._collect_server_spans()
        self._assert_client_and_server_spans(
            client_spans, server_spans, expected_network, is_http=is_http, operation_span_name=operation_span_name
        )
        return client_spans, server_spans

    async def _run_stdio_client(self, callback):
        # pylint: disable=import-outside-toplevel
        from mcp.client.session import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_script = str(Path(__file__).parent / "mcpserver.py")
        otlp_port = str(self.otlp_server.server_port)

        async with stdio_client(StdioServerParameters(command=sys.executable, args=[server_script, otlp_port])) as (
            read,
            write,
        ):
            async with ClientSession(read, write) as session:
                await callback(session)

    async def _run_http_client(self, callback, transport="http"):
        # pylint: disable=import-outside-toplevel
        from mcp.client.session import ClientSession

        if transport == "sse":
            from mcp.client.sse import sse_client as client

            path = "/sse"
        else:
            client = self._get_streamable_http_client()
            path = "/mcp"

        server_script = str(Path(__file__).parent / "mcpserver.py")
        otlp_port = str(self.otlp_server.server_port)
        http_port = self._get_free_port()

        server_proc = subprocess.Popen(
            [sys.executable, server_script, otlp_port, str(http_port)] + ([transport] if transport == "sse" else []),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_for_server(http_port)

        try:
            async with client(f"http://127.0.0.1:{http_port}{path}") as streams:
                read, write = streams[0], streams[1]
                async with ClientSession(read, write) as session:
                    await callback(session)
        finally:
            server_proc.send_signal(signal.SIGINT)
            server_proc.wait(timeout=5)

    def _assert_client_and_server_spans(
        self, client_spans, server_spans, expected_transport, is_http=False, operation_span_name=None
    ):
        session_span = self._get_span(client_spans, "mcp.session")
        self.assertEqual(session_span.kind, SpanKind.INTERNAL)

        init_span = self._get_span(client_spans, f"mcp {McpMethodNameValues.INITIALIZE.value}")
        self.assertIsNotNone(init_span.attributes.get(MCP_PROTOCOL_VERSION))

        client_notif_init_span = self._get_span(
            client_spans, f"mcp {McpMethodNameValues.NOTIFICATIONS_INITIALIZED.value}"
        )

        self.assertEqual(init_span.kind, SpanKind.CLIENT)
        self._assert_span_attrs(
            init_span, {MCP_METHOD_NAME: McpMethodNameValues.INITIALIZE.value, NETWORK_TRANSPORT: expected_transport}
        )

        self.assertEqual(client_notif_init_span.kind, SpanKind.CLIENT)
        self._assert_span_attrs(
            client_notif_init_span,
            {
                MCP_METHOD_NAME: McpMethodNameValues.NOTIFICATIONS_INITIALIZED.value,
                NETWORK_TRANSPORT: expected_transport,
            },
        )

        server_init_span = self._get_span(server_spans, f"mcp {McpMethodNameValues.NOTIFICATIONS_INITIALIZED.value}")
        self.assertEqual(server_init_span.kind, ProtoSpan.SpanKind.SPAN_KIND_SERVER)
        self._assert_span_attrs(
            server_init_span, {MCP_METHOD_NAME: McpMethodNameValues.NOTIFICATIONS_INITIALIZED.value}
        )
        self._assert_no_attr(server_init_span, NETWORK_TRANSPORT)
        self._assert_context_propagation(client_notif_init_span, server_init_span)

        client_op_spans = [init_span, client_notif_init_span]

        if operation_span_name:
            client_op_span = self._get_span(client_spans, operation_span_name)
            server_op_span = self._get_span(server_spans, operation_span_name)
            client_op_spans.append(client_op_span)

            self.assertEqual(client_op_span.kind, SpanKind.CLIENT)
            self._assert_span_attrs(client_op_span, {NETWORK_TRANSPORT: expected_transport})

            self.assertEqual(server_op_span.kind, ProtoSpan.SpanKind.SPAN_KIND_SERVER)
            self._assert_span_attrs(server_op_span, {NETWORK_TRANSPORT: expected_transport})

            if is_http:
                self.assertIsNotNone(self._get_attr(server_op_span, MCP_SESSION_ID))
                self.assertIsNotNone(self._get_attr(server_op_span, CLIENT_ADDRESS))
                self.assertIsNotNone(self._get_attr(server_op_span, CLIENT_PORT))

            self._assert_context_propagation(client_op_span, server_op_span)

        session_span_id = format(session_span.context.span_id, "016x")
        for span in client_op_spans:
            self.assertEqual(format(span.parent.span_id, "016x"), session_span_id)

    def _collect_server_spans(self):
        spans = []
        for resource_spans in self.server_telemetry.traces:
            for scope_spans in resource_spans.scope_spans:
                spans.extend(scope_spans.spans)
        return spans

    def _assert_no_attr(self, span, key):
        self.assertIsNone(self._get_attr(span, key), f"Attribute {key} should not be present")

    def _assert_context_propagation(self, parent_span, child_span):
        parent_trace_id = format(parent_span.context.trace_id, "032x")
        child_trace_id = child_span.trace_id.hex()
        self.assertEqual(parent_trace_id, child_trace_id)

        parent_span_id = format(parent_span.context.span_id, "016x")
        child_parent_id = child_span.parent_span_id.hex()
        self.assertEqual(parent_span_id, child_parent_id)


@unittest.skipIf(sys.version_info < (3, 10), "mcp requires Python >= 3.10")
class TestMcpInstrumentorInProcess(McpInstrumentorTestBase):

    def setUp(self):
        super().setUp()
        self.server = self._create_server()

    def test_server_tool_span(self):
        async def run(session):
            await session.call_tool("hello", {"name": "World"})

        asyncio.run(self._run_inprocess(run))
        spans = self.span_exporter.get_finished_spans()

        server_span = self._get_server_span(spans, "mcp tools/call hello")
        self.assertEqual(server_span.attributes.get(NETWORK_TRANSPORT), NetworkTransportValues.PIPE.value)
        self.assertEqual(server_span.attributes.get(MCP_METHOD_NAME), McpMethodNameValues.TOOLS_CALL.value)

    def test_server_resource_span(self):
        async def run(session):
            await session.read_resource("test://example")

        asyncio.run(self._run_inprocess(run))
        spans = self.span_exporter.get_finished_spans()

        server_span = self._get_server_span(spans, "mcp resources/read test://example")
        self.assertEqual(server_span.attributes.get(NETWORK_TRANSPORT), NetworkTransportValues.PIPE.value)

    def test_server_error_exception(self):
        from mcp.shared.exceptions import McpError  # pylint: disable=import-outside-toplevel

        async def run(session):
            await session.read_resource("nonexistent://resource")

        with self.assertRaises((McpError, BaseException)):
            asyncio.run(self._run_inprocess(run, raise_exceptions=True))

        spans = self.span_exporter.get_finished_spans()
        server_span = self._get_server_span(spans, "mcp resources/read nonexistent://resource")
        self.assertEqual(server_span.attributes.get(ERROR_TYPE), "ValueError")
        self.assertEqual(server_span.status.status_code, StatusCode.ERROR)

    def test_server_http_transport(self):
        async def run(session):
            await session.call_tool("hello", {"name": "World"})

        asyncio.run(self._run_http_inprocess(run))
        spans = self.span_exporter.get_finished_spans()

        server_span = self._get_server_span(spans, "mcp tools/call hello")
        self.assertEqual(server_span.attributes.get(NETWORK_TRANSPORT), NetworkTransportValues.TCP.value)
        self.assertIsNotNone(server_span.attributes.get(MCP_SESSION_ID))
        self.assertIsNotNone(server_span.attributes.get(CLIENT_ADDRESS))
        self.assertIsNotNone(server_span.attributes.get(CLIENT_PORT))

    def test_http_span_parented_under_mcp_request(self):

        HTTPXClientInstrumentor().instrument(tracer_provider=self.tracer_provider)
        try:

            async def run(session):
                await session.call_tool("hello", {"name": "World"})

            asyncio.run(self._run_http_inprocess(run))
            spans = self.span_exporter.get_finished_spans()

            tool_span = next(s for s in spans if s.name == "mcp tools/call hello" and s.kind == SpanKind.CLIENT)
            post_spans = [s for s in spans if s.name == "POST" and s.kind == SpanKind.CLIENT]

            self.assertTrue(len(post_spans) > 0, "Expected at least one httpx POST span")

            tool_span_id = format(tool_span.context.span_id, "016x")
            parented_posts = [s for s in post_spans if format(s.parent.span_id, "016x") == tool_span_id]
            self.assertTrue(
                len(parented_posts) > 0,
                "httpx POST span should be parented under MCP tool call span",
            )
        finally:
            HTTPXClientInstrumentor().uninstrument()

    def test_mcp_respects_active_parent_span(self):
        tracer = get_tracer("test", tracer_provider=self.tracer_provider)

        async def run(session):
            with tracer.start_as_current_span("execute_tool get_weather"):
                await session.call_tool("hello", {"name": "World"})

        asyncio.run(self._run_inprocess(run))
        spans = self.span_exporter.get_finished_spans()

        tool_parent = next(s for s in spans if s.name == "execute_tool get_weather")
        tool_call = next(s for s in spans if s.name == "mcp tools/call hello" and s.kind == SpanKind.CLIENT)

        tool_parent_id = format(tool_parent.context.span_id, "016x")
        self.assertEqual(format(tool_call.parent.span_id, "016x"), tool_parent_id)

    async def _run_inprocess(self, callback, raise_exceptions=False):
        from mcp.shared.memory import (  # pylint: disable=import-outside-toplevel
            create_connected_server_and_client_session,
        )

        async with create_connected_server_and_client_session(
            self.server, raise_exceptions=raise_exceptions
        ) as session:
            await callback(session)

    async def _run_http_inprocess(self, callback):
        # pylint: disable=import-outside-toplevel
        import anyio
        import uvicorn
        from mcp.client.session import ClientSession

        client = self._get_streamable_http_client()
        port = self._get_free_port()
        config = uvicorn.Config(self.server.streamable_http_app(), host="127.0.0.1", port=port, log_level="critical")
        server = uvicorn.Server(config)

        async with anyio.create_task_group() as tg:
            tg.start_soon(server.serve)
            await anyio.sleep(0.5)
            try:
                async with client(f"http://127.0.0.1:{port}/mcp") as streams:
                    async with ClientSession(streams[0], streams[1]) as session:
                        await session.initialize()
                        await callback(session)
            finally:
                server.should_exit = True

    @staticmethod
    def _create_server():
        from mcp.server.fastmcp import FastMCP  # pylint: disable=import-outside-toplevel

        server = FastMCP()

        @server.tool()
        def hello(name: str) -> str:
            return f"Hello, {name}!"

        @server.tool()
        def failing_tool() -> str:
            raise ValueError("Tool execution failed")

        @server.resource("test://example")
        def example_resource() -> str:
            return "Example resource content"

        return server

    @staticmethod
    def _get_server_span(spans, name):
        return next(s for s in spans if s.name == name and s.kind == SpanKind.SERVER)


if __name__ == "__main__":
    unittest.main()
