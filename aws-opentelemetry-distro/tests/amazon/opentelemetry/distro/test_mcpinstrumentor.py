# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for MCPInstrumentor - testing actual mcpinstrumentor methods
"""

import asyncio
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor import MCPInstrumentor


class SimpleSpanContext:
    """Simple mock span context without using MagicMock"""

    def __init__(self, trace_id: int, span_id: int) -> None:
        self.trace_id = trace_id
        self.span_id = span_id


class SimpleTracerProvider:
    """Simple mock tracer provider without using MagicMock"""

    def __init__(self) -> None:
        self.get_tracer_called = False
        self.tracer_name: Optional[str] = None

    def get_tracer(self, name: str) -> str:
        self.get_tracer_called = True
        self.tracer_name = name
        return "mock_tracer_from_provider"


class TestInjectTraceContext(unittest.TestCase):
    """Test the _inject_trace_context method"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_inject_trace_context_empty_dict(self) -> None:
        """Test injecting trace context into empty dictionary"""
        # Setup
        request_data = {}
        span_ctx = SimpleSpanContext(trace_id=12345, span_id=67890)

        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Verify - now uses traceparent W3C format
        self.assertIn("params", request_data)
        self.assertIn("_meta", request_data["params"])
        self.assertIn("traceparent", request_data["params"]["_meta"])

        # Verify traceparent format: "00-{trace_id:032x}-{span_id:016x}-01"
        traceparent = request_data["params"]["_meta"]["traceparent"]
        self.assertTrue(traceparent.startswith("00-"))
        self.assertTrue(traceparent.endswith("-01"))
        parts = traceparent.split("-")
        self.assertEqual(len(parts), 4)
        self.assertEqual(int(parts[1], 16), 12345)  # trace_id
        self.assertEqual(int(parts[2], 16), 67890)  # span_id

    def test_inject_trace_context_existing_params(self) -> None:
        """Test injecting trace context when params already exist"""
        # Setup
        request_data = {"params": {"existing_field": "test_value"}}
        span_ctx = SimpleSpanContext(trace_id=99999, span_id=11111)

        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Verify the existing field is preserved and traceparent is added
        self.assertEqual(request_data["params"]["existing_field"], "test_value")
        self.assertIn("_meta", request_data["params"])
        self.assertIn("traceparent", request_data["params"]["_meta"])

        # Verify traceparent format contains correct trace/span IDs
        traceparent = request_data["params"]["_meta"]["traceparent"]
        parts = traceparent.split("-")
        self.assertEqual(int(parts[1], 16), 99999)  # trace_id
        self.assertEqual(int(parts[2], 16), 11111)  # span_id


class TestTracerProvider(unittest.TestCase):
    """Test the tracer provider kwargs logic in _instrument method"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()
        # Reset tracer to ensure test isolation
        if hasattr(self.instrumentor, "tracer"):
            delattr(self.instrumentor, "tracer")

    def test_instrument_without_tracer_provider_kwargs(self) -> None:
        """Test _instrument method when no tracer_provider in kwargs - should use default tracer"""
        # Execute - Actually test the mcpinstrumentor method
        with unittest.mock.patch("opentelemetry.trace.get_tracer") as mock_get_tracer, unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.register_post_import_hook"
        ):
            mock_get_tracer.return_value = "default_tracer"
            self.instrumentor._instrument()

        # Verify - tracer should be set from trace.get_tracer
        self.assertTrue(hasattr(self.instrumentor, "tracer"))
        self.assertEqual(self.instrumentor.tracer, "default_tracer")
        mock_get_tracer.assert_called_with("instrumentation.mcp")

    def test_instrument_with_tracer_provider_kwargs(self) -> None:
        """Test _instrument method when tracer_provider is in kwargs - should use provider's tracer"""
        # Setup
        provider = SimpleTracerProvider()

        # Execute - Actually test the mcpinstrumentor method
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.register_post_import_hook"
        ):
            self.instrumentor._instrument(tracer_provider=provider)

        # Verify - tracer should be set from the provided tracer_provider
        self.assertTrue(hasattr(self.instrumentor, "tracer"))
        self.assertEqual(self.instrumentor.tracer, "mock_tracer_from_provider")
        self.assertTrue(provider.get_tracer_called)
        self.assertEqual(provider.tracer_name, "instrumentation.mcp")


class TestInstrumentationDependencies(unittest.TestCase):
    """Test the instrumentation_dependencies method"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_instrumentation_dependencies(self) -> None:
        """Test that instrumentation_dependencies method returns the expected dependencies"""
        # Execute - Actually test the mcpinstrumentor method
        dependencies = self.instrumentor.instrumentation_dependencies()

        # Verify - should return the _instruments collection
        self.assertIsNotNone(dependencies)
        # Should contain mcp dependency
        self.assertIn("mcp >= 1.6.0", dependencies)


class TestTraceContextInjection(unittest.TestCase):
    """Test trace context injection using actual mcpinstrumentor methods"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_trace_context_injection_with_realistic_request(self) -> None:
        """Test actual trace context injection using mcpinstrumentor._inject_trace_context with realistic MCP request"""

        # Create a realistic MCP request structure
        class CallToolRequest:
            def __init__(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None) -> None:
                self.root = self
                self.params = CallToolParams(tool_name, arguments)

            def model_dump(
                self, by_alias: bool = True, mode: str = "json", exclude_none: bool = True
            ) -> Dict[str, Any]:
                result = {"method": "call_tool", "params": {"name": self.params.name}}
                if self.params.arguments:
                    result["params"]["arguments"] = self.params.arguments
                # Include _meta if it exists (trace context injection point)
                if hasattr(self.params, "_meta") and self.params._meta:
                    result["params"]["_meta"] = self.params._meta
                return result

            # converting raw dictionary data back into an instance of this class
            @classmethod
            def model_validate(cls, data: Dict[str, Any]) -> "CallToolRequest":
                instance = cls(data["params"]["name"], data["params"].get("arguments"))
                # Restore _meta field if present
                if "_meta" in data["params"]:
                    instance.params._meta = data["params"]["_meta"]
                return instance

        class CallToolParams:
            def __init__(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> None:
                self.name = name
                self.arguments = arguments
                self._meta: Optional[Dict[str, Any]] = None  # Will hold trace context

        # Client creates original request
        client_request = CallToolRequest("create_metric", {"metric_name": "response_time", "value": 250})

        # Client injects trace context using ACTUAL mcpinstrumentor method
        original_trace_context = SimpleSpanContext(trace_id=98765, span_id=43210)
        request_data = client_request.model_dump()

        # This is the actual mcpinstrumentor method we're testing
        self.instrumentor._inject_trace_context(request_data, original_trace_context)

        # Create modified request with trace context
        modified_request = CallToolRequest.model_validate(request_data)

        # Verify the actual mcpinstrumentor method worked correctly
        client_data = modified_request.model_dump()
        self.assertIn("_meta", client_data["params"])
        self.assertIn("traceparent", client_data["params"]["_meta"])

        # Verify traceparent format contains correct trace/span IDs
        traceparent = client_data["params"]["_meta"]["traceparent"]
        parts = traceparent.split("-")
        self.assertEqual(int(parts[1], 16), 98765)  # trace_id
        self.assertEqual(int(parts[2], 16), 43210)  # span_id

        # Verify the tool call data is also preserved
        self.assertEqual(client_data["params"]["name"], "create_metric")
        self.assertEqual(client_data["params"]["arguments"]["metric_name"], "response_time")


class TestInstrumentedMCPServer(unittest.TestCase):
    """Test mcpinstrumentor with a mock MCP server to verify end-to-end functionality"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()
        # Initialize tracer so the instrumentor can work
        mock_tracer = MagicMock()
        self.instrumentor.tracer = mock_tracer

    def test_no_trace_context_fallback(self) -> None:
        """Test graceful handling when no trace context is present on server side"""

        class MockServerNoTrace:
            @staticmethod
            async def _handle_request(session: Any, request: Any) -> Dict[str, Any]:
                return {"success": True, "handled_without_trace": True}

        class MockServerRequestNoTrace:
            def __init__(self, tool_name: str) -> None:
                self.params = MockServerRequestParamsNoTrace(tool_name)

        class MockServerRequestParamsNoTrace:
            def __init__(self, name: str) -> None:
                self.name = name
                self.meta: Optional[Any] = None  # No trace context

        mock_server = MockServerNoTrace()
        server_request = MockServerRequestNoTrace("create_metric")

        # Setup mocks
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        mock_tracer.start_as_current_span.return_value.__exit__.return_value = None

        # Test server handling without trace context (fallback scenario)
        with unittest.mock.patch("opentelemetry.trace.get_tracer", return_value=mock_tracer), unittest.mock.patch.dict(
            "sys.modules", {"mcp.types": MagicMock(), "mcp": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "_generate_mcp_attributes"), unittest.mock.patch.object(
            self.instrumentor, "_get_mcp_operation", return_value="tools/create_metric"
        ):

            result = asyncio.run(
                self.instrumentor._wrap_handle_request(mock_server._handle_request, None, (None, server_request), {})
            )

        # Verify graceful fallback - no tracing spans should be created when no trace context
        # The wrapper should call the original function without creating distributed trace spans
        self.assertEqual(result["success"], True)
        self.assertEqual(result["handled_without_trace"], True)

        # Should not create traced spans when no trace context is present
        mock_tracer.start_as_current_span.assert_not_called()

    # pylint: disable=too-many-locals,too-many-statements
    def test_end_to_end_client_server_communication(
        self,
    ) -> None:
        """Test where server actually receives what client sends (including injected trace context)"""

        # Create realistic request/response classes
        class MCPRequest:
            def __init__(
                self, tool_name: str, arguments: Optional[Dict[str, Any]] = None, method: str = "call_tool"
            ) -> None:
                self.root = self
                self.params = MCPRequestParams(tool_name, arguments)
                self.method = method

            def model_dump(
                self, by_alias: bool = True, mode: str = "json", exclude_none: bool = True
            ) -> Dict[str, Any]:
                result = {"method": self.method, "params": {"name": self.params.name}}
                if self.params.arguments:
                    result["params"]["arguments"] = self.params.arguments
                # Include _meta if it exists (for trace context)
                if hasattr(self.params, "_meta") and self.params._meta:
                    result["params"]["_meta"] = self.params._meta
                return result

            @classmethod
            def model_validate(cls, data: Dict[str, Any]) -> "MCPRequest":
                method = data.get("method", "call_tool")
                instance = cls(data["params"]["name"], data["params"].get("arguments"), method)
                # Restore _meta field if present
                if "_meta" in data["params"]:
                    instance.params._meta = data["params"]["_meta"]
                return instance

        class MCPRequestParams:
            def __init__(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> None:
                self.name = name
                self.arguments = arguments
                self._meta: Optional[Dict[str, Any]] = None

        class MCPServerRequest:
            def __init__(self, client_request_data: Dict[str, Any]) -> None:
                """Server request created from client's serialized data"""
                self.method = client_request_data.get("method", "call_tool")
                self.params = MCPServerRequestParams(client_request_data["params"])

        class MCPServerRequestParams:
            def __init__(self, params_data: Dict[str, Any]) -> None:
                self.name = params_data["name"]
                self.arguments = params_data.get("arguments")
                # Extract traceparent from _meta if present
                if "_meta" in params_data and "traceparent" in params_data["_meta"]:
                    self.meta = MCPServerRequestMeta(params_data["_meta"]["traceparent"])
                else:
                    self.meta = None

        class MCPServerRequestMeta:
            def __init__(self, traceparent: str) -> None:
                self.traceparent = traceparent

        # Mock client and server that actually communicate
        class EndToEndMCPSystem:
            def __init__(self) -> None:
                self.communication_log: List[str] = []
                self.last_sent_request: Optional[Any] = None

            async def client_send_request(self, request: Any) -> Dict[str, Any]:
                """Client sends request - captures what gets sent"""
                self.communication_log.append("CLIENT: Preparing to send request")
                self.last_sent_request = request  # Capture the modified request

                # Simulate sending over network - serialize the request
                serialized_request = request.model_dump()
                self.communication_log.append(f"CLIENT: Sent {serialized_request}")

                # Return client response
                return {"success": True, "client_response": "Request sent successfully"}

            async def server_handle_request(self, session: Any, server_request: Any) -> Dict[str, Any]:
                """Server handles the request it received"""
                self.communication_log.append(f"SERVER: Received request for {server_request.params.name}")

                # Check if traceparent was received
                if server_request.params.meta and server_request.params.meta.traceparent:
                    traceparent = server_request.params.meta.traceparent
                    # Parse traceparent to extract trace_id and span_id
                    parts = traceparent.split("-")
                    if len(parts) == 4:
                        trace_id = int(parts[1], 16)
                        span_id = int(parts[2], 16)
                        self.communication_log.append(
                            f"SERVER: Found trace context - trace_id: {trace_id}, " f"span_id: {span_id}"
                        )
                    else:
                        self.communication_log.append("SERVER: Invalid traceparent format")
                else:
                    self.communication_log.append("SERVER: No trace context found")

                return {"success": True, "server_response": f"Handled {server_request.params.name}"}

        # Create the end-to-end system
        e2e_system = EndToEndMCPSystem()

        # Create original client request
        original_request = MCPRequest("create_metric", {"name": "cpu_usage", "value": 85})

        # Setup OpenTelemetry mocks
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.trace_id = 12345
        mock_span_context.span_id = 67890
        mock_span.get_span_context.return_value = mock_span_context
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        mock_tracer.start_as_current_span.return_value.__exit__.return_value = None

        # STEP 1: Client sends request through instrumentation
        with unittest.mock.patch("opentelemetry.trace.get_tracer", return_value=mock_tracer), unittest.mock.patch.dict(
            "sys.modules", {"mcp.types": MagicMock(), "mcp": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "_generate_mcp_attributes"):
            # Override the setup tracer with the properly mocked one
            self.instrumentor.tracer = mock_tracer

            client_result = asyncio.run(
                self.instrumentor._wrap_send_request(e2e_system.client_send_request, None, (original_request,), {})
            )

        # Verify client side worked
        self.assertEqual(client_result["success"], True)
        self.assertIn("CLIENT: Preparing to send request", e2e_system.communication_log)

        # Get the request that was actually sent (with trace context injected)
        sent_request = e2e_system.last_sent_request
        sent_request_data = sent_request.model_dump()

        # Verify traceparent was injected by client instrumentation
        self.assertIn("_meta", sent_request_data["params"])
        self.assertIn("traceparent", sent_request_data["params"]["_meta"])

        # Parse and verify traceparent contains correct trace/span IDs
        traceparent = sent_request_data["params"]["_meta"]["traceparent"]
        parts = traceparent.split("-")
        self.assertEqual(int(parts[1], 16), 12345)  # trace_id
        self.assertEqual(int(parts[2], 16), 67890)  # span_id

        # STEP 2: Server receives the EXACT request that client sent
        # Create server request from the client's serialized data
        server_request = MCPServerRequest(sent_request_data)

        # Reset tracer mock for server side
        mock_tracer.reset_mock()

        # Server processes the request it received
        with unittest.mock.patch("opentelemetry.trace.get_tracer", return_value=mock_tracer), unittest.mock.patch.dict(
            "sys.modules", {"mcp.types": MagicMock(), "mcp": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "_generate_mcp_attributes"), unittest.mock.patch.object(
            self.instrumentor, "_get_mcp_operation", return_value="tools/create_metric"
        ):

            server_result = asyncio.run(
                self.instrumentor._wrap_handle_request(
                    e2e_system.server_handle_request, None, (None, server_request), {}
                )
            )

        # Verify server side worked
        self.assertEqual(server_result["success"], True)

        # Verify end-to-end trace context propagation
        self.assertIn("SERVER: Found trace context - trace_id: 12345, span_id: 67890", e2e_system.communication_log)

        # Verify the server received the exact same data the client sent
        self.assertEqual(server_request.params.name, "create_metric")
        self.assertEqual(server_request.params.arguments["name"], "cpu_usage")
        self.assertEqual(server_request.params.arguments["value"], 85)

        # Verify the traceparent made it through end-to-end
        self.assertIsNotNone(server_request.params.meta)
        self.assertIsNotNone(server_request.params.meta.traceparent)

        # Parse traceparent and verify trace/span IDs
        traceparent = server_request.params.meta.traceparent
        parts = traceparent.split("-")
        self.assertEqual(int(parts[1], 16), 12345)  # trace_id
        self.assertEqual(int(parts[2], 16), 67890)  # span_id

        # Verify complete communication flow
        expected_log_entries = [
            "CLIENT: Preparing to send request",
            "CLIENT: Sent",  # Part of the serialized request log
            "SERVER: Received request for create_metric",
            "SERVER: Found trace context - trace_id: 12345, span_id: 67890",
        ]

        for expected_entry in expected_log_entries:
            self.assertTrue(
                any(expected_entry in log_entry for log_entry in e2e_system.communication_log),
                f"Expected log entry '{expected_entry}' not found in: {e2e_system.communication_log}",
            )


class TestMCPInstrumentorEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for MCP instrumentor"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_invalid_traceparent_format(self) -> None:
        """Test handling of malformed traceparent headers"""
        invalid_formats = [
            "invalid-format",
            "00-invalid-hex-01",
            "00-12345-67890",  # Missing part
            "00-12345-67890-01-extra",  # Too many parts
            "",  # Empty string
        ]

        for invalid_format in invalid_formats:
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                result = self.instrumentor._extract_span_context_from_traceparent(invalid_format)
                self.assertIsNone(result, f"Should return None for invalid format: {invalid_format}")

    def test_version_import(self) -> None:
        """Test that version can be imported"""
        from amazon.opentelemetry.distro.instrumentation.mcp import version

        self.assertIsNotNone(version)

    def test_constants_import(self) -> None:
        """Test that constants can be imported"""
        from amazon.opentelemetry.distro.instrumentation.mcp.constants import MCPEnvironmentVariables

        self.assertIsNotNone(MCPEnvironmentVariables.SERVER_NAME)

    def test_add_client_attributes_default_server_name(self) -> None:
        """Test _add_client_attributes uses default server name"""
        mock_span = MagicMock()

        class MockRequest:
            def __init__(self) -> None:
                self.params = MockParams()

        class MockParams:
            def __init__(self) -> None:
                self.name = "test_tool"

        request = MockRequest()
        self.instrumentor._add_client_attributes(mock_span, "test_operation", request)

        # Verify default server name is used
        mock_span.set_attribute.assert_any_call("rpc.service", "mcp server")
        mock_span.set_attribute.assert_any_call("rpc.method", "test_operation")
        mock_span.set_attribute.assert_any_call("mcp.tool.name", "test_tool")

    def test_add_client_attributes_without_tool_name(self) -> None:
        """Test _add_client_attributes when request has no tool name"""
        mock_span = MagicMock()

        class MockRequestNoTool:
            def __init__(self) -> None:
                self.params = None

        request = MockRequestNoTool()
        self.instrumentor._add_client_attributes(mock_span, "test_operation", request)

        # Should still set service and method, but not tool name
        mock_span.set_attribute.assert_any_call("rpc.service", "mcp server")
        mock_span.set_attribute.assert_any_call("rpc.method", "test_operation")

    def test_add_server_attributes_without_tool_name(self) -> None:
        """Test _add_server_attributes when request has no tool name"""
        mock_span = MagicMock()

        class MockRequestNoTool:
            def __init__(self) -> None:
                self.params = None

        request = MockRequestNoTool()
        self.instrumentor._add_server_attributes(mock_span, "test_operation", request)

        # Should not set any attributes for server when no tool name
        mock_span.set_attribute.assert_not_called()

    def test_inject_trace_context_empty_request(self) -> None:
        """Test trace context injection with minimal request data"""
        request_data = {}
        span_ctx = SimpleSpanContext(trace_id=111, span_id=222)

        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Should create params and _meta structure
        self.assertIn("params", request_data)
        self.assertIn("_meta", request_data["params"])
        self.assertIn("traceparent", request_data["params"]["_meta"])

        # Verify traceparent format
        traceparent = request_data["params"]["_meta"]["traceparent"]
        parts = traceparent.split("-")
        self.assertEqual(len(parts), 4)
        self.assertEqual(int(parts[1], 16), 111)  # trace_id
        self.assertEqual(int(parts[2], 16), 222)  # span_id

    def test_uninstrument(self) -> None:
        """Test _uninstrument method removes instrumentation"""
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.unwrap"
        ) as mock_unwrap:
            self.instrumentor._uninstrument()

            # Verify both unwrap calls are made
            self.assertEqual(mock_unwrap.call_count, 2)
            mock_unwrap.assert_any_call("mcp.shared.session", "BaseSession.send_request")
            mock_unwrap.assert_any_call("mcp.server.lowlevel.server", "Server._handle_request")

    def test_extract_span_context_valid_traceparent(self) -> None:
        """Test _extract_span_context_from_traceparent with valid format"""
        # Use correct hex values: 12345 = 0x3039, 67890 = 0x10932
        valid_traceparent = "00-0000000000003039-0000000000010932-01"
        result = self.instrumentor._extract_span_context_from_traceparent(valid_traceparent)

        self.assertIsNotNone(result)
        self.assertEqual(result.trace_id, 12345)
        self.assertEqual(result.span_id, 67890)
        self.assertTrue(result.is_remote)

    def test_extract_span_context_value_error(self) -> None:
        """Test _extract_span_context_from_traceparent with invalid hex values"""
        invalid_hex_traceparent = "00-invalid-hex-values-01"
        result = self.instrumentor._extract_span_context_from_traceparent(invalid_hex_traceparent)

        self.assertIsNone(result)

    def test_instrument_method_coverage(self) -> None:
        """Test _instrument method registers hooks"""
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.register_post_import_hook"
        ) as mock_register:
            self.instrumentor._instrument()

            # Should register two hooks
            self.assertEqual(mock_register.call_count, 2)
