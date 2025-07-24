"""
Unit tests for MCPInstrumentor - testing actual mcpinstrumentor methods
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import MagicMock

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
src_path = os.path.join(project_root, "src")
sys.path.insert(0, src_path)
from amazon.opentelemetry.distro.mcpinstrumentor.mcpinstrumentor import MCPInstrumentor  # noqa: E402


class SimpleSpanContext:
    """Simple mock span context without using MagicMock"""

    def __init__(self, trace_id, span_id):
        self.trace_id = trace_id
        self.span_id = span_id


class SimpleTracerProvider:
    """Simple mock tracer provider without using MagicMock"""

    def __init__(self):
        self.get_tracer_called = False
        self.tracer_name = None

    def get_tracer(self, name):
        self.get_tracer_called = True
        self.tracer_name = name
        return "mock_tracer_from_provider"


class TestInjectTraceContext(unittest.TestCase):
    """Test the _inject_trace_context method"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()

    def test_inject_trace_context_empty_dict(self):
        """Test injecting trace context into empty dictionary"""
        # Setup
        request_data = {}
        span_ctx = SimpleSpanContext(trace_id=12345, span_id=67890)

        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Verify
        expected = {"params": {"_meta": {"trace_context": {"trace_id": 12345, "span_id": 67890}}}}
        self.assertEqual(request_data, expected)

    def test_inject_trace_context_existing_params(self):
        """Test injecting trace context when params already exist"""
        # Setup
        request_data = {"params": {"existing_field": "test_value"}}
        span_ctx = SimpleSpanContext(trace_id=99999, span_id=11111)

        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Verify the existing field is preserved and trace context is added
        self.assertEqual(request_data["params"]["existing_field"], "test_value")
        self.assertEqual(request_data["params"]["_meta"]["trace_context"]["trace_id"], 99999)
        self.assertEqual(request_data["params"]["_meta"]["trace_context"]["span_id"], 11111)


class TestTracerProvider(unittest.TestCase):
    """Test the tracer provider kwargs logic in _instrument method"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()
        # Reset tracer_provider to ensure test isolation
        if hasattr(self.instrumentor, "tracer_provider"):
            delattr(self.instrumentor, "tracer_provider")

    def test_instrument_without_tracer_provider_kwargs(self):
        """Test _instrument method when no tracer_provider in kwargs - should set to None"""
        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._instrument()

        # Verify - tracer_provider should be None
        self.assertTrue(hasattr(self.instrumentor, "tracer_provider"))
        self.assertIsNone(self.instrumentor.tracer_provider)

    def test_instrument_with_tracer_provider_kwargs(self):
        """Test _instrument method when tracer_provider is in kwargs - should set to that value"""
        # Setup
        provider = SimpleTracerProvider()

        # Execute - Actually test the mcpinstrumentor method
        self.instrumentor._instrument(tracer_provider=provider)

        # Verify - tracer_provider should be set to the provided value
        self.assertTrue(hasattr(self.instrumentor, "tracer_provider"))
        self.assertEqual(self.instrumentor.tracer_provider, provider)


class TestInstrumentationDependencies(unittest.TestCase):
    """Test the instrumentation_dependencies method"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()

    def test_instrumentation_dependencies(self):
        """Test that instrumentation_dependencies method returns the expected dependencies"""
        # Execute - Actually test the mcpinstrumentor method
        dependencies = self.instrumentor.instrumentation_dependencies()

        # Verify - should return the _instruments collection
        self.assertIsNotNone(dependencies)
        # The dependencies come from openinference.instrumentation.mcp.package._instruments
        # which should be a collection


class TestTraceContextInjection(unittest.TestCase):
    """Test trace context injection using actual mcpinstrumentor methods"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()

    def test_trace_context_injection_with_realistic_request(self):
        """Test actual trace context injection using mcpinstrumentor._inject_trace_context with realistic MCP request"""

        # Create a realistic MCP request structure
        class CallToolRequest:
            def __init__(self, tool_name, arguments=None):
                self.root = self
                self.params = CallToolParams(tool_name, arguments)

            def model_dump(self, by_alias=True, mode="json", exclude_none=True):
                result = {"method": "call_tool", "params": {"name": self.params.name}}
                if self.params.arguments:
                    result["params"]["arguments"] = self.params.arguments
                # Include _meta if it exists (trace context injection point)
                if hasattr(self.params, "_meta") and self.params._meta:
                    result["params"]["_meta"] = self.params._meta
                return result

            @classmethod
            def model_validate(cls, data):
                instance = cls(data["params"]["name"], data["params"].get("arguments"))
                # Restore _meta field if present
                if "_meta" in data["params"]:
                    instance.params._meta = data["params"]["_meta"]
                return instance

        class CallToolParams:
            def __init__(self, name, arguments=None):
                self.name = name
                self.arguments = arguments
                self._meta = None  # Will hold trace context

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
        self.assertIn("trace_context", client_data["params"]["_meta"])
        self.assertEqual(client_data["params"]["_meta"]["trace_context"]["trace_id"], 98765)
        self.assertEqual(client_data["params"]["_meta"]["trace_context"]["span_id"], 43210)

        # Verify the tool call data is also preserved
        self.assertEqual(client_data["params"]["name"], "create_metric")
        self.assertEqual(client_data["params"]["arguments"]["metric_name"], "response_time")


class TestInstrumentedMCPServer(unittest.TestCase):
    """Test mcpinstrumentor with a mock MCP server to verify end-to-end functionality"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()
        self.instrumentor.tracer_provider = None

    def test_no_trace_context_fallback(self):
        """Test graceful handling when no trace context is present on server side"""

        class MockServerNoTrace:
            async def _handle_request(self, session, request):
                return {"success": True, "handled_without_trace": True}

        class MockServerRequestNoTrace:
            def __init__(self, tool_name):
                self.params = MockServerRequestParamsNoTrace(tool_name)

        class MockServerRequestParamsNoTrace:
            def __init__(self, name):
                self.name = name
                self.meta = None  # No trace context

        mock_server = MockServerNoTrace()
        server_request = MockServerRequestNoTrace("create_metric")

        # Setup mocks
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        mock_tracer.start_as_current_span.return_value.__exit__.return_value = None

        # Test server handling without trace context (fallback scenario)
        with unittest.mock.patch("opentelemetry.trace.get_tracer", return_value=mock_tracer), unittest.mock.patch.dict(
            "sys.modules", {"mcp.types": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "handle_attributes"), unittest.mock.patch.object(
            self.instrumentor, "_get_span_name", return_value="tools/create_metric"
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

    def test_end_to_end_client_server_communication(self):
        """Test where server actually receives what client sends (including injected trace context)"""

        # Create realistic request/response classes
        class MCPRequest:
            def __init__(self, tool_name, arguments=None, method="call_tool"):
                self.root = self
                self.params = MCPRequestParams(tool_name, arguments)
                self.method = method

            def model_dump(self, by_alias=True, mode="json", exclude_none=True):
                result = {"method": self.method, "params": {"name": self.params.name}}
                if self.params.arguments:
                    result["params"]["arguments"] = self.params.arguments
                # Include _meta if it exists (for trace context)
                if hasattr(self.params, "_meta") and self.params._meta:
                    result["params"]["_meta"] = self.params._meta
                return result

            @classmethod
            def model_validate(cls, data):
                method = data.get("method", "call_tool")
                instance = cls(data["params"]["name"], data["params"].get("arguments"), method)
                # Restore _meta field if present
                if "_meta" in data["params"]:
                    instance.params._meta = data["params"]["_meta"]
                return instance

        class MCPRequestParams:
            def __init__(self, name, arguments=None):
                self.name = name
                self.arguments = arguments
                self._meta = None

        class MCPServerRequest:
            def __init__(self, client_request_data):
                """Server request created from client's serialized data"""
                self.method = client_request_data.get("method", "call_tool")
                self.params = MCPServerRequestParams(client_request_data["params"])

        class MCPServerRequestParams:
            def __init__(self, params_data):
                self.name = params_data["name"]
                self.arguments = params_data.get("arguments")
                # Extract trace context from _meta if present
                if "_meta" in params_data and "trace_context" in params_data["_meta"]:
                    self.meta = MCPServerRequestMeta(params_data["_meta"]["trace_context"])
                else:
                    self.meta = None

        class MCPServerRequestMeta:
            def __init__(self, trace_context):
                self.trace_context = trace_context

        # Mock client and server that actually communicate
        class EndToEndMCPSystem:
            def __init__(self):
                self.communication_log = []
                self.last_sent_request = None

            async def client_send_request(self, request):
                """Client sends request - captures what gets sent"""
                self.communication_log.append("CLIENT: Preparing to send request")
                self.last_sent_request = request  # Capture the modified request

                # Simulate sending over network - serialize the request
                serialized_request = request.model_dump()
                self.communication_log.append(f"CLIENT: Sent {serialized_request}")

                # Return client response
                return {"success": True, "client_response": "Request sent successfully"}

            async def server_handle_request(self, session, server_request):
                """Server handles the request it received"""
                self.communication_log.append(f"SERVER: Received request for {server_request.params.name}")

                # Check if trace context was received
                if server_request.params.meta and server_request.params.meta.trace_context:
                    trace_info = server_request.params.meta.trace_context
                    self.communication_log.append(
                        f"SERVER: Found trace context - trace_id: {trace_info['trace_id']}, "
                        f"span_id: {trace_info['span_id']}"
                    )
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
            "sys.modules", {"mcp.types": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "handle_attributes"):

            client_result = asyncio.run(
                self.instrumentor._wrap_send_request(e2e_system.client_send_request, None, (original_request,), {})
            )

        # Verify client side worked
        self.assertEqual(client_result["success"], True)
        self.assertIn("CLIENT: Preparing to send request", e2e_system.communication_log)

        # Get the request that was actually sent (with trace context injected)
        sent_request = e2e_system.last_sent_request
        sent_request_data = sent_request.model_dump()

        # Verify trace context was injected by client instrumentation
        self.assertIn("_meta", sent_request_data["params"])
        self.assertIn("trace_context", sent_request_data["params"]["_meta"])
        self.assertEqual(sent_request_data["params"]["_meta"]["trace_context"]["trace_id"], 12345)
        self.assertEqual(sent_request_data["params"]["_meta"]["trace_context"]["span_id"], 67890)

        # STEP 2: Server receives the EXACT request that client sent
        # Create server request from the client's serialized data
        server_request = MCPServerRequest(sent_request_data)

        # Reset tracer mock for server side
        mock_tracer.reset_mock()

        # Server processes the request it received
        with unittest.mock.patch("opentelemetry.trace.get_tracer", return_value=mock_tracer), unittest.mock.patch.dict(
            "sys.modules", {"mcp.types": MagicMock()}
        ), unittest.mock.patch.object(self.instrumentor, "handle_attributes"), unittest.mock.patch.object(
            self.instrumentor, "_get_span_name", return_value="tools/create_metric"
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

        # Verify the trace context made it through end-to-end
        self.assertIsNotNone(server_request.params.meta)
        self.assertIsNotNone(server_request.params.meta.trace_context)
        self.assertEqual(server_request.params.meta.trace_context["trace_id"], 12345)
        self.assertEqual(server_request.params.meta.trace_context["span_id"], 67890)

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


if __name__ == "__main__":
    unittest.main()
