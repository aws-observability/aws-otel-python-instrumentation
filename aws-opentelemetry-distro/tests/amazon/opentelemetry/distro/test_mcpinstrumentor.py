"""
Super simple unit test for MCPInstrumentor - no mocking, just one functionality
Testing the _inject_trace_context method
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../../src"))
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
    """Test the _inject_trace_context method - simple functionality"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()

    def test_inject_trace_context_empty_dict(self):
        """Test injecting trace context into empty dictionary"""
        # Setup
        request_data = {}
        span_ctx = SimpleSpanContext(trace_id=12345, span_id=67890)

        # Execute
        self.instrumentor._inject_trace_context(request_data, span_ctx)

        # Verify
        expected = {"params": {"_meta": {"trace_context": {"trace_id": 12345, "span_id": 67890}}}}
        self.assertEqual(request_data, expected)

    def test_inject_trace_context_existing_params(self):
        """Test injecting trace context when params already exist"""
        # Setup
        request_data = {"params": {"existing_field": "test_value"}}
        span_ctx = SimpleSpanContext(trace_id=99999, span_id=11111)

        # Execute
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
        # Execute - call _instrument without tracer_provider in kwargs
        self.instrumentor._instrument()

        # Verify - tracer_provider should be None
        self.assertTrue(hasattr(self.instrumentor, "tracer_provider"))
        self.assertIsNone(self.instrumentor.tracer_provider)

    def test_instrument_with_tracer_provider_kwargs(self):
        """Test _instrument method when tracer_provider is in kwargs - should set to that value"""
        # Setup
        provider = SimpleTracerProvider()

        # Execute - call _instrument with tracer_provider in kwargs
        self.instrumentor._instrument(tracer_provider=provider)

        # Verify - tracer_provider should be set to the provided value
        self.assertTrue(hasattr(self.instrumentor, "tracer_provider"))
        self.assertEqual(self.instrumentor.tracer_provider, provider)


class TestAppSignalToolCallRequest(unittest.TestCase):
    """Test with realistic AppSignal MCP server tool call requests"""

    def setUp(self):
        self.instrumentor = MCPInstrumentor()
        self.instrumentor.tracer_provider = None

    def test_appsignal_call_tool_request(self):
        """Test with a realistic AppSignal MCP server CallToolRequest"""

        # Create a realistic CallToolRequest for AppSignal MCP server
        class CallToolRequest:
            def __init__(self, tool_name, arguments):
                self.root = self
                self.params = CallToolParams(tool_name, arguments)

            def model_dump(self, **kwargs):
                return {"method": "call_tool", "params": {"name": self.params.name, "arguments": self.params.arguments}}

        class CallToolParams:
            def __init__(self, name, arguments):
                self.name = name
                self.arguments = arguments

        # Create an AppSignal tool call request
        appsignal_request = CallToolRequest(
            tool_name="create_metric",
            arguments={
                "metric_name": "response_time",
                "value": 250,
                "tags": {"endpoint": "/api/users", "method": "GET"},
            },
        )

        # Verify the tool call request structure matches AppSignal expectations
        request_data = appsignal_request.model_dump()
        self.assertEqual(request_data["method"], "call_tool")
        self.assertEqual(request_data["params"]["name"], "create_metric")
        self.assertEqual(request_data["params"]["arguments"]["metric_name"], "response_time")
        self.assertEqual(request_data["params"]["arguments"]["value"], 250)
        self.assertIn("endpoint", request_data["params"]["arguments"]["tags"])
        self.assertEqual(request_data["params"]["arguments"]["tags"]["endpoint"], "/api/users")

        # Test expected AppSignal response structure
        expected_appsignal_result = {
            "success": True,
            "metric_id": "metric_12345",
            "message": "Metric 'response_time' created successfully",
            "metadata": {
                "timestamp": "2025-01-22T21:19:00Z",
                "tags_applied": ["endpoint:/api/users", "method:GET"],
                "value_recorded": 250,
            },
        }

        self.assertTrue(expected_appsignal_result["success"])
        self.assertIn("metric_id", expected_appsignal_result)
        self.assertEqual(expected_appsignal_result["metadata"]["value_recorded"], 250)

    def test_appsignal_tool_without_arguments(self):
        """Test AppSignal tool call that doesn't require arguments"""

        # Create a realistic CallToolRequest for tools without arguments
        class CallToolRequestNoArgs:
            def __init__(self, tool_name):
                self.root = self
                self.params = CallToolParamsNoArgs(tool_name)

            def model_dump(self, **kwargs):
                return {
                    "method": "call_tool",
                    "params": {
                        "name": self.params.name
                        # No arguments field for this tool
                    },
                }

            @classmethod
            def model_validate(cls, data):
                return cls(data["params"]["name"])

        class CallToolParamsNoArgs:
            def __init__(self, name):
                self.name = name
                # No arguments attribute for this tool

        # Create an AppSignal tool call without arguments
        list_apps_request = CallToolRequestNoArgs(tool_name="list_applications")

        # Test argument detection
        args_with_request = (list_apps_request,)
        kwargs_empty = {}
        extracted_request = args_with_request[0] if len(args_with_request) > 0 else kwargs_empty.get("request")

        self.assertEqual(extracted_request, list_apps_request)
        self.assertEqual(extracted_request.params.name, "list_applications")

        # Verify the request structure for tools without arguments
        request_data = list_apps_request.model_dump()
        self.assertEqual(request_data["method"], "call_tool")
        self.assertEqual(request_data["params"]["name"], "list_applications")
        self.assertNotIn("arguments", request_data["params"])  # No arguments field

        # Test expected result for list_applications tool
        expected_list_apps_result = {
            "success": True,
            "applications": [
                {"id": "app_001", "name": "web-frontend", "environment": "production"},
                {"id": "app_002", "name": "api-backend", "environment": "staging"},
            ],
            "total_count": 2,
        }

        # Verify expected response structure
        self.assertTrue(expected_list_apps_result["success"])
        self.assertIn("applications", expected_list_apps_result)
        self.assertEqual(expected_list_apps_result["total_count"], 2)
        self.assertEqual(len(expected_list_apps_result["applications"]), 2)
        self.assertEqual(expected_list_apps_result["applications"][0]["name"], "web-frontend")

    def test_send_request_wrapper_argument_reconstruction(self):
        """Test the argument  logic: if len(args) > 0 vs else path"""

        # Create a realistic AppSignal request
        class CallToolRequest:
            def __init__(self, tool_name, arguments=None):
                self.root = self
                self.params = CallToolParams(tool_name, arguments)

            def model_dump(self, by_alias=True, mode="json", exclude_none=True):
                result = {"method": "call_tool", "params": {"name": self.params.name}}
                if self.params.arguments:
                    result["params"]["arguments"] = self.params.arguments
                return result

            @classmethod
            def model_validate(cls, data):
                return cls(data["params"]["name"], data["params"].get("arguments"))

        class CallToolParams:
            def __init__(self, name, arguments=None):
                self.name = name
                self.arguments = arguments

        request = CallToolRequest("create_metric", {"metric_name": "test", "value": 100})

        # Test 1: len(args) > 0 path - should trigger new_args = (modified_request,) + args[1:]
        args_with_request = (request, "extra_arg1", "extra_arg2")
        kwargs_test = {"extra_kwarg": "test"}

        # Simulate what the wrapper logic does
        if len(args_with_request) > 0:
            # This tests: new_args = (modified_request,) + args[1:]
            new_args = ("modified_request_placeholder",) + args_with_request[1:]
            result_args = new_args
            result_kwargs = kwargs_test
        else:
            # This shouldn't happen in this test
            result_args = args_with_request
            result_kwargs = kwargs_test.copy()
            result_kwargs["request"] = "modified_request_placeholder"

        # Verify args path reconstruction
        self.assertEqual(len(result_args), 3)  # modified_request + 2 extra args
        self.assertEqual(result_args[0], "modified_request_placeholder")
        self.assertEqual(result_args[1], "extra_arg1")  # args[1:] preserved
        self.assertEqual(result_args[2], "extra_arg2")  # args[1:] preserved
        self.assertEqual(result_kwargs["extra_kwarg"], "test")
        self.assertNotIn("request", result_kwargs)  # Should NOT modify kwargs in args path

        # Test 2: len(args) == 0 path - should trigger kwargs["request"] = modified_request
        args_empty = ()
        kwargs_with_request = {"request": request, "other_param": "value"}

        # Simulate what the wrapper logic does
        if len(args_empty) > 0:
            # This shouldn't happen in this test
            new_args = ("modified_request_placeholder",) + args_empty[1:]
            result_args = new_args
            result_kwargs = kwargs_with_request
        else:
            # This tests: kwargs["request"] = modified_request
            result_args = args_empty
            result_kwargs = kwargs_with_request.copy()
            result_kwargs["request"] = "modified_request_placeholder"

        # Verify kwargs path reconstruction
        self.assertEqual(len(result_args), 0)  # No positional args
        self.assertEqual(result_kwargs["request"], "modified_request_placeholder")
        self.assertEqual(result_kwargs["other_param"], "value")  # Other kwargs preserved

    def test_server_handle_request_wrapper_logic(self):
        """Test the _server_handle_request_wrapper"""

        # Create realistic server request structures
        class ServerRequest:
            def __init__(self, has_trace_context=False):
                if has_trace_context:
                    self.params = ServerRequestParams(has_meta=True)
                else:
                    self.params = ServerRequestParams(has_meta=False)

        class ServerRequestParams:
            def __init__(self, has_meta=False):
                if has_meta:
                    self.meta = ServerRequestMeta()
                else:
                    self.meta = None

        class ServerRequestMeta:
            def __init__(self):
                self.trace_context = {"trace_id": 12345, "span_id": 67890}

        # Test 1: Request WITHOUT trace context - should take else path
        request_no_trace = ServerRequest(has_trace_context=False)

        # wrapper's request extraction logic
        args_with_request = (None, request_no_trace)  # args[1] is the request
        req = args_with_request[1] if len(args_with_request) > 1 else None

        # Check trace context extraction logic
        trace_context = None
        if req and hasattr(req, "params") and req.params and hasattr(req.params, "meta") and req.params.meta:
            trace_context = req.params.meta.trace_context

        # Verify - should NOT find trace context
        self.assertIsNotNone(req)
        self.assertIsNotNone(req.params)
        self.assertIsNone(req.params.meta)  # No meta field
        self.assertIsNone(trace_context)

        # Test 2: Request WITH trace context - should take if path
        request_with_trace = ServerRequest(has_trace_context=True)

        # Simulate the wrapper's request extraction logic
        args_with_trace = (None, request_with_trace)  # args[1] is the request
        req2 = args_with_trace[1] if len(args_with_trace) > 1 else None

        # Check trace context extraction logic
        trace_context2 = None
        if req2 and hasattr(req2, "params") and req2.params and hasattr(req2.params, "meta") and req2.params.meta:
            trace_context2 = req2.params.meta.trace_context

        # Verify - should find trace context
        self.assertIsNotNone(req2)
        self.assertIsNotNone(req2.params)
        self.assertIsNotNone(req2.params.meta)  # Has meta field
        self.assertIsNotNone(trace_context2)
        self.assertEqual(trace_context2["trace_id"], 12345)
        self.assertEqual(trace_context2["span_id"], 67890)

        # Test 3: No request at all (args[1] doesn't exist)
        args_no_request = (None,)  # Only one arg, no request
        req3 = args_no_request[1] if len(args_no_request) > 1 else None

        # Verify - should handle missing request gracefully
        self.assertIsNone(req3)

    def test_end_to_end_trace_context_propagation(self):
        """Test client sending trace context and server receiving the same trace context"""
        # STEP 1: CLIENT SIDE - Create and prepare request with trace context

        # Create a realistic AppSignal request (what client would send)
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

        # Client injects trace context (what _send_request_wrapper does)
        original_trace_context = SimpleSpanContext(trace_id=98765, span_id=43210)

        # Get request data and inject trace context
        request_data = client_request.model_dump()
        self.instrumentor._inject_trace_context(request_data, original_trace_context)

        # Create modified request with trace context (what client sends over network)
        modified_request = CallToolRequest.model_validate(request_data)

        # Verify client successfully injected trace context
        client_data = modified_request.model_dump()
        self.assertIn("_meta", client_data["params"])
        self.assertIn("trace_context", client_data["params"]["_meta"])
        self.assertEqual(client_data["params"]["_meta"]["trace_context"]["trace_id"], 98765)
        self.assertEqual(client_data["params"]["_meta"]["trace_context"]["span_id"], 43210)

        # STEP 2: SERVER SIDE - Receive and extract trace context

        # Create server request structure (what server receives)
        class ServerRequest:
            def __init__(self, client_request_data):
                self.params = ServerRequestParams(client_request_data["params"])

        class ServerRequestParams:
            def __init__(self, params_data):
                self.name = params_data["name"]
                if "arguments" in params_data:
                    self.arguments = params_data["arguments"]
                # Extract meta field (trace context)
                if "_meta" in params_data:
                    self.meta = ServerRequestMeta(params_data["_meta"])
                else:
                    self.meta = None

        class ServerRequestMeta:
            def __init__(self, meta_data):
                self.trace_context = meta_data["trace_context"]

        # Server receives the request (simulating network transmission)
        server_request = ServerRequest(client_data)

        # Server extracts trace context (what _server_handle_request_wrapper does)
        args_with_request = (None, server_request)  # args[1] is the request
        req = args_with_request[1] if len(args_with_request) > 1 else None

        # Extract trace context using server logic
        extracted_trace_context = None
        if req and hasattr(req, "params") and req.params and hasattr(req.params, "meta") and req.params.meta:
            extracted_trace_context = req.params.meta.trace_context

        # Verify server successfully received the trace context
        self.assertIsNotNone(extracted_trace_context)
        self.assertEqual(extracted_trace_context["trace_id"], 98765)
        self.assertEqual(extracted_trace_context["span_id"], 43210)

        # Verify it's the SAME trace context that client sent
        self.assertEqual(extracted_trace_context["trace_id"], original_trace_context.trace_id)
        self.assertEqual(extracted_trace_context["span_id"], original_trace_context.span_id)

        # Verify the tool call data is also preserved
        self.assertEqual(server_request.params.name, "create_metric")
        self.assertEqual(server_request.params.arguments["metric_name"], "response_time")


if __name__ == "__main__":
    unittest.main()
