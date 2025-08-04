# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for MCPInstrumentor - testing actual mcpinstrumentor methods
"""

import asyncio
import unittest
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.instrumentation.mcp import version
from amazon.opentelemetry.distro.instrumentation.mcp.constants import MCPEnvironmentVariables
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
        self.assertIsNotNone(version)

    def test_constants_import(self) -> None:
        """Test that constants can be imported"""
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


class TestGenerateMCPAttributes(unittest.TestCase):
    """Test _generate_mcp_attributes method with mocked imports"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_generate_attributes_with_mock_types(self) -> None:
        """Test _generate_mcp_attributes with mocked MCP types"""
        mock_span = MagicMock()

        class MockRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "test_tool"

        request = MockRequest()

        # Mock the isinstance checks to avoid importing mcp.types
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance"
        ) as mock_isinstance:
            mock_isinstance.side_effect = lambda obj, cls: False  # No matches

            self.instrumentor._generate_mcp_attributes(mock_span, request, True)

            # Should call _add_client_attributes since is_client=True
            self.assertTrue(mock_isinstance.called)


class TestGetMCPOperation(unittest.TestCase):
    """Test _get_mcp_operation method with mocked imports"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_get_operation_with_mock_types(self) -> None:
        """Test _get_mcp_operation with mocked MCP types"""

        class MockRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "test_tool"

        request = MockRequest()

        # Mock the isinstance checks to return unknown
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance"
        ) as mock_isinstance:
            mock_isinstance.side_effect = lambda obj, cls: False  # No matches

            result = self.instrumentor._get_mcp_operation(request)

            self.assertEqual(result, "unknown")


class TestWrapHandleRequestEdgeCases(unittest.TestCase):
    """Test _wrap_handle_request edge cases"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        mock_tracer.start_as_current_span.return_value.__exit__.return_value = None
        self.instrumentor.tracer = mock_tracer

    def test_wrap_handle_request_no_request(self) -> None:
        """Test _wrap_handle_request when no request in args"""

        async def mock_wrapped(*args, **kwargs):
            return {"result": "no_request"}

        result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session",), {}))

        self.assertEqual(result["result"], "no_request")

    def test_wrap_handle_request_no_params(self) -> None:
        """Test _wrap_handle_request when request has no params"""

        class MockRequestNoParams:
            def __init__(self):
                self.params = None

        async def mock_wrapped(*args, **kwargs):
            return {"result": "no_params"}

        request = MockRequestNoParams()
        result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session", request), {}))

        self.assertEqual(result["result"], "no_params")

    def test_wrap_handle_request_no_meta(self) -> None:
        """Test _wrap_handle_request when request params has no meta"""

        class MockRequestNoMeta:
            def __init__(self):
                self.params = MockParamsNoMeta()

        class MockParamsNoMeta:
            def __init__(self):
                self.meta = None

        async def mock_wrapped(*args, **kwargs):
            return {"result": "no_meta"}

        request = MockRequestNoMeta()
        result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session", request), {}))

        self.assertEqual(result["result"], "no_meta")

    def test_wrap_handle_request_with_valid_traceparent(self) -> None:
        """Test _wrap_handle_request with valid traceparent"""

        class MockRequestWithTrace:
            def __init__(self):
                self.params = MockParamsWithTrace()

        class MockParamsWithTrace:
            def __init__(self):
                self.meta = MockMeta()

        class MockMeta:
            def __init__(self):
                self.traceparent = "00-0000000000003039-0000000000010932-01"

        async def mock_wrapped(*args, **kwargs):
            return {"result": "with_trace"}

        request = MockRequestWithTrace()

        with unittest.mock.patch.object(self.instrumentor, "_generate_mcp_attributes"), unittest.mock.patch.object(
            self.instrumentor, "_get_mcp_operation", return_value="tools/test"
        ):
            result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session", request), {}))

        self.assertEqual(result["result"], "with_trace")


class TestInstrumentorStaticMethods(unittest.TestCase):
    """Test static methods of MCPInstrumentor"""

    @staticmethod
    def test_instrumentation_dependencies_static() -> None:
        """Test instrumentation_dependencies as static method"""
        deps = MCPInstrumentor.instrumentation_dependencies()
        assert "mcp >= 1.6.0" in deps

    @staticmethod
    def test_uninstrument_static() -> None:
        """Test _uninstrument as static method"""
        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.unwrap"
        ) as mock_unwrap:
            MCPInstrumentor._uninstrument()

            assert mock_unwrap.call_count == 2
            mock_unwrap.assert_any_call("mcp.shared.session", "BaseSession.send_request")
            mock_unwrap.assert_any_call("mcp.server.lowlevel.server", "Server._handle_request")


class TestMCPInstrumentorMissingCoverage(unittest.TestCase):
    """Tests targeting specific uncovered lines in MCPInstrumentor"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_generate_mcp_attributes_list_tools_server_side(self) -> None:
        """Test _generate_mcp_attributes for ListToolsRequest on server side"""
        mock_span = MagicMock()

        class MockListToolsRequest:
            pass

        request = MockListToolsRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "ListToolsRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].ListToolsRequest = MockListToolsRequest

                self.instrumentor._generate_mcp_attributes(mock_span, request, False)

                mock_span.set_attribute.assert_called_with("mcp.list_tools", True)
                mock_span.update_name.assert_not_called()

    def test_generate_mcp_attributes_initialize_server_side(self) -> None:
        """Test _generate_mcp_attributes for InitializeRequest on server side"""
        mock_span = MagicMock()

        class MockInitializeRequest:
            pass

        request = MockInitializeRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "InitializeRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].InitializeRequest = MockInitializeRequest

                self.instrumentor._generate_mcp_attributes(mock_span, request, False)

                mock_span.set_attribute.assert_called_with("notifications/initialize", True)
                mock_span.update_name.assert_not_called()

    def test_generate_mcp_attributes_call_tool_server_side(self) -> None:
        """Test _generate_mcp_attributes for CallToolRequest on server side"""
        mock_span = MagicMock()

        class MockCallToolRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "server_tool"

        request = MockCallToolRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "CallToolRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].CallToolRequest = MockCallToolRequest

                self.instrumentor._generate_mcp_attributes(mock_span, request, False)

                # Should set both mcp.call_tool and mcp.tool.name attributes
                mock_span.set_attribute.assert_any_call("mcp.call_tool", True)
                mock_span.set_attribute.assert_any_call("mcp.tool.name", "server_tool")
                mock_span.update_name.assert_not_called()

    def test_get_mcp_operation_list_tools_request(self) -> None:
        """Test _get_mcp_operation for ListToolsRequest"""

        class MockListToolsRequest:
            pass

        request = MockListToolsRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "ListToolsRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].ListToolsRequest = MockListToolsRequest

                result = self.instrumentor._get_mcp_operation(request)

                self.assertEqual(result, "tools/list")

    def test_get_mcp_operation_call_tool_request(self) -> None:
        """Test _get_mcp_operation for CallToolRequest"""

        class MockCallToolRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "test_tool"

        request = MockCallToolRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "CallToolRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].CallToolRequest = MockCallToolRequest

                result = self.instrumentor._get_mcp_operation(request)

                self.assertEqual(result, "tools/test_tool")

    def test_add_client_attributes_with_params_but_no_name(self) -> None:
        """Test _add_client_attributes when params exists but has no name"""
        mock_span = MagicMock()

        class MockRequest:
            def __init__(self):
                self.params = MockParamsNoName()

        class MockParamsNoName:
            def __init__(self):
                self.other_field = "value"

        request = MockRequest()
        self.instrumentor._add_client_attributes(mock_span, "test_op", request)

        mock_span.set_attribute.assert_any_call("rpc.service", "mcp server")
        mock_span.set_attribute.assert_any_call("rpc.method", "test_op")
        self.assertEqual(mock_span.set_attribute.call_count, 2)

    def test_add_server_attributes_with_params_but_no_name(self) -> None:
        """Test _add_server_attributes when params exists but has no name"""
        mock_span = MagicMock()

        class MockRequest:
            def __init__(self):
                self.params = MockParamsNoName()

        class MockParamsNoName:
            def __init__(self):
                self.other_field = "value"

        request = MockRequest()
        self.instrumentor._add_server_attributes(mock_span, "test_op", request)

        mock_span.set_attribute.assert_not_called()

    def test_extract_span_context_with_wrong_part_count(self) -> None:
        """Test _extract_span_context_from_traceparent with wrong number of parts"""
        result = self.instrumentor._extract_span_context_from_traceparent("00-12345")
        self.assertIsNone(result)

        result = self.instrumentor._extract_span_context_from_traceparent("00-12345-67890-01-extra")
        self.assertIsNone(result)

    def test_wrap_handle_request_with_hasattr_false(self) -> None:
        """Test _wrap_handle_request when hasattr returns False for meta"""

        class MockRequestNoMetaAttr:
            def __init__(self):
                self.params = MockParamsNoMetaAttr()

        class MockParamsNoMetaAttr:
            def __init__(self):
                pass

        async def mock_wrapped(*args, **kwargs):
            return {"result": "no_meta_attr"}

        request = MockRequestNoMetaAttr()
        result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session", request), {}))

        self.assertEqual(result["result"], "no_meta_attr")

    def test_add_client_attributes_missing_params_attribute(self) -> None:
        """Test _add_client_attributes when request has no params attribute"""
        mock_span = MagicMock()

        class MockRequestNoParams:
            pass

        request = MockRequestNoParams()
        self.instrumentor._add_client_attributes(mock_span, "test_op", request)

        mock_span.set_attribute.assert_any_call("rpc.service", "mcp server")
        mock_span.set_attribute.assert_any_call("rpc.method", "test_op")

    def test_add_server_attributes_missing_params_attribute(self) -> None:
        """Test _add_server_attributes when request has no params attribute"""
        mock_span = MagicMock()

        class MockRequestNoParams:
            pass

        request = MockRequestNoParams()
        self.instrumentor._add_server_attributes(mock_span, "test_op", request)

        mock_span.set_attribute.assert_not_called()

    def test_inject_trace_context_existing_meta(self) -> None:
        """Test _inject_trace_context when _meta already exists"""
        request_data = {"params": {"_meta": {"existing_field": "should_be_preserved"}}}
        span_ctx = SimpleSpanContext(trace_id=123, span_id=456)

        self.instrumentor._inject_trace_context(request_data, span_ctx)

        self.assertEqual(request_data["params"]["_meta"]["existing_field"], "should_be_preserved")
        self.assertIn("traceparent", request_data["params"]["_meta"])

    def test_extract_span_context_zero_values(self) -> None:
        """Test _extract_span_context_from_traceparent with zero values"""
        result = self.instrumentor._extract_span_context_from_traceparent(
            "00-00000000000000000000000000000000-1234567890123456-01"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.trace_id, 0)
        self.assertEqual(result.span_id, 0x1234567890123456)

        result = self.instrumentor._extract_span_context_from_traceparent(
            "00-12345678901234567890123456789012-0000000000000000-01"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.trace_id, 0x12345678901234567890123456789012)
        self.assertEqual(result.span_id, 0)


class TestMCPCoverage(unittest.TestCase):
    """Essential tests for missing coverage"""

    def setUp(self) -> None:
        self.instrumentor = MCPInstrumentor()

    def test_generate_mcp_attributes_server_side(self) -> None:
        """Test server-side MCP attribute generation"""
        mock_span = MagicMock()

        class MockCallToolRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "server_tool"

        request = MockCallToolRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "CallToolRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].CallToolRequest = MockCallToolRequest

                self.instrumentor._generate_mcp_attributes(mock_span, request, False)

                # Should set both mcp.call_tool and mcp.tool.name attributes
                mock_span.set_attribute.assert_any_call("mcp.call_tool", True)
                mock_span.set_attribute.assert_any_call("mcp.tool.name", "server_tool")
                mock_span.update_name.assert_not_called()

    def test_get_mcp_operation_list_tools(self) -> None:
        """Test _get_mcp_operation for ListToolsRequest"""

        class MockListToolsRequest:
            pass

        request = MockListToolsRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "ListToolsRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].ListToolsRequest = MockListToolsRequest

                result = self.instrumentor._get_mcp_operation(request)
                self.assertEqual(result, "tools/list")

    def test_get_mcp_operation_call_tool(self) -> None:
        """Test _get_mcp_operation for CallToolRequest"""

        class MockCallToolRequest:
            def __init__(self):
                self.params = MockParams()

        class MockParams:
            def __init__(self):
                self.name = "test_tool"

        request = MockCallToolRequest()

        def mock_isinstance(obj, cls):
            return cls.__name__ == "CallToolRequest"

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.instrumentation.mcp.mcp_instrumentor.isinstance", side_effect=mock_isinstance
        ):
            with unittest.mock.patch.dict("sys.modules", {"mcp.types": MagicMock()}):
                import sys

                sys.modules["mcp.types"].CallToolRequest = MockCallToolRequest

                result = self.instrumentor._get_mcp_operation(request)
                self.assertEqual(result, "tools/test_tool")

    def test_add_attributes_edge_cases(self) -> None:
        """Test attribute setting edge cases"""
        mock_span = MagicMock()

        # Test client attributes with no params
        class MockRequestNoParams:
            def __init__(self):
                self.params = None

        request = MockRequestNoParams()
        self.instrumentor._add_client_attributes(mock_span, "test_op", request)
        mock_span.set_attribute.assert_any_call("rpc.service", "mcp server")
        mock_span.set_attribute.assert_any_call("rpc.method", "test_op")

        # Test server attributes with no params
        mock_span.reset_mock()
        self.instrumentor._add_server_attributes(mock_span, "test_op", request)
        mock_span.set_attribute.assert_not_called()

    def test_extract_span_context_edge_cases(self) -> None:
        """Test span context extraction edge cases"""
        # Test wrong part count
        result = self.instrumentor._extract_span_context_from_traceparent("00-12345")
        self.assertIsNone(result)

        # Test invalid hex
        result = self.instrumentor._extract_span_context_from_traceparent("00-invalid-hex-values-01")
        self.assertIsNone(result)

        # Test zero values (should still work)
        result = self.instrumentor._extract_span_context_from_traceparent(
            "00-00000000000000000000000000000000-1234567890123456-01"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.trace_id, 0)

    def test_wrap_handle_request_no_meta_attr(self) -> None:
        """Test _wrap_handle_request when meta attribute doesn't exist"""

        class MockRequestNoMetaAttr:
            def __init__(self):
                self.params = MockParamsNoMetaAttr()

        class MockParamsNoMetaAttr:
            def __init__(self):
                pass

        async def mock_wrapped(*args, **kwargs):
            return {"result": "no_meta_attr"}

        request = MockRequestNoMetaAttr()
        result = asyncio.run(self.instrumentor._wrap_handle_request(mock_wrapped, None, ("session", request), {}))

        self.assertEqual(result["result"], "no_meta_attr")

    def test_inject_trace_context_existing_meta(self) -> None:
        """Test trace context injection preserves existing _meta"""
        request_data = {"params": {"_meta": {"existing": "preserved"}}}
        span_ctx = SimpleSpanContext(trace_id=123, span_id=456)

        self.instrumentor._inject_trace_context(request_data, span_ctx)

        self.assertEqual(request_data["params"]["_meta"]["existing"], "preserved")
        self.assertIn("traceparent", request_data["params"]["_meta"])
