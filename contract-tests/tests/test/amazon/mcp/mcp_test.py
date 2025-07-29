# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from opentelemetry.proto.trace.v1.trace_pb2 import Span


class MCPTest(ContractTestBase):

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-mcp-app"

    def test_mcp_echo_tool(self):
        """Test MCP echo tool call creates proper spans"""
        self.do_test_requests("mcp/echo", "GET", 200, 0, 0, tool_name="echo")

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans, path: str, **kwargs) -> None:
        pass

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans, method: str, path: str, status_code: int, **kwargs
    ) -> None:

        tool_name = kwargs.get("tool_name", "echo")
        initialize_client_span = None
        list_tools_client_span = None
        list_tools_server_span = None
        call_tool_client_span = None
        call_tool_server_span = None

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span

            if span.name == "client.send_request" and span.kind == Span.SPAN_KIND_CLIENT:
                for attr in span.attributes:
                    if attr.key == "mcp.initialize" and attr.value.bool_value:
                        initialize_client_span = span
                        break
                    elif attr.key == "mcp.list_tools" and attr.value.bool_value:
                        list_tools_client_span = span
                        break
                    elif attr.key == "mcp.call_tool" and attr.value.bool_value:
                        call_tool_client_span = span
                        break

            elif span.name == "tools/list" and span.kind == Span.SPAN_KIND_SERVER:
                list_tools_server_span = span
            elif span.name == f"tools/{tool_name}" and span.kind == Span.SPAN_KIND_SERVER:
                call_tool_server_span = span

        # Validate initialize client span (no server span expected)
        self.assertIsNotNone(initialize_client_span, "Initialize client span not found")
        self.assertEqual(initialize_client_span.kind, Span.SPAN_KIND_CLIENT)

        init_attributes = {attr.key: attr.value for attr in initialize_client_span.attributes}
        self.assertIn("mcp.initialize", init_attributes)
        self.assertTrue(init_attributes["mcp.initialize"].bool_value)

        # Validate list tools client span
        self.assertIsNotNone(list_tools_client_span, "List tools client span not found")
        self.assertEqual(list_tools_client_span.kind, Span.SPAN_KIND_CLIENT)

        list_client_attributes = {attr.key: attr.value for attr in list_tools_client_span.attributes}
        self.assertIn("mcp.list_tools", list_client_attributes)
        self.assertTrue(list_client_attributes["mcp.list_tools"].bool_value)

        # Validate list tools server span
        self.assertIsNotNone(list_tools_server_span, "List tools server span not found")
        self.assertEqual(list_tools_server_span.kind, Span.SPAN_KIND_SERVER)

        list_server_attributes = {attr.key: attr.value for attr in list_tools_server_span.attributes}
        self.assertIn("mcp.list_tools", list_server_attributes)
        self.assertTrue(list_server_attributes["mcp.list_tools"].bool_value)

        # Validate call tool client span
        self.assertIsNotNone(call_tool_client_span, f"Call tool client span for {tool_name} not found")
        self.assertEqual(call_tool_client_span.kind, Span.SPAN_KIND_CLIENT)

        call_client_attributes = {attr.key: attr.value for attr in call_tool_client_span.attributes}
        self.assertIn("mcp.call_tool", call_client_attributes)
        self.assertTrue(call_client_attributes["mcp.call_tool"].bool_value)
        self.assertIn("aws.remote.operation", call_client_attributes)
        self.assertEqual(call_client_attributes["aws.remote.operation"].string_value, tool_name)

        # Validate call tool server span
        self.assertIsNotNone(call_tool_server_span, f"Call tool server span for {tool_name} not found")
        self.assertEqual(call_tool_server_span.kind, Span.SPAN_KIND_SERVER)

        call_server_attributes = {attr.key: attr.value for attr in call_tool_server_span.attributes}
        self.assertIn("mcp.call_tool", call_server_attributes)
        self.assertTrue(call_server_attributes["mcp.call_tool"].bool_value)

        # Validate distributed tracing for paired spans
        self.assertEqual(
            list_tools_server_span.trace_id,
            list_tools_client_span.trace_id,
            "List tools client and server spans should have the same trace ID",
        )
        self.assertEqual(
            call_tool_server_span.trace_id,
            call_tool_client_span.trace_id,
            "Call tool client and server spans should have the same trace ID",
        )

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass
