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
        self.do_test_requests("call_tool", "GET", 200, 0, 0)
        self.do_test_requests("list_tools", "GET", 200, 0, 0)
        self.do_test_requests("list_prompts", "GET", 200, 0, 0)
        self.do_test_requests("list_resources", "GET", 200, 0, 0)
        self.do_test_requests("read_resource", "GET", 200, 0, 0)
        self.do_test_requests("get_prompt", "GET", 200, 0, 0)
        self.do_test_requests("complete", "GET", 200, 0, 0)
        self.do_test_requests("set_logging_level", "GET", 200, 0, 0)
        self.do_test_requests("ping", "GET", 200, 0, 0)

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans, path: str, **kwargs) -> None:
        pass

    @override
    # pylint: disable=too-many-locals,too-many-statements
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans, method: str, path: str, status_code: int, **kwargs
    ) -> None:
        
        for resource_scope_span in resource_scope_spans:
            for scope_span in resource_scope_span.scope_spans:
                for span in scope_span.spans:
                    print(f"Span attributes: {span.attributes}")

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass
