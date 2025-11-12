# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeSpan
from requests import Response
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from opentelemetry.semconv.attributes.code_attributes import CODE_LINE_NUMBER


class CodeAttributesTest(ContractTestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-requests-app"

    @override
    def get_application_network_aliases(self) -> List[str]:
        """
        This will be the target hostname of the clients making http requests in the application image, so that they
        don't use localhost.
        """
        return ["backend"]

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        """
        This does not appear to do anything, as it does not seem that OTEL supports peer service for Python. Keeping
        for consistency with Java contract tests at this time.
        """
        return {
            "OTEL_INSTRUMENTATION_COMMON_PEER_SERVICE_MAPPING": "backend=backend:8080",
            "OTEL_AWS_ENHANCED_CODE_ATTRIBUTES": "true",
        }

    def test_success(self) -> None:
        self.do_test_requests("success", "GET", 200, 0, 0, request_method="GET")

    def test_error(self) -> None:
        self.do_test_requests("error", "GET", 400, 1, 0, request_method="GET")

    def test_fault(self) -> None:
        self.do_test_requests("fault", "GET", 500, 0, 1, request_method="GET")

    def test_success_post(self) -> None:
        self.do_test_requests("success/postmethod", "POST", 200, 0, 0, request_method="POST")

    def test_error_post(self) -> None:
        self.do_test_requests("error/postmethod", "POST", 400, 1, 0, request_method="POST")

    def test_fault_post(self) -> None:
        self.do_test_requests("fault/postmethod", "POST", 500, 0, 1, request_method="POST")

    def do_test_requests(
        self, path: str, method: str, status_code: int, expected_error: int, expected_fault: int, **kwargs
    ) -> None:
        response: Response = self.send_request(method, path)
        self.assertEqual(status_code, response.status_code)

        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        self._assert_span_code_attributes(resource_scope_spans, path, **kwargs)

    @override
    def _assert_span_code_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self._assert_code_attribute(target_spans[0].attributes)

    def _assert_code_attribute(self, attributes_list: List[KeyValue]):
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        # The line number of calling requests.request in requests_server.py
        self._assert_int_attribute(attributes_dict, CODE_LINE_NUMBER, 41)
