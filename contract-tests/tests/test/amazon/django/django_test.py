# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from amazon.utils.app_signals_constants import AWS_LOCAL_OPERATION, AWS_LOCAL_SERVICE, AWS_SPAN_KIND
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import ExponentialHistogramDataPoint, Metric
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from opentelemetry.semconv.trace import SpanAttributes


class DjangoTest(ContractTestBase):
    @override
    def get_application_image_name(self) -> str:
        return "aws-appsignals-tests-django-app"

    @override
    def get_application_wait_pattern(self) -> str:
        return "Quit the server with CONTROL-C."

    @override
    def get_application_extra_environment_variables(self):
        return {"DJANGO_SETTINGS_MODULE": "django_server.settings"}

    def test_success(self) -> None:
        self.do_test_requests("success", "GET", 200, 0, 0, request_method="GET", local_operation="GET success")

    def test_post_success(self) -> None:
        self.do_test_requests(
            "post_success", "POST", 201, 0, 0, request_method="POST", local_operation="POST post_success"
        )

    def test_route(self) -> None:
        self.do_test_requests(
            "users/userId/orders/orderId",
            "GET",
            200,
            0,
            0,
            request_method="GET",
            local_operation="GET users/<str:user_id>/orders/<str:order_id>",
        )

    def test_error(self) -> None:
        self.do_test_requests("error", "GET", 400, 1, 0, request_method="GET", local_operation="GET error")

    def test_fault(self) -> None:
        self.do_test_requests("fault", "GET", 500, 0, 1, request_method="GET", local_operation="GET fault")

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_SERVER:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self._assert_aws_attributes(
            target_spans[0].attributes, kwargs.get("request_method"), kwargs.get("local_operation")
        )

    def _assert_aws_attributes(self, attributes_list: List[KeyValue], method: str, local_operation: str) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, local_operation)
        self._assert_str_attribute(attributes_dict, AWS_SPAN_KIND, "LOCAL_ROOT")

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_SERVER:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self.assertEqual(target_spans[0].name, kwargs.get("local_operation"))
        self._assert_semantic_conventions_attributes(target_spans[0].attributes, method, path, status_code)

    def _assert_semantic_conventions_attributes(
        self, attributes_list: List[KeyValue], method: str, endpoint: str, status_code: int
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_int_attribute(attributes_dict, SpanAttributes.HTTP_STATUS_CODE, status_code)
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        self._assert_str_attribute(
            attributes_dict, SpanAttributes.HTTP_URL, "http://" + address + ":" + port + "/" + endpoint
        )
        self._assert_str_attribute(attributes_dict, SpanAttributes.HTTP_METHOD, method)
        self.assertNotIn(SpanAttributes.HTTP_TARGET, attributes_dict)

    @override
    def _assert_metric_attributes(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected_sum: int,
        **kwargs,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)

        self.assertGreater(len(target_metrics), 1)
        target_metric: Metric = target_metrics[-1]
        dp_list: List[ExponentialHistogramDataPoint] = target_metric.exponential_histogram.data_points

        self.assertEqual(len(dp_list), 1)
        service_dp: ExponentialHistogramDataPoint = dp_list[0]

        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(service_dp.attributes)
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, kwargs.get("local_operation"))
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "LOCAL_ROOT")
        self.check_sum(metric_name, service_dp.sum, expected_sum)
