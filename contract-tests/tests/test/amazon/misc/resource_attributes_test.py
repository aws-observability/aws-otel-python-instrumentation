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
from requests import Response, request

from amazon.utils.app_signals_constants import ERROR_METRIC, FAULT_METRIC, LATENCY_METRIC


class ResourceAttributesTest(ContractTestBase):
    @override
    def get_application_image_name(self) -> str:
        return "aws-appsignals-tests-django-app"

    @override
    def get_application_wait_pattern(self) -> str:
        return "Quit the server with CONTROL-C."

    @override
    def get_application_extra_environment_variables(self):
        return {"DJANGO_SETTINGS_MODULE": "django_server.settings"}

    def _get_k8s_attributes(self):
        return {"k8s.namespace.name": "namespace-name",
                "k8s.pod.name": "pod-name",
                "k8s.deployment.name": "deployment-name"}

    def do_misc_test_request(self):
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/success"
        response: Response = request("GET", url, timeout=20)
        self.assertEqual(200, response.status_code)
        self.assert_resource_attributes()

    def assert_resource_attributes(self, service_name):
        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )

        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.name == "GET /success":
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(target_spans[0].attributes)
        for key, value in self._get_k8s_attributes():
            self.assertEqual(attributes_dict[key], value)
        self.assertEqual(attributes_dict["service.name"], service_name)

        target_metrics: List[Metric] = []
        for resource_scope_metric in metrics:
            if resource_scope_metric.metric.name.lower() in ["Error", "Fault", "Latency"]:
                target_metrics.append(resource_scope_metric.metric)
        for target_metric in target_metrics:
            dp_list: List[ExponentialHistogramDataPoint] = target_metric.exponential_histogram.data_points
            self.assertEqual(len(dp_list), 1)
            metric_attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(dp_list[0].attributes)
            for key, value in self._get_k8s_attributes():
                self.assertEqual(metric_attributes_dict[key], value)
            self.assertEqual(metric_attributes_dict["service.name"], service_name)

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

