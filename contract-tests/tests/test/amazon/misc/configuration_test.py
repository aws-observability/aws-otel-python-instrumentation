# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List
import time

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from typing_extensions import override
from opentelemetry.sdk.metrics.export import AggregationTemporality

from amazon.base.contract_test_base import ContractTestBase
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import ExponentialHistogramDataPoint, Metric
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from requests import Response, request

from amazon.utils.app_signals_constants import ERROR_METRIC, FAULT_METRIC, LATENCY_METRIC

import re


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

    def test_configuration_metrics(self):
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/success"
        response: Response = request("GET", url, timeout=20)
        self.assertEqual(200, response.status_code)
        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        self._assert_metric_configuration(metrics, metric_name="Error")
        self._assert_metric_configuration(metrics, metric_name="Fault")
        self._assert_metric_configuration(metrics, metric_name="Latency")

    def _assert_metric_configuration(self, metrics: List[ResourceScopeMetric], metric_name: str):
        for metric in metrics:
            if metric.metric.name == metric_name:
                self.assertIsNotNone(metric.metric.exponential_histogram)
                self.assertEqual(metric.metric.exponential_histogram.aggregation_temporality, AggregationTemporality.DELTA)

    def test_xray_id_format(self):
        seen: List[int]
        for _ in range(20):
            address: str = self.application.get_container_host_ip()
            port: str = self.application.get_exposed_port(self.get_application_port())
            url: str = f"http://{address}:{port}/success"
            response: Response = request("GET", url, timeout=20)
            self.assertEqual(200, response.status_code)

            start_time_sec: int = int(time.time())

            resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
            target_span: ResourceScopeSpan = resource_scope_spans[0]
            self.assertEqual(target_span.span.name, "GET success")
            trace_id_time_stamp_int: int = int(target_span.span.trace_id.hex()[:8], 16)
            self.assertGreater(trace_id_time_stamp_int, start_time_sec - 60)
            self.assertGreater(start_time_sec + 60, trace_id_time_stamp_int)
            self.mock_collector_client.clear_signals()



    def assert_resource_attributes(self, service_name):
        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            print("XYXYX")
            print(resource_scope_span.span.attributes)
            # pylint: disable=no-member
            if resource_scope_span.span.name == "GET success":
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(target_spans[0].attributes)
        for key, value in self._get_k8s_attributes().items():
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
            for key, value in self._get_k8s_attributes().items():
                self.assertEqual(metric_attributes_dict[key], value)
            self.assertEqual(metric_attributes_dict["service.name"], service_name)