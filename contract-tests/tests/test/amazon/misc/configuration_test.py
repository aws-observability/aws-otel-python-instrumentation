# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from typing import List

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from requests import Response, request
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from amazon.utils.application_signals_constants import ERROR_METRIC, FAULT_METRIC, LATENCY_METRIC
from opentelemetry.sdk.metrics.export import AggregationTemporality


class ResourceAttributesTest(ContractTestBase):
    @override
    def get_application_image_name(self) -> str:
        return "aws-application-signals-tests-django-app"

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
                self.assertEqual(
                    metric.metric.exponential_histogram.aggregation_temporality, AggregationTemporality.DELTA
                )

    def test_xray_id_format(self):
        seen: List[str] = []
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

            self.assertTrue(target_span.span.trace_id.hex() not in seen)
            seen.append(target_span.span.trace_id.hex())

            trace_id_time_stamp_int: int = int(target_span.span.trace_id.hex()[:8], 16)
            self.assertGreater(trace_id_time_stamp_int, start_time_sec - 60)
            self.assertGreater(start_time_sec + 60, trace_id_time_stamp_int)
            self.mock_collector_client.clear_signals()
