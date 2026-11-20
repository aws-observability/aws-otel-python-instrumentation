# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from mock_collector_client import ResourceScopeMetric
from requests import Response, request
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from amazon.utils.application_signals_constants import ERROR_METRIC, FAULT_METRIC, LATENCY_METRIC
from opentelemetry.sdk.metrics.export import AggregationTemporality

# Tests in this class are supposed to validate that the SDK was configured in the correct way:
# Metrics are deltaPreferred. Type of the metrics are exponentialHistogram


class ConfigurationTest(ContractTestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
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

        self.assertEqual(len(metrics), 3)
        for metric in metrics:
            self.assertIsNotNone(metric.metric.exponential_histogram)
            self.assertEqual(metric.metric.exponential_histogram.aggregation_temporality, AggregationTemporality.DELTA)
