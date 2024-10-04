# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeMetric
from requests import Response
from typing_extensions import override

import amazon.utils.application_signals_constants as constants
from amazon.base.contract_test_base import ContractTestBase
from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric, NumberDataPoint


class RuntimeMetricsTest(ContractTestBase):
    @override
    def is_runtime_enabled(self) -> str:
        return "true"

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

    def test_runtime_succeeds(self) -> None:
        self.mock_collector_client.clear_signals()
        response: Response = self.send_request("GET", "success")
        self.assertEqual(200, response.status_code)

        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {
                constants.LATENCY_METRIC,
                constants.ERROR_METRIC,
                constants.FAULT_METRIC,
                constants.PYTHON_PROCESS_CPU_TIME,
                constants.PYTHON_PROCESS_CPU_UTILIZATION,
                constants.PYTHON_PROCESS_GC_COUNT,
                constants.PYTHON_PROCESS_MEMORY_USED,
                constants.PYTHON_PROCESS_THREAD_COUNT,
            },
            False,
        )
        self._assert_resource_attributes(metrics)
        self._assert_counter_attribute_exists(metrics, constants.PYTHON_PROCESS_CPU_TIME, "")
        self._assert_gauge_attribute_exists(metrics, constants.PYTHON_PROCESS_CPU_UTILIZATION, "")
        self._assert_gauge_attribute_exists(metrics, constants.PYTHON_PROCESS_GC_COUNT, "count")
        self._assert_gauge_attribute_exists(metrics, constants.PYTHON_PROCESS_MEMORY_USED, "type")
        self._assert_gauge_attribute_exists(metrics, constants.PYTHON_PROCESS_THREAD_COUNT, "")

    def _assert_resource_attributes(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
    ) -> None:
        for metric in resource_scope_metrics:
            attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(metric.resource_metrics.resource.attributes)
            self._assert_str_attribute(
                attribute_dict, constants.AWS_LOCAL_SERVICE, self.get_application_otel_service_name()
            )

    def _assert_gauge_attribute_exists(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        attribute_key: str,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)
        self.assertTrue(len(target_metrics) > 0)

        for target_metric in target_metrics:
            dp_list: List[NumberDataPoint] = target_metric.gauge.data_points
            self.assertTrue(len(dp_list) > 0)
            if attribute_key != "":
                attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(dp_list[0].attributes)
                self.assertIsNotNone(attribute_dict.get(attribute_key))

    def _assert_counter_attribute_exists(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        attribute_key: str,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)
        self.assertTrue(len(target_metrics) > 0)

        for target_metric in target_metrics:
            dp_list: List[NumberDataPoint] = target_metric.sum.data_points
            self.assertTrue(len(dp_list) > 0)
            if attribute_key != "":
                attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(dp_list[0].attributes)
                self.assertIsNotNone(attribute_dict.get(attribute_key))
