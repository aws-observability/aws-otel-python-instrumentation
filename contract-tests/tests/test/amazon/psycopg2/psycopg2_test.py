# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from requests import Response, request
from testcontainers.postgres import PostgresContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase
from amazon.utils.app_signals_constants import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
    ERROR_METRIC,
    FAULT_METRIC,
    LATENCY_METRIC,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import ExponentialHistogramDataPoint, Metric
from opentelemetry.proto.trace.v1.trace_pb2 import Span


class Psycopg2Test(ContractTestBase):
    @override
    @classmethod
    def set_up_dependency_container(cls) -> None:
        cls.container = (
            PostgresContainer(user="dbuser", password="example", dbname="postgres")
            .with_kwargs(network=NETWORK_NAME)
            .with_name("mydb")
        )
        cls.container.start()

    @override
    @classmethod
    def tear_down_dependency_container(cls) -> None:
        cls.container.stop()

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "DB_HOST": "mydb",
            "DB_USER": "dbuser",
            "DB_PASS": "example",
            "DB_NAME": "postgres",
        }

    @override
    def get_application_image_name(self) -> str:
        return "aws-appsignals-tests-psycopg2-app"

    def test_success(self) -> None:
        self.mock_collector_client.clear_signals()
        self.do_test_requests("success", "GET", "SELECT", 200, 0, 0)

    def test_fault(self) -> None:
        self.mock_collector_client.clear_signals()
        self.do_test_requests("fault", "GET", "SELECT DISTINCT", 500, 0, 1)

    def do_test_requests(
        self,
        path: str,
        method: str,
        sql_command: str,
        status_code: int,
        expected_error: int,
        expected_fault: int,
    ) -> None:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        response: Response = request(method, url, timeout=20)

        self.assertEqual(status_code, response.status_code)

        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        self._assert_aws_span_attributes(resource_scope_spans, sql_command, path)
        self._assert_semantic_conventions_span_attributes(resource_scope_spans, sql_command)

        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        self._assert_metric_attribute(metrics, LATENCY_METRIC, 5000, sql_command)
        self._assert_metric_attribute(metrics, ERROR_METRIC, expected_error, sql_command)
        self._assert_metric_attribute(metrics, FAULT_METRIC, expected_fault, sql_command)

    def _assert_aws_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], sql_command: str, path: str
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self._assert_aws_attributes(target_spans[0].attributes, sql_command)

    def _assert_aws_attributes(self, attributes_list: List[KeyValue], command: str) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # InternalOperation as OTEL does not instrument the basic server we are using, so the client span is a local
        # root.
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_SERVICE, "postgresql")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_OPERATION, f"{command}")
        # See comment above AWS_LOCAL_OPERATION
        self._assert_str_attribute(attributes_dict, AWS_SPAN_KIND, "LOCAL_ROOT")

    def _get_attributes_dict(self, attributes_list: List[KeyValue]) -> Dict[str, AnyValue]:
        attributes_dict: Dict[str, AnyValue] = {}
        for attribute in attributes_list:
            key: str = attribute.key
            value: AnyValue = attribute.value
            if key in attributes_dict:
                old_value: AnyValue = attributes_dict[key]
                self.fail(f"Attribute {key} unexpectedly duplicated. Value 1: {old_value} Value 2: {value}")
            attributes_dict[key] = value
        return attributes_dict

    def _assert_str_attribute(self, attributes_dict: Dict[str, AnyValue], key: str, expected_value: str):
        self.assertIn(key, attributes_dict)
        actual_value: AnyValue = attributes_dict[key]
        self.assertIsNotNone(actual_value)
        self.assertEqual(expected_value, actual_value.string_value)

    def _assert_int_attribute(self, attributes_dict: Dict[str, AnyValue], key: str, expected_value: int):
        self.assertIn(key, attributes_dict)
        actual_value: AnyValue = attributes_dict[key]
        self.assertIsNotNone(actual_value)
        self.assertEqual(expected_value, actual_value.int_value)

    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], commands: str
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self.assertEqual(target_spans[0].name, commands.split()[0])
        self._assert_semantic_conventions_attributes(target_spans[0].attributes, commands)

    def _assert_semantic_conventions_attributes(self, attributes_list: List[KeyValue], command: str) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self.assertTrue(attributes_dict.get("db.statement").string_value.index(command) >= 0)
        self._assert_str_attribute(attributes_dict, "db.system", "postgresql")
        self._assert_str_attribute(attributes_dict, "db.name", "postgres")

    def _assert_metric_attribute(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected_sum: int,
        aws_remote_operation: str,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)

        self.assertEqual(len(target_metrics), 1)
        target_metric: Metric = target_metrics[0]
        dp_list: List[ExponentialHistogramDataPoint] = target_metric.exponential_histogram.data_points

        self.assertEqual(len(dp_list), 2)
        dp: ExponentialHistogramDataPoint = dp_list[0]
        if len(dp_list[1].attributes) > len(dp_list[0].attributes):
            dp = dp_list[1]
        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(dp.attributes)
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # See comment on AWS_LOCAL_OPERATION in _assert_aws_attributes
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_SERVICE, "postgresql")
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_OPERATION, aws_remote_operation)
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "CLIENT")
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())

        actual_sum: float = dp.sum
        if metric_name is LATENCY_METRIC:
            self.assertTrue(0 < actual_sum < expected_sum)
        else:
            self.assertEqual(actual_sum, expected_sum)
