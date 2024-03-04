# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from testcontainers.postgres import PostgresContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase
from amazon.utils.app_signals_constants import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
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
        self.do_test_requests("success", "GET", 200, 0, 0, sql_command="SELECT")

    def test_fault(self) -> None:
        self.mock_collector_client.clear_signals()
        self.do_test_requests("fault", "GET", 500, 0, 1, sql_command="SELECT DISTINCT")

    @override
    def _assert_aws_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self._assert_aws_attributes(target_spans[0].attributes, **kwargs)

    @override
    def _assert_aws_attributes(self, attributes_list: List[KeyValue], **kwargs) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # InternalOperation as OTEL does not instrument the basic server we are using, so the client span is a local
        # root.
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_SERVICE, "postgresql")
        command: str = kwargs.get("sql_command")
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

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method, path, status_code, **kwargs
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self.assertEqual(target_spans[0].name, kwargs.get("sql_command").split()[0])
        self._assert_semantic_conventions_attributes(target_spans[0].attributes, kwargs.get("sql_command"))

    def _assert_semantic_conventions_attributes(self, attributes_list: List[KeyValue], command: str) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self.assertTrue(attributes_dict.get("db.statement").string_value.startswith(command))
        self._assert_str_attribute(attributes_dict, "db.system", "postgresql")
        self._assert_str_attribute(attributes_dict, "db.name", "postgres")

    @override
    def _assert_metric_attribute(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected_sum: int,
        **kwargs
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
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_OPERATION, kwargs.get("sql_command"))
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "CLIENT")
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())

        actual_sum: float = dp.sum
        if metric_name is LATENCY_METRIC:
            self.assertTrue(0 < actual_sum < expected_sum)
        else:
            self.assertEqual(actual_sum, expected_sum)
