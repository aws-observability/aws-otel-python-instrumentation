# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

import docker
from docker import DockerClient
from docker.errors import DockerException
from docker.models.containers import Container
from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from requests import Response, request
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
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
from opentelemetry.semconv.trace import SpanAttributes

NETWORK_NAME: str = "aws-appsignals-network"


class Psychopg2Test(ContractTestBase):
    @override
    def get_application_network_aliases(self) -> List[str]:
        """
        This will be the target hostname of the clients making http requests in the application image, so that they
        don't use localhost.
        """
        return ["backend"]

    @override
    @classmethod
    def set_up_dependency_container(cls) -> None:
        client: DockerClient = docker.from_env()
        client.containers.run(
            "postgres:latest",
            environment={"POSTGRES_PASSWORD": "example"},
            detach=True,
            name="mydb",
            network=NETWORK_NAME,
        )

    @override
    @classmethod
    def tear_down_dependency_container(cls) -> None:
        client = docker.from_env()
        try:
            container: Container = client.containers.get("mydb")
            container.stop()
            container.remove()
        except DockerException as exception:
            print("error when cleaning docker resource:", exception)

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "DB_HOST": "mydb",
            "DB_USER": "postgres",
            "DB_PASS": "example",
            "DB_NAME": "postgres",
            "OTEL_INSTRUMENTATION_COMMON_PEER_SERVICE_MAPPING": "backend=backend:8080",
        }

    @override
    def get_application_image_name(self) -> str:
        return "aws-appsignals-tests-psychopg2-app"

    def test_success(self) -> None:
        self.do_test_requests("success", "GET", ["DROP TABLE", "CREATE TABLE", "INSERT INTO", "INSERT INTO", "SELECT"], 200, 0, 0)

    def test_fault(self) -> None:
        self.do_test_requests("fault", "GET", ["DROP TABLE", "CREATE TABLE", "INSERT INTO", "INSERT INTO", "SELECT"], 500, 0, 1)

    def do_test_requests(
        self, path: str, method: str, sql_commands: List[str], status_code: int, expected_error: int, expected_fault: int
    ) -> None:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        response: Response = request(method, url, timeout=20)

        self.assertEqual(status_code, response.status_code)

        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        self._assert_aws_span_attributes(resource_scope_spans, sql_commands, path)
        self._assert_semantic_conventions_span_attributes(resource_scope_spans, sql_commands)

        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        self._assert_metric_attributes(metrics, method, path, LATENCY_METRIC, 5000)
        self._assert_metric_attributes(metrics, method, path, ERROR_METRIC, expected_error)
        self._assert_metric_attributes(metrics, method, path, FAULT_METRIC, expected_fault)

    def _assert_aws_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], sql_commands: List[str], path: str
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), len(sql_commands))
        for command in sql_commands:
            self._assert_aws_attributes(target_spans[sql_commands.index(command)].attributes, command)

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
        self, resource_scope_spans: List[ResourceScopeSpan], commands: List[str]
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), len(commands))
        for target_span in target_spans:
            index: int = target_spans.index(target_span)
            self.assertEqual(target_span.name, commands[index].split()[0])
            self._assert_semantic_conventions_attributes(target_spans[index].attributes, commands[index])

    def _assert_semantic_conventions_attributes(
        self, attributes_list: List[KeyValue], command: str
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, "net.peer.name", "mydb")
        self._assert_int_attribute(attributes_dict, "net.peer.port", 5432)
        self.assertTrue(attributes_dict.get("db.statement").string_value.index(command) >= 0)
        self._assert_str_attribute(attributes_dict, "db.system", "postgresql")
        self._assert_str_attribute(attributes_dict, "db.name", "postgres")
        self._assert_str_attribute(attributes_dict, "db.user", "postgres")

    def _assert_metric_attributes(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        method: str,
        path: str,
        metric_name: str,
        expected_sum: int,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)

        self.assertEqual(len(target_metrics), 2)
        target_metric: Metric = target_metrics[1]
        dp_list: List[ExponentialHistogramDataPoint] = target_metric.exponential_histogram.data_points

        self.assertEqual(len(dp_list), 2)
        dp: ExponentialHistogramDataPoint = dp_list[0]
        print(dp_list)
        print(dp)
        if len(dp_list[1].attributes) > len(dp_list[0].attributes):
            dp = dp_list[1]
        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(dp.attributes)
        print(attribute_dict)
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # See comment on AWS_LOCAL_OPERATION in _assert_aws_attributes
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_SERVICE, "postgresql")
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_OPERATION, "SELECT")
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "LOCAL_ROOT")

        actual_sum: float = dp.sum
        if metric_name is LATENCY_METRIC:
            self.assertTrue(0 < actual_sum < expected_sum)
        else:
            self.assertEqual(actual_sum, expected_sum)
