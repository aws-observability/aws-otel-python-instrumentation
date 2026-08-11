# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
import uuid
from logging import INFO, Logger, getLogger
from typing import Any, Dict, List, Optional
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from docker.types import EndpointConfig
from mock_collector_client import MockCollectorClient, ResourceScopeMetric
from mock_collector_service_pb2 import GetTracesRequest
from requests import Response, request
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

from opentelemetry.environment_variables import OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.sdk.environment_variables import (
    OTEL_BSP_SCHEDULE_DELAY,
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_METRIC_EXPORT_INTERVAL,
    OTEL_SERVICE_NAME,
    OTEL_TRACES_SAMPLER,
)
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_COLLECTION_NAME,
    DB_OPERATION_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_METHOD, RPC_SERVICE, RPC_SYSTEM_NAME
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, HTTP_ROUTE
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME
from opentelemetry.trace import SpanKind, StatusCode

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)


# pylint: disable=broad-exception-caught
class SpanMetricsContractTestBase(TestCase):
    __test__ = False

    application: Optional[DockerContainer] = None
    mock_collector: Optional[DockerContainer] = None
    mock_collector_client: Optional[MockCollectorClient] = None
    network: Optional[Network] = None

    @override
    def setUp(self) -> None:
        self.addCleanup(self.tear_down)
        network_name = f"span-metrics-contract-{uuid.uuid4().hex[:8]}"
        self.network = NetworkCollection(client=DockerClient()).create(network_name)

        collector_networking_config = {network_name: EndpointConfig(version="1.22", aliases=["collector"])}
        application_networking_config = {network_name: EndpointConfig(version="1.22", aliases=["application"])}

        self.mock_collector = (
            DockerContainer("aws-application-signals-mock-collector-python")
            .with_exposed_ports(4315)
            .with_kwargs(network=network_name, networking_config=collector_networking_config)
        )
        self.mock_collector.start()
        wait_for_logs(self.mock_collector, "Ready", timeout=20)
        self.mock_collector_client = MockCollectorClient(
            self.mock_collector.get_container_host_ip(),
            self.mock_collector.get_exposed_port(4315),
        )

        self.application = (
            DockerContainer("aws-application-signals-tests-cloudwatch-plugin-otel-app")
            .with_exposed_ports(8080)
            .with_kwargs(network=network_name, networking_config=application_networking_config)
        )
        for key, value in self.env().items():
            self.application.with_env(key, value)
        if self.get_mode() == "manual":
            self.application.with_command("python -u ./span_metrics/server.py")

        self.application.start()
        wait_for_logs(self.application, "Running on", timeout=30)
        self.mock_collector_client.clear_signals()

    def tear_down(self) -> None:
        try:
            if self.application is not None:
                _logger.info("Application stdout\n%s", self.application.get_logs()[0].decode())
                _logger.info("Application stderr\n%s", self.application.get_logs()[1].decode())
                self.application.stop()
        except Exception:
            _logger.exception("Failed to tear down application")
        try:
            if self.mock_collector is not None:
                self.mock_collector.stop()
        except Exception:
            _logger.exception("Failed to tear down mock collector")
        try:
            if self.network is not None:
                self.network.remove()
        except Exception:
            _logger.exception("Failed to remove Docker network")

    def test_derives_metrics_for_auto_instrumented_and_explicit_spans(self) -> None:
        self.assertEqual(self.send_request("GET", "exercise").status_code, 200)
        self.assertEqual(self.send_request("GET", "error").status_code, 500)

        metrics = self._get_plugin_metrics({HTTP_ROUTE: "/error"})
        self._assert_http_server(metrics, "/exercise", StatusCode.UNSET, 200)
        self._assert_http_server(metrics, "/error", StatusCode.ERROR, 500, error_type="RuntimeError")
        self._assert_metric_pair(
            metrics,
            {"span.name": "GET", "span.kind": SpanKind.CLIENT.name, HTTP_REQUEST_METHOD: "GET"},
        )
        self._assert_metric_pair(metrics, {"span.name": "internal-work", "span.kind": SpanKind.INTERNAL.name})
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "SELECT users",
                "span.kind": SpanKind.CLIENT.name,
                DB_SYSTEM_NAME: "sqlite",
                DB_OPERATION_NAME: "SELECT",
                DB_COLLECTION_NAME: "users",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "S3.ListBuckets",
                "span.kind": SpanKind.CLIENT.name,
                RPC_SYSTEM_NAME: "aws-api",
                RPC_SERVICE: "S3",
                RPC_METHOD: "ListBuckets",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "SQS.SendMessage",
                "span.kind": SpanKind.CLIENT.name,
                MESSAGING_SYSTEM: "aws.sqs",
                MESSAGING_DESTINATION_NAME: "orders",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.kind": SpanKind.PRODUCER.name,
                MESSAGING_SYSTEM: "aws.sns",
                MESSAGING_DESTINATION_NAME: "arn:aws:sns:us-east-1:123456789012:orders",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "orders receive",
                "span.kind": SpanKind.CONSUMER.name,
                MESSAGING_SYSTEM: "contract-broker",
                MESSAGING_OPERATION_NAME: "receive",
                MESSAGING_OPERATION_TYPE: "receive",
                MESSAGING_DESTINATION_NAME: "orders",
            },
        )

        trace_ids = {
            resource_scope_span.span.trace_id
            for resource_scope_span in self.mock_collector_client.get_traces()
            if self._span_attribute(resource_scope_span.span, HTTP_ROUTE) == "/exercise"
        }
        self.assertEqual(len(trace_ids), 1)
        exercise_trace_spans = [
            resource_scope_span.span
            for resource_scope_span in self.mock_collector_client.get_traces()
            if resource_scope_span.span.trace_id in trace_ids
        ]
        self.assertGreaterEqual(len(exercise_trace_spans), 8)

    def test_always_off_records_metrics_without_exporting_spans(self) -> None:
        self.assertEqual(self.send_request("GET", "exercise").status_code, 200)

        metrics = self._get_plugin_metrics({HTTP_ROUTE: "/exercise"})
        self._assert_http_server(metrics, "/exercise", StatusCode.UNSET, 200)
        self._assert_metric_pair(metrics, {"span.name": "internal-work", "span.kind": SpanKind.INTERNAL.name})
        self._assert_metric_pair(metrics, {"span.kind": SpanKind.PRODUCER.name, MESSAGING_SYSTEM: "aws.sns"})
        self._assert_metric_pair(metrics, {"span.name": "orders receive", "span.kind": SpanKind.CONSUMER.name})

        response = self.mock_collector_client.client.get_traces(GetTracesRequest())
        self.assertEqual(list(response.traces), [])

    def send_request(self, method: str, path: str) -> Response:
        address = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(8080)
        return request(method, f"http://{address}:{port}/{path}", timeout=20)

    def _get_plugin_metrics(self, required_attributes: Dict[str, Any]) -> List[ResourceScopeMetric]:
        deadline = time.time() + 20
        plugin_metrics = []
        while time.time() < deadline:
            metrics = self.mock_collector_client.get_metrics(
                {"traces.span.metrics.calls", "traces.span.metrics.duration"},
                exact_match=False,
            )
            plugin_metrics = [
                metric for metric in metrics if metric.scope_metrics.scope.name == "cloudwatch.plugin.otel.span-metrics"
            ]
            if self._matching_data_points(plugin_metrics, "traces.span.metrics.calls", required_attributes):
                self.assertTrue(all(metric.scope_metrics.scope.version for metric in plugin_metrics))
                return plugin_metrics
            time.sleep(0.1)
        raise AssertionError(
            f"No calls point matched {required_attributes}; found {len(plugin_metrics)} plugin metrics"
        )

    def _assert_http_server(
        self,
        metrics: List[ResourceScopeMetric],
        route: str,
        status_code: StatusCode,
        response_status_code: int,
        *,
        error_type: Optional[str] = None,
    ) -> None:
        expected = {
            "span.kind": SpanKind.SERVER.name,
            "status.code": status_code.name,
            HTTP_REQUEST_METHOD: "GET",
            HTTP_RESPONSE_STATUS_CODE: response_status_code,
            HTTP_ROUTE: route,
        }
        if error_type is not None:
            expected[ERROR_TYPE] = error_type
        self._assert_metric_pair(metrics, expected)

    def _assert_metric_pair(self, metrics: List[ResourceScopeMetric], expected: Dict[str, Any]) -> None:
        calls = self._latest_data_point(metrics, "traces.span.metrics.calls", expected)
        calls_attributes = self._attributes(calls.attributes)
        self.assertGreaterEqual(self._number_value(calls), 1)
        self.assertEqual(calls_attributes[SERVICE_NAME], "cloudwatch-plugin-otel-contract-test")
        self.assertEqual(calls_attributes["aws.otel.span.metrics.schema"], "v1")
        self.assertTrue(calls_attributes["aws.otel.extension.lib.version"])

        duration = self._latest_data_point(
            metrics,
            "traces.span.metrics.duration",
            calls_attributes,
        )
        self.assertGreaterEqual(duration.count, 1)
        self.assertGreaterEqual(duration.sum, 0)

    def _latest_data_point(
        self,
        metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected: Dict[str, Any],
    ):
        candidates = self._matching_data_points(metrics, metric_name, expected)
        self.assertTrue(candidates, f"No {metric_name} point matched {expected}")
        return max(candidates, key=lambda data_point: data_point.time_unix_nano)

    def _matching_data_points(
        self,
        metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected: Dict[str, Any],
    ) -> List:
        candidates = []
        for resource_scope_metric in metrics:
            if resource_scope_metric.metric.name != metric_name:
                continue
            data_points = self._data_points(resource_scope_metric)
            for data_point in data_points:
                attributes = self._attributes(data_point.attributes)
                if all(attributes.get(key) == value for key, value in expected.items()):
                    candidates.append(data_point)
        return candidates

    @staticmethod
    def _data_points(resource_scope_metric: ResourceScopeMetric):
        metric = resource_scope_metric.metric
        if metric.HasField("sum"):
            return metric.sum.data_points
        if metric.HasField("histogram"):
            return metric.histogram.data_points
        return []

    @classmethod
    def _attributes(cls, attributes) -> Dict[str, Any]:
        return {attribute.key: cls._any_value(attribute.value) for attribute in attributes}

    @staticmethod
    def _any_value(value: AnyValue) -> Any:
        kind = value.WhichOneof("value")
        return getattr(value, kind) if kind is not None else None

    @staticmethod
    def _number_value(data_point) -> float:
        kind = data_point.WhichOneof("value")
        return getattr(data_point, kind)

    @classmethod
    def _span_attribute(cls, span, key: str) -> Any:
        return cls._attributes(span.attributes).get(key)

    def get_sampler(self) -> str:
        if self._testMethodName == "test_always_off_records_metrics_without_exporting_spans":
            return "always_off"
        return "always_on"

    def env(self) -> Dict[str, str]:
        return {
            OTEL_TRACES_EXPORTER: "otlp",
            OTEL_METRICS_EXPORTER: "otlp",
            OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
            OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4315",
            OTEL_METRIC_EXPORT_INTERVAL: "100",
            OTEL_BSP_SCHEDULE_DELAY: "50",
            OTEL_SERVICE_NAME: "cloudwatch-plugin-otel-contract-test",
            OTEL_TRACES_SAMPLER: self.get_sampler(),
            "SPAN_METRICS_MODE": self.get_mode(),
        }

    def get_mode(self) -> str:
        raise NotImplementedError
