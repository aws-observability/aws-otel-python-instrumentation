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

from opentelemetry.proto.common.v1.common_pb2 import AnyValue

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

_APPLICATION_IMAGE = "aws-application-signals-tests-cloudwatch-span-metrics-app"
_APPLICATION_PORT = 8080
_COLLECTOR_ALIAS = "collector"
_COLLECTOR_IMAGE = "aws-application-signals-mock-collector-python"
_COLLECTOR_PORT = 4315
_METRIC_NAMES = {"traces.span.metrics.calls", "traces.span.metrics.duration"}
_PLUGIN_SCOPE = "cloudwatch.plugin.otel.span-metrics"
_SERVICE_NAME = "cloudwatch-span-metrics-contract-test"
_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:orders"


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

        collector_networking_config = {network_name: EndpointConfig(version="1.22", aliases=[_COLLECTOR_ALIAS])}
        application_networking_config = {network_name: EndpointConfig(version="1.22", aliases=["application"])}

        self.mock_collector = (
            DockerContainer(_COLLECTOR_IMAGE)
            .with_exposed_ports(_COLLECTOR_PORT)
            .with_kwargs(network=network_name, networking_config=collector_networking_config)
        )
        self.mock_collector.start()
        wait_for_logs(self.mock_collector, "Ready", timeout=20)
        self.mock_collector_client = MockCollectorClient(
            self.mock_collector.get_container_host_ip(),
            self.mock_collector.get_exposed_port(_COLLECTOR_PORT),
        )

        self.application = (
            DockerContainer(_APPLICATION_IMAGE)
            .with_exposed_ports(_APPLICATION_PORT)
            .with_env("OTEL_TRACES_EXPORTER", "otlp")
            .with_env("OTEL_METRICS_EXPORTER", "otlp")
            .with_env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
            .with_env("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://{_COLLECTOR_ALIAS}:{_COLLECTOR_PORT}")
            .with_env("OTEL_METRIC_EXPORT_INTERVAL", "100")
            .with_env("OTEL_BSP_SCHEDULE_DELAY", "50")
            .with_env("OTEL_SERVICE_NAME", _SERVICE_NAME)
            .with_env("OTEL_TRACES_SAMPLER", self.get_sampler())
            .with_env("SPAN_METRICS_MODE", self.get_mode())
            .with_kwargs(network=network_name, networking_config=application_networking_config)
        )
        if self.get_mode() == "manual":
            self.application.with_command("python -u ./span_metrics_server.py")

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

        metrics = self._get_plugin_metrics({"http.route": "/error"})
        self._assert_http_server(metrics, "/exercise", "UNSET", 200)
        self._assert_http_server(metrics, "/error", "ERROR", 500, error_type="RuntimeError")
        self._assert_metric_pair(metrics, {"span.name": "GET", "span.kind": "CLIENT", "http.request.method": "GET"})
        self._assert_metric_pair(metrics, {"span.name": "internal-work", "span.kind": "INTERNAL"})
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "SELECT users",
                "span.kind": "CLIENT",
                "db.system.name": "sqlite",
                "db.operation.name": "SELECT",
                "db.collection.name": "users",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "S3.ListBuckets",
                "span.kind": "CLIENT",
                "rpc.system.name": "aws-api",
                "rpc.service": "S3",
                "rpc.method": "ListBuckets",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "SQS.SendMessage",
                "span.kind": "CLIENT",
                "messaging.system": "aws.sqs",
                "messaging.destination.name": "orders",
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.kind": "PRODUCER",
                "messaging.system": "aws.sns",
                "messaging.destination.name": _TOPIC_ARN,
            },
        )
        self._assert_metric_pair(
            metrics,
            {
                "span.name": "orders receive",
                "span.kind": "CONSUMER",
                "messaging.system": "contract-broker",
                "messaging.operation.name": "receive",
                "messaging.operation.type": "receive",
                "messaging.destination.name": "orders",
            },
        )

        trace_ids = {
            resource_scope_span.span.trace_id
            for resource_scope_span in self.mock_collector_client.get_traces()
            if self._span_attribute(resource_scope_span.span, "http.route") == "/exercise"
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

        metrics = self._get_plugin_metrics({"http.route": "/exercise"})
        self._assert_http_server(metrics, "/exercise", "UNSET", 200)
        self._assert_metric_pair(metrics, {"span.name": "internal-work", "span.kind": "INTERNAL"})
        self._assert_metric_pair(metrics, {"span.kind": "PRODUCER", "messaging.system": "aws.sns"})
        self._assert_metric_pair(metrics, {"span.name": "orders receive", "span.kind": "CONSUMER"})

        response = self.mock_collector_client.client.get_traces(GetTracesRequest())
        self.assertEqual(list(response.traces), [])

    def send_request(self, method: str, path: str) -> Response:
        address = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(_APPLICATION_PORT)
        return request(method, f"http://{address}:{port}/{path}", timeout=20)

    def _get_plugin_metrics(self, required_attributes: Dict[str, Any]) -> List[ResourceScopeMetric]:
        deadline = time.time() + 20
        plugin_metrics = []
        while time.time() < deadline:
            metrics = self.mock_collector_client.get_metrics(_METRIC_NAMES, exact_match=False)
            plugin_metrics = [metric for metric in metrics if metric.scope_metrics.scope.name == _PLUGIN_SCOPE]
            if self._matching_data_points(plugin_metrics, "traces.span.metrics.calls", required_attributes):
                self.assertTrue(all(metric.scope_metrics.scope.version for metric in plugin_metrics))
                return plugin_metrics
            time.sleep(0.1)
        self.fail(f"No calls point matched {required_attributes}; found {len(plugin_metrics)} plugin metrics")

    def _assert_http_server(
        self,
        metrics: List[ResourceScopeMetric],
        route: str,
        status_code: str,
        response_status_code: int,
        *,
        error_type: Optional[str] = None,
    ) -> None:
        expected = {
            "span.kind": "SERVER",
            "status.code": status_code,
            "http.request.method": "GET",
            "http.response.status_code": response_status_code,
            "http.route": route,
        }
        if error_type is not None:
            expected["error.type"] = error_type
        self._assert_metric_pair(metrics, expected)

    def _assert_metric_pair(self, metrics: List[ResourceScopeMetric], expected: Dict[str, Any]) -> None:
        calls = self._latest_data_point(metrics, "traces.span.metrics.calls", expected)
        calls_attributes = self._attributes(calls.attributes)
        self.assertGreaterEqual(self._number_value(calls), 1)
        self.assertEqual(calls_attributes["service.name"], _SERVICE_NAME)
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

    def get_mode(self) -> str:
        raise NotImplementedError
