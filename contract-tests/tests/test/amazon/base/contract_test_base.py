# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
import time
from logging import INFO, Logger, getLogger
from typing import Dict, List
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from docker.types import EndpointConfig
from mock_collector_client import MockCollectorClient, ResourceScopeMetric, ResourceScopeSpan
from requests import Response, request
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

from amazon.utils.application_signals_constants import ERROR_METRIC, FAULT_METRIC, LATENCY_METRIC
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue

NETWORK_NAME: str = "aws-application-signals-network"

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)
_MOCK_COLLECTOR_ALIAS: str = "collector"
_MOCK_COLLECTOR_NAME: str = "aws-application-signals-mock-collector-python"
_MOCK_COLLECTOR_PORT: int = 4315


# pylint: disable=broad-exception-caught
class ContractTestBase(TestCase):
    """Base class for implementing a contract test.

    This class will create all the boilerplate necessary to run a contract test. It will: 1.Create a mock collector
    container that receives telemetry data of the application being tested. 2. Create an application container which
    will be used to exercise the library under test.

    Several methods are provided that can be overridden to customize the test scenario.
    """

    application: DockerContainer
    mock_collector: DockerContainer
    mock_collector_client: MockCollectorClient
    network: Network

    @classmethod
    @override
    def setUpClass(cls) -> None:
        cls.addClassCleanup(cls.class_tear_down)
        cls.network = NetworkCollection(client=DockerClient()).create(NETWORK_NAME)
        mock_collector_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_MOCK_COLLECTOR_ALIAS])
        }
        cls.mock_collector: DockerContainer = (
            DockerContainer(_MOCK_COLLECTOR_NAME)
            .with_exposed_ports(_MOCK_COLLECTOR_PORT)
            .with_name(_MOCK_COLLECTOR_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=mock_collector_networking_config)
        )
        cls.mock_collector.start()
        wait_for_logs(cls.mock_collector, "Ready", timeout=20)
        cls.set_up_dependency_container()

    @classmethod
    def class_tear_down(cls) -> None:
        try:
            cls.tear_down_dependency_container()
        except Exception:
            _logger.exception("Failed to tear down dependency container")

        try:
            _logger.info("MockCollector stdout")
            _logger.info(cls.mock_collector.get_logs()[0].decode())
            _logger.info("MockCollector stderr")
            _logger.info(cls.mock_collector.get_logs()[1].decode())
            cls.mock_collector.stop()
        except Exception:
            _logger.exception("Failed to tear down mock collector")

        cls.network.remove()

    @override
    def setUp(self) -> None:
        self.addCleanup(self.tear_down)
        application_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(version="1.22", aliases=self.get_application_network_aliases())
        }
        self.application: DockerContainer = (
            DockerContainer(self.get_application_image_name())
            .with_exposed_ports(self.get_application_port())
            .with_env("OTEL_METRIC_EXPORT_INTERVAL", "50")
            .with_env("OTEL_PYTHON_DISTRO", "aws_distro")
            .with_env("OTEL_PYTHON_CONFIGURATOR", "aws_configurator")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "true")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", self.is_runtime_enabled())
            .with_env("OTEL_METRICS_EXPORTER", "none")
            .with_env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
            .with_env("OTEL_BSP_SCHEDULE_DELAY", "1")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT", f"http://collector:{_MOCK_COLLECTOR_PORT}")
            .with_env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", f"http://collector:{_MOCK_COLLECTOR_PORT}")
            .with_env("OTEL_RESOURCE_ATTRIBUTES", self.get_application_otel_resource_attributes())
            .with_env("OTEL_TRACES_SAMPLER", "always_on")
            .with_kwargs(network=NETWORK_NAME, networking_config=application_networking_config)
            .with_name(self.get_application_image_name())
        )

        extra_env: Dict[str, str] = self.get_application_extra_environment_variables()
        for key in extra_env:
            self.application.with_env(key, extra_env.get(key))
        self.application.start()
        wait_for_logs(self.application, self.get_application_wait_pattern(), timeout=20)
        self.mock_collector_client: MockCollectorClient = MockCollectorClient(
            self.mock_collector.get_container_host_ip(), self.mock_collector.get_exposed_port(_MOCK_COLLECTOR_PORT)
        )
        # Sleep for 100ms to ensure any startup metrics have been exported
        time.sleep(0.1)
        # Clear all start up metrics, so tests are only testing telemetry generated by their invocations.
        self.mock_collector_client.clear_signals()

    def tear_down(self) -> None:
        try:
            _logger.info("Application stdout")
            _logger.info(self.application.get_logs()[0].decode())
            _logger.info("Application stderr")
            _logger.info(self.application.get_logs()[1].decode())
            self.application.stop()
        except Exception:
            _logger.exception("Failed to tear down application")

        self.mock_collector_client.clear_signals()

    def do_test_requests(
        self, path: str, method: str, status_code: int, expected_error: int, expected_fault: int, **kwargs
    ) -> None:
        response: Response = self.send_request(method, path)
        self.assertEqual(status_code, response.status_code)

        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        self._assert_aws_span_attributes(resource_scope_spans, path, **kwargs)
        self._assert_semantic_conventions_span_attributes(resource_scope_spans, method, path, status_code, **kwargs)

        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        self._assert_metric_attributes(metrics, LATENCY_METRIC, 5000, **kwargs)
        self._assert_metric_attributes(metrics, ERROR_METRIC, expected_error, **kwargs)
        self._assert_metric_attributes(metrics, FAULT_METRIC, expected_fault, **kwargs)

    def send_request(self, method, path) -> Response:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        return request(method, url, timeout=20)

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

    def _assert_int_attribute(self, attributes_dict: Dict[str, AnyValue], key: str, expected_value: int) -> None:
        self.assertIn(key, attributes_dict)
        actual_value: AnyValue = attributes_dict[key]
        self.assertIsNotNone(actual_value)
        self.assertEqual(expected_value, actual_value.int_value)

    def _assert_float_attribute(self, attributes_dict: Dict[str, AnyValue], key: str, expected_value: float) -> None:
        self.assertIn(key, attributes_dict)
        actual_value: AnyValue = attributes_dict[key]
        self.assertIsNotNone(actual_value)
        self.assertEqual(expected_value, actual_value.double_value)

    def _assert_match_attribute(self, attributes_dict: Dict[str, AnyValue], key: str, pattern: str) -> None:
        self.assertIn(key, attributes_dict)
        actual_value: AnyValue = attributes_dict[key]
        self.assertIsNotNone(actual_value)
        self.assertRegex(actual_value.string_value, pattern)

    def check_sum(self, metric_name: str, actual_sum: float, expected_sum: float) -> None:
        if metric_name is LATENCY_METRIC:
            self.assertTrue(0 < actual_sum < expected_sum)
        else:
            self.assertEqual(actual_sum, expected_sum)

    # pylint: disable=no-self-use
    # Methods that should be overridden in subclasses
    @classmethod
    def set_up_dependency_container(cls):
        return

    @classmethod
    def tear_down_dependency_container(cls):
        return

    def get_application_port(self) -> int:
        return 8080

    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {}

    def get_application_network_aliases(self) -> List[str]:
        return []

    @staticmethod
    def get_application_image_name() -> str:
        return None

    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def get_application_otel_service_name(self) -> str:
        return self.get_application_image_name()

    def get_application_otel_resource_attributes(self) -> str:
        return "service.name=" + self.get_application_otel_service_name()

    def is_runtime_enabled(self) -> str:
        return "false"

    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs):
        self.fail("Tests must implement this function")

    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ):
        self.fail("Tests must implement this function")

    def _assert_metric_attributes(
        self, resource_scope_metrics: List[ResourceScopeMetric], metric_name: str, expected_sum: int, **kwargs
    ):
        self.fail("Tests must implement this function")

    def _is_valid_regex(self, pattern: str) -> bool:
        try:
            re.compile(pattern)
            return True
        except (re.error, StopIteration, RuntimeError, KeyError):
            return False
