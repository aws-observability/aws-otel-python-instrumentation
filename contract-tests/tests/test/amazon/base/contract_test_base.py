# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import Dict, List
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from docker.types import EndpointConfig
from mock_collector_client import MockCollectorClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

NETWORK_NAME: str = "aws-appsignals-network"

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)
_MOCK_COLLECTOR_ALIAS: str = "collector"
_MOCK_COLLECTOR_NAME: str = "aws-appsignals-mock-collector-python"
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
        self.addCleanup(self.test_tear_down)
        application_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(version="1.22", aliases=self.get_application_network_aliases())
        }
        self.application: DockerContainer = (
            DockerContainer(self.get_application_image_name())
            .with_exposed_ports(self.get_application_port())
            .with_env("OTEL_METRIC_EXPORT_INTERVAL", "100")
            .with_env("OTEL_SMP_ENABLED", "true")
            .with_env("OTEL_METRICS_EXPORTER", "none")
            .with_env("OTEL_BSP_SCHEDULE_DELAY", "1")
            .with_env("OTEL_AWS_SMP_EXPORTER_ENDPOINT", f"http://collector:{_MOCK_COLLECTOR_PORT}")
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

    def test_tear_down(self) -> None:
        try:
            _logger.info("Application stdout")
            _logger.info(self.application.get_logs()[0].decode())
            _logger.info("Application stderr")
            _logger.info(self.application.get_logs()[1].decode())
            self.application.stop()
        except Exception:
            _logger.exception("Failed to tear down application")

        self.mock_collector_client.clear_signals()

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

    def get_application_image_name(self) -> str:
        return None

    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def get_application_otel_service_name(self) -> str:
        return self.get_application_image_name()

    def get_application_otel_resource_attributes(self) -> str:
        return "service.name=" + self.get_application_otel_service_name()
