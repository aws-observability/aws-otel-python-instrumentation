# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from mock_collector_client import MockCollectorClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

_MOCK_COLLECTOR_ALIAS: str = "collector"
_MOCK_COLLECTOR_NAME: str = "aws-appsignals-mock-collector-python"
_MOCK_COLLECTOR_PORT: int = 4315
_NETWORK_NAME: str = "aws-appsignals-network"

_MOCK_COLLECTOR: DockerContainer = (
    DockerContainer(_MOCK_COLLECTOR_NAME).with_exposed_ports(_MOCK_COLLECTOR_PORT).with_name(_MOCK_COLLECTOR_NAME)
)
_NETWORK: Network = NetworkCollection(client=DockerClient()).create(_NETWORK_NAME)


class ContractTestBase(TestCase):
    """Base class for implementing a contract test.

    This class will create all the boilerplate necessary to run a contract test. It will: 1.Create a mock collector
    container that receives telemetry data of the application being tested. 2. Create an application container which
    will be used to exercise the library under test.

    Several methods are provided that can be overridden to customize the test scenario.
    """

    _mock_collector_client: MockCollectorClient
    _application: DockerContainer

    @classmethod
    @override
    def setUpClass(cls) -> None:
        _MOCK_COLLECTOR.start()
        wait_for_logs(_MOCK_COLLECTOR, "Ready", timeout=20)
        _NETWORK.connect(_MOCK_COLLECTOR_NAME, aliases=[_MOCK_COLLECTOR_ALIAS])

    @classmethod
    @override
    def tearDownClass(cls) -> None:
        _MOCK_COLLECTOR.stop()
        _NETWORK.remove()

    @override
    def setUp(self) -> None:
        self._application: DockerContainer = (
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
            .with_name(self.get_application_image_name())
        )

        extra_env: dict[str, str] = self.get_application_extra_environment_variables()
        for key in extra_env:
            self._application.with_env(key, extra_env.get(key))
        self._application.start()
        wait_for_logs(self._application, self.get_application_wait_pattern(), timeout=20)
        _NETWORK.connect(self.get_application_image_name(), aliases=self.get_application_network_aliases())

        self._mock_collector_client: MockCollectorClient = MockCollectorClient(
            _MOCK_COLLECTOR.get_container_host_ip(), _MOCK_COLLECTOR.get_exposed_port(_MOCK_COLLECTOR_PORT)
        )

    @override
    def tearDown(self) -> None:
        self._application.stop()
        self._mock_collector_client.clear_signals()

    # pylint: disable=no-self-use
    # Methods that should be overridden in subclasses
    def get_application_port(self) -> int:
        return 8080

    def get_application_extra_environment_variables(self) -> dict[str, str]:
        return {}

    def get_application_network_aliases(self) -> list[str]:
        return []

    def get_application_image_name(self) -> str:
        return None

    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def get_application_otel_service_name(self) -> str:
        return self.get_application_image_name()

    def get_application_otel_resource_attributes(self) -> str:
        return "service.name=" + self.get_application_otel_service_name()
