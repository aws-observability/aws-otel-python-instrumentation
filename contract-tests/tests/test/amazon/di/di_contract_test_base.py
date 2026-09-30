# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Base class for DI (Dynamic Instrumentation) contract tests.

Follows the same mock-collector pattern as the trace/metrics contract tests:
- Starts a mock collector container that receives OTLP telemetry
- Starts an application container pointed at the mock collector
- Tests query the mock collector via gRPC to verify snapshot LogRecords

Snapshot pipeline:
  App function hit -> SnapshotOtlpEmitter -> POST /v1/logs -> mock collector
  Tests -> mock_collector_client.peek_logs_by_event_name() -> ResourceScopeLogRecord
"""

import sys
import time
import uuid
from logging import INFO, Logger, getLogger
from typing import Any, Dict, List, Optional
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from docker.types import EndpointConfig
from requests import Response, request
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

DI_POLL_INTERVAL = "10"  # 10s poll interval for faster test feedback
DI_WAIT_TIMEOUT = 30.0  # Wait up to 30s for snapshots (poller needs time)
DI_POLL_SLEEP = 1.0
DI_EVENT_NAME = "aws.dynamic_instrumentation.snapshot"

_MOCK_COLLECTOR_IMAGE = "aws-application-signals-mock-collector-python"
_MOCK_COLLECTOR_GRPC_PORT = 4315
_MOCK_COLLECTOR_HTTP_PORT = 4316
_MOCK_COLLECTOR_ALIAS = "collector"
_NETWORK_NAME = "di-contract-test-network"

# Expose the mock collector client on sys.path (same pattern as trace/metrics tests).
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[5] / "images" / "mock-collector"))


# pylint: disable=broad-exception-caught
class DITestInfrastructure(TestCase):
    """Infrastructure base class for DI contract tests.

    Mirrors ContractTestBase from the trace/metrics tests:
    - Mock collector receives OTLP LogRecords via HTTP
    - Tests query via gRPC and assert on protobuf objects
    - Helpers for attribute and body extraction from LogRecords
    """

    application: Optional[DockerContainer] = None
    mock_collector: Optional[DockerContainer] = None
    mock_collector_client = None
    _network: Optional[Network] = None

    def setUp(self) -> None:
        self.addCleanup(self.tear_down)
        self.application = None
        self.mock_collector = None
        self.mock_collector_client = None
        self._network = None

        network_name = f"{_NETWORK_NAME}-{uuid.uuid4().hex[:8]}"
        self._network = NetworkCollection(client=DockerClient()).create(network_name)
        collector_networking_config = {network_name: EndpointConfig(version="1.22", aliases=[_MOCK_COLLECTOR_ALIAS])}
        app_networking_config = {network_name: EndpointConfig(version="1.22", aliases=["application"])}

        self.mock_collector = (
            DockerContainer(_MOCK_COLLECTOR_IMAGE)
            .with_exposed_ports(_MOCK_COLLECTOR_GRPC_PORT, _MOCK_COLLECTOR_HTTP_PORT)
            .with_kwargs(network=network_name, networking_config=collector_networking_config)
        )
        self.mock_collector.start()
        wait_for_logs(self.mock_collector, "Ready", timeout=20)

        from mock_collector_client import MockCollectorClient

        self.mock_collector_client = MockCollectorClient(
            self.mock_collector.get_container_host_ip(),
            self.mock_collector.get_exposed_port(_MOCK_COLLECTOR_GRPC_PORT),
        )

        otlp_logs_endpoint = f"http://{_MOCK_COLLECTOR_ALIAS}:{_MOCK_COLLECTOR_HTTP_PORT}/v1/logs"

        self.application = (
            DockerContainer(self.get_application_image_name())
            .with_exposed_ports(self.get_application_port())
            .with_kwargs(network=network_name, networking_config=app_networking_config)
            .with_env("OTEL_PYTHON_DISTRO", "aws_distro")
            .with_env("OTEL_PYTHON_CONFIGURATOR", "aws_configurator")
            .with_env("OTEL_TRACES_EXPORTER", "none")
            .with_env("OTEL_METRICS_EXPORTER", "none")
            .with_env("OTEL_LOGS_EXPORTER", "none")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false")
            .with_env("OTEL_TRACES_SAMPLER", "always_on")
            .with_env("OTEL_SERVICE_NAME", self.get_application_otel_service_name())
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
            .with_env("OTEL_AWS_SERVICE_EVENTS_ENABLED", "false")
            # DI is opt-in; explicitly enable it for contract tests.
            .with_env("OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED", "true")
            .with_env("OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL", "http://localhost:3030")
            .with_env("OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL", DI_POLL_INTERVAL)
            .with_env("OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL", DI_POLL_INTERVAL)
            .with_env("OTEL_AWS_OTLP_LOGS_ENDPOINT", otlp_logs_endpoint)
        )

        extra_env = self.get_application_extra_environment_variables()
        for key in extra_env:
            self.application.with_env(key, extra_env.get(key))

        self.application.start()
        wait_for_logs(
            self.application, self.get_application_wait_pattern(), timeout=self.get_application_start_timeout()
        )
        # Wait for DI poller to fetch configs and instrument functions.
        time.sleep(int(DI_POLL_INTERVAL) + 5)

    def tear_down(self) -> None:
        try:
            if self.application is not None:
                _logger.info("Application stdout")
                _logger.info(self.application.get_logs()[0].decode())
                _logger.info("Application stderr")
                _logger.info(self.application.get_logs()[1].decode())
                self.application.stop()
        except Exception:
            _logger.exception("Failed to tear down application")
        try:
            if self.mock_collector is not None:
                self.mock_collector.stop()
        except Exception:
            _logger.exception("Failed to tear down mock collector")
        try:
            if self._network is not None:
                self._network.remove()
        except Exception:
            _logger.exception("Failed to remove Docker network")

    # -------------------------------------------------------------------------
    # OTLP value parsing helpers (mirrors ContractTestBase pattern)
    # -------------------------------------------------------------------------

    @staticmethod
    def _any_value_to_python(any_value) -> Any:
        """Convert a protobuf AnyValue to a Python native type."""
        kind = any_value.WhichOneof("value")
        if kind == "string_value":
            return any_value.string_value
        if kind == "bool_value":
            return any_value.bool_value
        if kind == "int_value":
            return any_value.int_value
        if kind == "double_value":
            return any_value.double_value
        if kind == "array_value":
            return [DITestInfrastructure._any_value_to_python(v) for v in any_value.array_value.values]
        if kind == "kvlist_value":
            return {kv.key: DITestInfrastructure._any_value_to_python(kv.value) for kv in any_value.kvlist_value.values}
        if kind == "bytes_value":
            return any_value.bytes_value
        return None

    @classmethod
    def attrs(cls, log) -> Dict[str, Any]:
        """Extract flat attributes dict from a ResourceScopeLogRecord."""
        return {kv.key: cls._any_value_to_python(kv.value) for kv in log.log_record.attributes}

    @classmethod
    def body(cls, log) -> Any:
        """Extract structured body (captures, stack) from a ResourceScopeLogRecord."""
        return cls._any_value_to_python(log.log_record.body)

    # -------------------------------------------------------------------------
    # Snapshot retrieval and filtering
    # -------------------------------------------------------------------------

    def _peek_snapshots(self) -> List:
        """Return all DI snapshot logs currently in the mock collector (non-blocking)."""
        if self.mock_collector_client is None:
            return []
        return self.mock_collector_client.peek_logs_by_event_name(DI_EVENT_NAME)

    def wait_for_snapshots(self, min_count: int = 1, timeout: float = DI_WAIT_TIMEOUT) -> List:
        """Poll the mock collector for DI snapshot OTLP LogRecords."""
        start = time.time()
        logs: List = []
        while time.time() - start < timeout:
            logs = self._peek_snapshots()
            if len(logs) >= min_count:
                return logs
            time.sleep(DI_POLL_SLEEP)
        logs = self._peek_snapshots()
        if len(logs) < min_count:
            self.fail(f"Timed out waiting for {min_count} snapshot(s). Found {len(logs)} after {timeout}s.")
        return logs

    def wait_for_method_snapshots(self, method_name: str, min_count: int = 1, timeout: float = DI_WAIT_TIMEOUT) -> List:
        """Poll until at least min_count snapshots for a specific method are present.

        Unlike wait_for_snapshots (which returns as soon as ANY snapshot arrives),
        this waits for the named method's snapshots specifically. Necessary because
        the OTLP logs use a batch processor, so a given function's snapshot may flush
        slightly later than another's.
        """
        start = time.time()
        method_logs: List = []
        while time.time() - start < timeout:
            method_logs = self.logs_for_method(self._peek_snapshots(), method_name)
            if len(method_logs) >= min_count:
                return method_logs
            time.sleep(DI_POLL_SLEEP)
        method_logs = self.logs_for_method(self._peek_snapshots(), method_name)
        if len(method_logs) < min_count:
            self.fail(
                f"Timed out waiting for {min_count} snapshot(s) for method '{method_name}'. "
                f"Found {len(method_logs)} after {timeout}s."
            )
        return method_logs

    def logs_for_method(self, logs: List, method_name: str) -> List:
        """Filter snapshot LogRecords by aws.di.method_name attribute."""
        return [log for log in logs if self.attrs(log).get("aws.di.method_name") == method_name]

    def logs_for_location_hash(self, logs: List, location_hash: str) -> List:
        """Filter snapshot LogRecords by aws.di.location_hash attribute."""
        return [log for log in logs if self.attrs(log).get("aws.di.location_hash") == location_hash]

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------

    def send_request(self, method: str, path: str, **kwargs) -> Response:
        address = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(self.get_application_port())
        url = f"http://{address}:{port}/{path}"
        return request(method, url, timeout=20, **kwargs)

    # -------------------------------------------------------------------------
    # Assertion helpers (operate on ResourceScopeLogRecord protobuf objects)
    # -------------------------------------------------------------------------

    def assert_snapshot_attr(self, log, key: str, expected_value) -> None:
        """Assert a specific attribute value on a snapshot LogRecord."""
        actual = self.attrs(log).get(key)
        self.assertEqual(actual, expected_value, f"Expected {key}={expected_value!r}, got {actual!r}")

    def assert_snapshot_has_attr(self, log, key: str) -> None:
        """Assert a specific attribute exists on a snapshot LogRecord."""
        self.assertIn(key, self.attrs(log), f"Missing attribute: {key}")

    def assert_body_has_entry_or_return(self, log) -> None:
        """Assert the snapshot body has entry and/or return captures."""
        b = self.body(log)
        self.assertIsInstance(b, dict, "Body should be a dict")
        captures = b.get("captures", {})
        has_entry = "entry" in captures
        has_return = "return" in captures
        self.assertTrue(has_entry or has_return, f"Expected entry or return captures, got: {list(captures.keys())}")

    def assert_body_has_lines_capture(self, log) -> None:
        """Assert the snapshot body has captures.lines (line-level snapshot)."""
        b = self.body(log)
        self.assertIsInstance(b, dict, "Body should be a dict")
        captures = b.get("captures", {})
        self.assertIn("lines", captures, f"Expected captures.lines, got: {list(captures.keys())}")
        self.assertGreater(len(captures["lines"]), 0, "captures.lines should have at least one entry")

    def assert_body_has_stack(self, log) -> None:
        """Assert the snapshot body has a stack frames list."""
        b = self.body(log)
        self.assertIsInstance(b, dict, "Body should be a dict")
        stack = b.get("stack", [])
        self.assertIsInstance(stack, list, "stack should be a list")

    def assert_has_trace_context(self, log) -> None:
        """Assert the LogRecord has non-zero trace_id and span_id."""
        self.assertIsNotNone(log.log_record.trace_id)
        self.assertIsNotNone(log.log_record.span_id)

    # -------------------------------------------------------------------------
    # Overridable methods
    # -------------------------------------------------------------------------

    @staticmethod
    def get_application_image_name() -> str:
        raise NotImplementedError

    def get_application_port(self) -> int:
        return 8080

    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {}

    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def get_application_otel_service_name(self) -> str:
        return "di-test-service"

    def get_application_start_timeout(self) -> int:
        return 30
