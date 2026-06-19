# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
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

SERVICE_EVENTS_FLUSH_INTERVAL_MS: str = "2000"
OTLP_POLL_TIMEOUT: float = 20.0
OTLP_POLL_INTERVAL: float = 1.0

# Deployment/VCS metadata injected via env so the OTLP VCS + deployment attributes
# (vcs.ref.head.revision, vcs.repository.url.full, aws.service_events.deployment.id)
# can be asserted end-to-end on EndpointSummary / IncidentSnapshot signals.
TEST_DEPLOYMENT_ID: str = "deploy-contract-test"
TEST_GIT_COMMIT_SHA: str = "0123456789abcdef0123456789abcdef01234567"
TEST_GIT_REPO_URL: str = "https://github.com/aws/contract-test-repo"
# DeploymentEvent-only metadata: these ride solely on the deployment_event signal
# (aws.service_events.deployment.{url,timestamp}), so they're asserted there.
TEST_DEPLOYMENT_URL: str = "https://github.com/aws/contract-test-repo/actions/runs/123"
TEST_DEPLOYMENT_TIMESTAMP: str = "2026-01-01T00:00:00Z"

VALID_SEVERITIES = {"critical", "high", "medium", "low", "info"}

# Mock collector config
_MOCK_COLLECTOR_IMAGE: str = "aws-application-signals-mock-collector-python"
_MOCK_COLLECTOR_GRPC_PORT: int = 4315
_MOCK_COLLECTOR_HTTP_PORT: int = 4316
_MOCK_COLLECTOR_ALIAS: str = "collector"
_NETWORK_NAME: str = "serviceevents-contract-test-network"

# Add mock collector client to path
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[5] / "images" / "mock-collector"))


# pylint: disable=broad-exception-caught
class ServiceEventsTestInfrastructure(TestCase):
    """Infrastructure base class with container lifecycle and OTLP assertion helpers.

    Telemetry is exported exclusively via OTLP to a mock collector container.
    The file exporter is not supported. Tests must inspect OTLP LogRecords/Metrics
    via the mock collector client.
    """

    application: Optional[DockerContainer] = None
    mock_collector: Optional[DockerContainer] = None
    mock_collector_client = None
    _network: Optional[Network] = None

    def setUp(self) -> None:
        self.addCleanup(self.tear_down)
        # Initialize so tear_down can run even if setUp fails mid-way.
        self.application = None
        self.mock_collector = None
        self.mock_collector_client = None
        self._network = None

        # Unique network name per test to avoid 409 conflicts across parallel or
        # sequential tests that didn't clean up cleanly.
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

        collector_host = self.mock_collector.get_container_host_ip()
        collector_grpc_port = self.mock_collector.get_exposed_port(_MOCK_COLLECTOR_GRPC_PORT)
        self.mock_collector_client = MockCollectorClient(collector_host, collector_grpc_port)

        # OTLPLogExporter/OTLPMetricExporter POST to the endpoint as-is (no /v1/logs appended
        # when endpoint is passed to the constructor). Include the full path.
        otlp_logs_endpoint = f"http://{_MOCK_COLLECTOR_ALIAS}:{_MOCK_COLLECTOR_HTTP_PORT}/v1/logs"
        otlp_metrics_endpoint = f"http://{_MOCK_COLLECTOR_ALIAS}:{_MOCK_COLLECTOR_HTTP_PORT}/v1/metrics"

        self.application = (
            DockerContainer(self.get_application_image_name())
            .with_exposed_ports(self.get_application_port())
            .with_kwargs(network=network_name, networking_config=app_networking_config)
            # Standard OTel config
            .with_env("OTEL_PYTHON_DISTRO", "aws_distro")
            .with_env("OTEL_PYTHON_CONFIGURATOR", "aws_configurator")
            .with_env("OTEL_TRACES_EXPORTER", "none")
            .with_env("OTEL_METRICS_EXPORTER", "none")
            .with_env("OTEL_LOGS_EXPORTER", "none")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false")
            .with_env("OTEL_TRACES_SAMPLER", "always_on")
            .with_env("OTEL_SERVICE_NAME", self.get_application_otel_service_name())
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
            # ServiceEvents config (OTLP-only export path)
            .with_env("OTEL_AWS_SERVICE_EVENTS_ENABLED", "true")
            .with_env("OTEL_AWS_SERVICE_EVENTS_FUNCTION_INSTRUMENT_ENABLED", "true")
            # Flush intervals (and any subclass knobs) are internal now; inject the fast test
            # cadence via the test-config hook. See get_test_config_hook_overrides().
            .with_env("DEBUG_SE_TEST_CONFIG", self._build_test_config_hook_value())
            .with_env("OTEL_AWS_SERVICE_EVENTS_SAMPLING_MODE", "always")
            # Deployment/VCS metadata — feeds the VCS + deployment OTLP attributes.
            .with_env("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID", TEST_DEPLOYMENT_ID)
            .with_env("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA", TEST_GIT_COMMIT_SHA)
            .with_env("OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL", TEST_GIT_REPO_URL)
            # DeploymentEvent-only metadata — feeds deployment.url / deployment.timestamp.
            .with_env("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_URL", TEST_DEPLOYMENT_URL)
            .with_env("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_TIMESTAMP", TEST_DEPLOYMENT_TIMESTAMP)
            # Explicit allowlist required — there is no implicit default scope. Covers the
            # contract-test apps' own modules: `helpers` (Flask/FastAPI) and the
            # `serviceevents_*_server` / `serviceevents_django.*` app code. fnmatch's
            # `*` spans dots, so `serviceevents_*` also matches `serviceevents_django.views`.
            .with_env(
                "OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE",
                "helpers,serviceevents_*",
            )
            .with_env("OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_PER_MINUTE", "1000")
            .with_env("OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_SAME_ERROR", "100")
            .with_env("OTEL_AWS_OTLP_LOGS_ENDPOINT", otlp_logs_endpoint)
            .with_env("OTEL_AWS_OTLP_METRICS_ENDPOINT", otlp_metrics_endpoint)
            # Force the OTel metric reader to flush every 2s so contract tests
            # see service.function.duration / count
            # within the OTLP_POLL_TIMEOUT window. Production defaults to 60s.
            .with_env("OTEL_METRIC_EXPORT_INTERVAL", SERVICE_EVENTS_FLUSH_INTERVAL_MS)
        )

        extra_env: Dict[str, str] = self.get_application_extra_environment_variables()
        for key in extra_env:
            self.application.with_env(key, extra_env.get(key))

        self.application.start()
        wait_for_logs(
            self.application, self.get_application_wait_pattern(), timeout=self.get_application_start_timeout()
        )
        time.sleep(0.5)

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
    # OTLP value parsing helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _any_value_to_python(any_value) -> Any:
        """Convert an OTLP AnyValue protobuf to a Python primitive/dict/list."""
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
            return [ServiceEventsTestInfrastructure._any_value_to_python(v) for v in any_value.array_value.values]
        if kind == "kvlist_value":
            return {
                kv.key: ServiceEventsTestInfrastructure._any_value_to_python(kv.value)
                for kv in any_value.kvlist_value.values
            }
        if kind == "bytes_value":
            return any_value.bytes_value
        return None

    @classmethod
    def attrs(cls, log) -> Dict[str, Any]:
        """Return log record's attributes as a flat {key: python_value} dict."""
        return {kv.key: cls._any_value_to_python(kv.value) for kv in log.log_record.attributes}

    @classmethod
    def body(cls, log) -> Any:
        """Return log record's body converted to a python primitive/dict/list."""
        return cls._any_value_to_python(log.log_record.body)

    # -------------------------------------------------------------------------
    # OTLP log helpers
    # -------------------------------------------------------------------------

    def get_otlp_logs_by_event_name(self, event_name: str) -> List:
        """Get OTLP log records from mock collector filtered by event.name (non-blocking)."""
        if self.mock_collector_client is None:
            return []
        return self.mock_collector_client.peek_logs_by_event_name(event_name)

    def wait_for_otlp_logs(self, event_name: str, min_count: int = 1, timeout: Optional[float] = None) -> List:
        """Poll mock collector until min_count logs with event_name appear."""
        if self.mock_collector_client is None:
            self.fail("Mock collector not initialized — cannot poll OTLP logs")
        if timeout is None:
            timeout = OTLP_POLL_TIMEOUT

        start = time.time()
        records: List = []
        while time.time() - start < timeout:
            records = self.get_otlp_logs_by_event_name(event_name)
            if len(records) >= min_count:
                return records
            time.sleep(OTLP_POLL_INTERVAL)

        records = self.get_otlp_logs_by_event_name(event_name)
        if len(records) < min_count:
            self.fail(
                f"Timed out waiting for {min_count} OTLP log(s) with event.name='{event_name}'. "
                f"Found {len(records)} after {timeout}s."
            )
        return records

    def get_endpoint_summary_logs(self, method: str, route: str) -> List:
        """Return EndpointSummary OTLP logs filtered by method + route."""
        logs = self.get_otlp_logs_by_event_name("aws.service_events.endpoint_summary")
        result = []
        for log in logs:
            a = self.attrs(log)
            if a.get("http.request.method") == method and a.get("url.route") == route:
                result.append(log)
        return result

    def wait_for_endpoint_summary(self, method: str, route: str, timeout: Optional[float] = None) -> List:
        """Poll for an EndpointSummary log matching method + route."""
        if timeout is None:
            timeout = OTLP_POLL_TIMEOUT
        start = time.time()
        logs: List = []
        while time.time() - start < timeout:
            logs = self.get_endpoint_summary_logs(method, route)
            if logs:
                return logs
            time.sleep(OTLP_POLL_INTERVAL)
        logs = self.get_endpoint_summary_logs(method, route)
        if not logs:
            self.fail(f"Timed out waiting for EndpointSummary log for {method} {route} after {timeout}s.")
        return logs

    # -------------------------------------------------------------------------
    # OTLP metric helpers — function-call latency Histogram
    # -------------------------------------------------------------------------
    #
    # FunctionCall telemetry flows through a single OTel metric:
    #
    #   - service.function.duration (Histogram): sampled calls only — latency

    _FUNCTION_DURATION_METRIC_NAME: str = "service.function.duration"

    def _peek_function_duration_data_points(self) -> List:
        """Return all data points for the service.function.duration histogram (non-blocking)."""
        if self.mock_collector_client is None:
            return []
        # exact_match=False so we don't time out when other metrics aren't present yet.
        try:
            metrics = self.mock_collector_client.get_metrics({self._FUNCTION_DURATION_METRIC_NAME}, exact_match=False)
        except RuntimeError:
            return []
        data_points: List = []
        for rsm in metrics:
            if rsm.metric.name != self._FUNCTION_DURATION_METRIC_NAME:
                continue
            histogram_proto = rsm.metric.WhichOneof("data")
            if histogram_proto == "exponential_histogram":
                data_points.extend(rsm.metric.exponential_histogram.data_points)
            elif histogram_proto == "histogram":
                data_points.extend(rsm.metric.histogram.data_points)
        return data_points

    def wait_for_function_duration_metric(self, min_count: int = 1, timeout: Optional[float] = None) -> List:
        """Poll until at least min_count data points appear in the function-duration histogram."""
        if self.mock_collector_client is None:
            self.fail("Mock collector not initialized — cannot poll OTLP metrics")
        if timeout is None:
            timeout = OTLP_POLL_TIMEOUT
        start = time.time()
        data_points: List = []
        while time.time() - start < timeout:
            data_points = self._peek_function_duration_data_points()
            if len(data_points) >= min_count:
                return data_points
            time.sleep(OTLP_POLL_INTERVAL)
        if len(data_points) < min_count:
            self.fail(
                f"Timed out waiting for {min_count} '{self._FUNCTION_DURATION_METRIC_NAME}' "
                f"histogram data point(s). Found {len(data_points)} after {timeout}s."
            )
        return data_points

    # EndpointErrorMetrics flows through a single OTel Counter:
    #
    #   - count (Sum, monotonic): per-endpoint, per-exception-type error count
    #
    # Dimensions: Telemetry.Source, service_name, environment, operation, exception.

    _ERROR_COUNT_METRIC_NAME: str = "count"

    def _peek_error_count_data_points(self) -> List:
        """Return all data points for the `count` error Counter (non-blocking)."""
        if self.mock_collector_client is None:
            return []
        # exact_match=False so we don't time out when other metrics aren't present yet.
        try:
            metrics = self.mock_collector_client.get_metrics({self._ERROR_COUNT_METRIC_NAME}, exact_match=False)
        except RuntimeError:
            return []
        data_points: List = []
        for rsm in metrics:
            if rsm.metric.name != self._ERROR_COUNT_METRIC_NAME:
                continue
            if rsm.metric.WhichOneof("data") == "sum":
                data_points.extend(rsm.metric.sum.data_points)
        return data_points

    def wait_for_error_count_metric(self, min_count: int = 1, timeout: Optional[float] = None) -> List:
        """Poll until at least min_count data points appear in the `count` error Counter."""
        if self.mock_collector_client is None:
            self.fail("Mock collector not initialized — cannot poll OTLP metrics")
        if timeout is None:
            timeout = OTLP_POLL_TIMEOUT
        start = time.time()
        data_points: List = []
        while time.time() - start < timeout:
            data_points = self._peek_error_count_data_points()
            if len(data_points) >= min_count:
                return data_points
            time.sleep(OTLP_POLL_INTERVAL)
        if len(data_points) < min_count:
            self.fail(
                f"Timed out waiting for {min_count} '{self._ERROR_COUNT_METRIC_NAME}' "
                f"Counter data point(s). Found {len(data_points)} after {timeout}s."
            )
        return data_points

    @classmethod
    def dp_attrs(cls, data_point) -> Dict[str, Any]:
        """Return a metric data point's attributes as a flat {key: python_value} dict."""
        return {kv.key: cls._any_value_to_python(kv.value) for kv in data_point.attributes}

    @staticmethod
    def dp_value(data_point) -> float:
        """Return a NumberDataPoint's value, regardless of int/double oneof."""
        if data_point.WhichOneof("value") == "as_int":
            return data_point.as_int
        return data_point.as_double

    def assert_function_duration_data_point(self, data_point, **kwargs) -> None:
        """Assert a `service.function.duration` data point has the expected attribute structure.

        Note: ``exception.type`` is intentionally NOT a histogram dimension —
        the only error signal on this metric is ``status="error"``. Exception
        class names live on the IncidentSnapshot log signal so cardinality stays
        bounded. Use ``status`` here, and assert the class on incident snapshot
        logs (see ``assert_incident_snapshot``).
        """
        attrs = self.dp_attrs(data_point)
        self.assertIn("function.name", attrs)
        self.assertGreater(data_point.count, 0, "Expected histogram data point count > 0")

        if "function_name" in kwargs:
            self.assertEqual(attrs["function.name"], kwargs["function_name"])
        if "status" in kwargs:
            self.assertEqual(attrs.get("status"), kwargs["status"])
        if "has_caller" in kwargs and kwargs["has_caller"]:
            self.assertIn("aws.service_events.caller", attrs)

    # -------------------------------------------------------------------------
    # Request helper
    # -------------------------------------------------------------------------

    def send_request(self, method: str, path: str, **kwargs) -> Response:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        return request(method, url, timeout=20, **kwargs)

    # -------------------------------------------------------------------------
    # Assertion helpers for OTLP LogRecords
    # -------------------------------------------------------------------------

    def assert_endpoint_summary(self, log, **kwargs) -> None:
        """Assert an EndpointSummary OTLP log has expected attributes and body structure."""
        attrs = self.attrs(log)
        body = self.body(log)

        self.assertEqual(attrs.get("event.name"), "aws.service_events.endpoint_summary")
        self.assertIn("http.request.method", attrs)
        self.assertIn("url.route", attrs)
        self.assertIn("aws.service_events.operation", attrs)
        self.assertIn("aws.service_events.request.count", attrs)
        # incident.count is always emitted (0 when no incidents fired this window).
        self.assertIn("aws.service_events.incident.count", attrs)
        self.assertIsInstance(body, dict)
        self.assertIn("duration", body)

        if "method" in kwargs:
            self.assertEqual(attrs["http.request.method"], kwargs["method"])
        if "route" in kwargs:
            self.assertEqual(attrs["url.route"], kwargs["route"])
        if "operation" in kwargs:
            self.assertEqual(attrs["aws.service_events.operation"], kwargs["operation"])
        if "min_count" in kwargs:
            self.assertGreaterEqual(attrs["aws.service_events.request.count"], kwargs["min_count"])
        if "min_incident_count" in kwargs:
            self.assertGreaterEqual(attrs["aws.service_events.incident.count"], kwargs["min_incident_count"])
        if "has_faults" in kwargs and kwargs["has_faults"]:
            self.assertGreater(attrs.get("aws.service_events.request.faults", 0), 0, "Expected faults > 0")
        if "no_faults" in kwargs and kwargs["no_faults"]:
            self.assertEqual(attrs.get("aws.service_events.request.faults", 0), 0, "Expected faults == 0")
        if "has_errors" in kwargs and kwargs["has_errors"]:
            self.assertGreater(attrs.get("aws.service_events.request.errors", 0), 0, "Expected errors > 0")
        if "no_errors" in kwargs and kwargs["no_errors"]:
            self.assertEqual(attrs.get("aws.service_events.request.errors", 0), 0, "Expected errors == 0")

    def assert_vcs_and_deployment_attrs(self, log) -> None:
        """Assert the VCS + deployment attributes set from env flow onto the OTLP signal.

        These ride on EndpointSummary and IncidentSnapshot via the emitter's
        _put_vcs_and_deployment_attrs(), sourced from the OTEL_AWS_SERVICE_EVENTS_*
        env vars wired in setUp.
        """
        attrs = self.attrs(log)
        self.assertEqual(attrs.get("vcs.ref.head.revision"), TEST_GIT_COMMIT_SHA)
        self.assertEqual(attrs.get("vcs.repository.url.full"), TEST_GIT_REPO_URL)
        self.assertEqual(attrs.get("aws.service_events.deployment.id"), TEST_DEPLOYMENT_ID)

    def assert_incident_snapshot(self, log, **kwargs) -> None:
        """Assert an IncidentSnapshot OTLP log has expected attributes and body."""
        attrs = self.attrs(log)
        body = self.body(log)

        self.assertEqual(attrs.get("event.name"), "aws.service_events.incident_snapshot")
        self.assertIn("aws.service_events.snapshot_id", attrs)
        self.assertIn("aws.service_events.trigger_type", attrs)
        self.assertIn("aws.service_events.operation", attrs)
        self.assertIsInstance(body, dict)
        self.assertIn("exception_info", body)

        # Always-emitted snapshot attributes — assert their presence/contract so a
        # regression that drops them is caught regardless of the per-test kwargs.
        self.assertIn("aws.service_events.duration_ms", attrs)
        self.assertIn("aws.service_events.is_partial", attrs)
        self.assertEqual(attrs.get("aws.service_events.request.type"), "http")

        if "trigger_type" in kwargs:
            self.assertEqual(attrs["aws.service_events.trigger_type"], kwargs["trigger_type"])
        if "operation" in kwargs:
            self.assertEqual(attrs["aws.service_events.operation"], kwargs["operation"])
        if "status_code" in kwargs:
            # http.response.status_code is a top-level snapshot attribute, distinct
            # from the body's request_context.status_code.
            self.assertEqual(attrs.get("http.response.status_code"), kwargs["status_code"])
        if "exception_type" in kwargs:
            exc_info = body.get("exception_info", [])
            self.assertTrue(len(exc_info) > 0, "Expected non-empty exception_info")
            self.assertEqual(exc_info[0].get("exception_type"), kwargs["exception_type"])
        if "has_call_path" in kwargs and kwargs["has_call_path"]:
            exc_info = body.get("exception_info", [])
            self.assertTrue(len(exc_info) > 0, "Expected non-empty exception_info")
            call_path = exc_info[0].get("call_path", [])
            self.assertTrue(len(call_path) > 0, "Expected non-empty call_path")

    def assert_duration_structure(self, duration: Dict) -> None:
        """Assert a duration histogram body has the expected shape."""
        for key in ("Values", "Counts", "Max", "Min", "Count", "Sum"):
            self.assertIn(key, duration)
        self.assertGreater(duration["Count"], 0)
        self.assertGreater(duration["Sum"], 0)

    # -------------------------------------------------------------------------
    # Overridable methods
    # -------------------------------------------------------------------------

    @staticmethod
    def get_application_image_name() -> str:
        raise NotImplementedError("Subclasses must implement get_application_image_name")

    def get_application_port(self) -> int:
        return 8080

    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {}

    def get_test_config_hook_overrides(self) -> Dict[str, str]:
        """Internal test-config hook overrides (KEY -> value) injected via DEBUG_SE_TEST_CONFIG.

        These knobs are internal (no public env var); black-box tests set them through the hook.
        The base wires the fast flush cadence; subclasses extend (don't replace) for extra knobs
        like SAMPLE_TIER1_THRESHOLD.
        """
        return {
            "ENDPOINT_FLUSH_INTERVAL": SERVICE_EVENTS_FLUSH_INTERVAL_MS,
            "INCIDENT_SNAPSHOT_FLUSH_INTERVAL": SERVICE_EVENTS_FLUSH_INTERVAL_MS,
        }

    def _build_test_config_hook_value(self) -> str:
        """Serialize get_test_config_hook_overrides() into the delimited hook format."""
        return ";".join(f"{key}={value}" for key, value in self.get_test_config_hook_overrides().items())

    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def get_application_otel_service_name(self) -> str:
        return self.get_application_image_name()

    def get_application_start_timeout(self) -> int:
        return 30


class ServiceEventsContractTestBase(ServiceEventsTestInfrastructure):
    """Standard OTLP test suite inherited by all framework test classes."""

    __test__ = False

    def route_label(self, path: str) -> str:
        """Expected route label for a registered path segment.

        Flask and FastAPI route templates carry a leading slash natively, so the
        default prepends one. Django stores routes slash-less by convention, and
        ServiceEvents records them verbatim to match Application Signals (which
        derives the operation from span.name). The Django test classes override
        this to return the path unchanged.
        """
        return "/" + path

    def operation_label(self, method: str, path: str) -> str:
        """Expected ServiceEvents operation label ("METHOD route") for a path."""
        return f"{method} {self.route_label(path)}"

    def test_endpoint_summary_success(self) -> None:
        for _ in range(3):
            response = self.send_request("GET", "success")
            self.assertEqual(200, response.status_code)

        logs = self.wait_for_endpoint_summary("GET", self.route_label("success"))
        total_count = sum(self.attrs(log).get("aws.service_events.request.count", 0) for log in logs)
        total_faults = sum(self.attrs(log).get("aws.service_events.request.faults", 0) for log in logs)
        total_errors = sum(self.attrs(log).get("aws.service_events.request.errors", 0) for log in logs)
        self.assertGreaterEqual(total_count, 3)
        self.assertEqual(total_faults, 0)
        self.assertEqual(total_errors, 0)
        self.assert_endpoint_summary(
            logs[0],
            method="GET",
            route=self.route_label("success"),
            operation=self.operation_label("GET", "success"),
        )
        # Verify resource attrs carry service.name and deployment.environment.name
        resource_attrs = {
            kv.key: self._any_value_to_python(kv.value) for kv in logs[0].resource_logs.resource.attributes
        }
        self.assertEqual(resource_attrs.get("service.name"), self.get_application_otel_service_name())
        self.assertEqual(resource_attrs.get("deployment.environment.name"), "test")
        # VCS + deployment metadata (from env) ride on the EndpointSummary attributes.
        self.assert_vcs_and_deployment_attrs(logs[0])

    def test_endpoint_summary_fault(self) -> None:
        for _ in range(2):
            response = self.send_request("GET", "fault")
            self.assertEqual(500, response.status_code)

        logs = self.wait_for_endpoint_summary("GET", self.route_label("fault"))
        total_faults = sum(self.attrs(log).get("aws.service_events.request.faults", 0) for log in logs)
        self.assertGreater(total_faults, 0, "Expected faults > 0")

    def test_endpoint_summary_duration(self) -> None:
        self.send_request("GET", "success")

        logs = self.wait_for_endpoint_summary("GET", self.route_label("success"))
        self.assert_duration_structure(self.body(logs[0])["duration"])

    def test_function_call_records_exist(self) -> None:
        """FunctionCall telemetry now flows through the service.function.duration histogram."""
        for _ in range(3):
            self.send_request("GET", "success")

        data_points = self.wait_for_function_duration_metric()
        self.assertGreater(len(data_points), 0)
        for dp in data_points:
            self.assert_function_duration_data_point(dp)

        # At least one data point should carry the caller attribute (nested calls).
        has_caller = any("aws.service_events.caller" in self.dp_attrs(dp) for dp in data_points)
        self.assertTrue(has_caller, "Expected at least one data point with 'aws.service_events.caller'")

    # NOTE: operation (e.g., "GET /success") is intentionally NOT a histogram
    # attribute on `service.function.duration`. Tagging by operation × function ×
    # status × exception.type would balloon attribute cardinality without bound.
    # Operation→function correlation lives on the EndpointSummary log signal,
    # asserted in test_endpoint_summary_basic_attributes via the
    # `operation="GET /success"` kwarg.

    def test_incident_snapshot_on_exception(self) -> None:
        response = self.send_request("GET", "exception")
        self.assertEqual(500, response.status_code)

        logs = self.wait_for_otlp_logs("aws.service_events.incident_snapshot")
        self.assertGreater(len(logs), 0)
        self.assert_incident_snapshot(
            logs[0],
            trigger_type="exception",
            exception_type="ValueError",
            operation=self.operation_label("GET", "exception"),
            status_code=500,
        )
        # Verify trace context present (trace_id and span_id are bytes in OTLP proto).
        self.assertTrue(any(logs[0].log_record.trace_id), "Expected non-zero trace_id")
        self.assertTrue(any(logs[0].log_record.span_id), "Expected non-zero span_id")
        # The OTLP proto LogRecord exposes the trace flags as `flags`; SAMPLED (1)
        # is set when both trace_id and span_id are present.
        self.assertEqual(logs[0].log_record.flags, 1, "Expected SAMPLED trace flags")
        # VCS + deployment metadata (from env) also ride on IncidentSnapshot attributes.
        self.assert_vcs_and_deployment_attrs(logs[0])
        # Verify request_context body fields
        body = self.body(logs[0])
        req_ctx = body.get("request_context", {})
        self.assertEqual(req_ctx.get("type"), "http")
        self.assertEqual(req_ctx.get("status_code"), 500)

    def test_endpoint_error_metrics_counter(self) -> None:
        """`/exception` (500, ValueError) populates the EndpointErrorMetrics `count`
        Counter with the per-exception-type breakdown, across every framework.

        Unlike the EndpointSummary error fields, this is a distinct OTel Counter
        (name=count, unit=Count) carrying the exception class as a dimension — which
        EndpointSummary does not — so the backend can break errors down by exception
        type. Only endpoints raising an exception (status>=400 with extracted
        error_info) populate it; /error (400, no exception) does not. The exception
        breakdown also surfaces in the EndpointSummary body's exception_breakdown.
        """
        for _ in range(3):
            response = self.send_request("GET", "exception")
            self.assertEqual(500, response.status_code)

        expected_operation = self.operation_label("GET", "exception")
        data_points = self.wait_for_error_count_metric()
        matching = [
            dp
            for dp in data_points
            if self.dp_attrs(dp).get("operation") == expected_operation
            and self.dp_attrs(dp).get("exception") == "ValueError"
        ]
        self.assertGreater(
            len(matching),
            0,
            f"Expected a `count` data point with operation='{expected_operation}' and exception='ValueError'",
        )
        dp = matching[0]
        attrs = self.dp_attrs(dp)
        self.assertEqual(attrs.get("Telemetry.Source"), "ServiceEvents")
        self.assertGreaterEqual(self.dp_value(dp), 1, "Expected error count >= 1")

        # The same per-exception-type breakdown surfaces in the EndpointSummary body.
        summary_logs = self.wait_for_endpoint_summary("GET", self.route_label("exception"))
        breakdown = None
        for log in summary_logs:
            body = self.body(log)
            if isinstance(body, dict) and body.get("exception_breakdown"):
                breakdown = body["exception_breakdown"]
                break
        self.assertIsNotNone(breakdown, "Expected a non-empty exception_breakdown in the EndpointSummary body")
        exception_types = {
            exc.get("exception_type")
            for entry in breakdown
            for exc in entry.get("exceptions", [])
            if isinstance(exc, dict)
        }
        self.assertIn("ValueError", exception_types)

    def test_incident_snapshot_on_fault(self) -> None:
        response = self.send_request("GET", "fault")
        self.assertEqual(500, response.status_code)

        logs = self.wait_for_otlp_logs("aws.service_events.incident_snapshot")
        self.assertGreater(len(logs), 0)
        self.assert_incident_snapshot(logs[0], trigger_type="exception", exception_type="RuntimeError")

    def test_incident_snapshot_has_call_path(self) -> None:
        self.send_request("GET", "exception")

        logs = self.wait_for_otlp_logs("aws.service_events.incident_snapshot")
        self.assertGreater(len(logs), 0)
        self.assert_incident_snapshot(logs[0], has_call_path=True)

    def test_deployment_event_exported(self) -> None:
        self.send_request("GET", "success")

        logs = self.wait_for_otlp_logs("aws.service_events.deployment_event")
        self.assertGreaterEqual(len(logs), 1)
        self.assertEqual(logs[0].scope_logs.scope.name, "serviceevents")
        self.assertEqual(logs[0].scope_logs.scope.version, "1.0")
        # The collector stamps a trigger on every emit; the first (startup) emit is
        # always present, and any later emits are "periodic". Assert the contract.
        triggers = [self.attrs(log).get("aws.service_events.deployment.trigger") for log in logs]
        for trigger in triggers:
            self.assertIn(trigger, ("startup", "periodic", "shutdown"))
        self.assertIn("startup", triggers, "Expected the first DeploymentEvent to carry trigger='startup'")

        # DeploymentContext (from OTEL_AWS_SERVICE_EVENTS_* env) rides on this signal.
        # deployment.url / deployment.timestamp are emitted ONLY here, so assert them.
        attrs = self.attrs(logs[0])
        self.assertEqual(attrs.get("aws.service_events.deployment.id"), TEST_DEPLOYMENT_ID)
        self.assertEqual(attrs.get("vcs.ref.head.revision"), TEST_GIT_COMMIT_SHA)
        self.assertEqual(attrs.get("vcs.repository.url.full"), TEST_GIT_REPO_URL)
        self.assertEqual(attrs.get("aws.service_events.deployment.url"), TEST_DEPLOYMENT_URL)
        self.assertEqual(attrs.get("aws.service_events.deployment.timestamp"), TEST_DEPLOYMENT_TIMESTAMP)

    def test_all_telemetry_types_present(self) -> None:
        self.send_request("GET", "success")
        self.send_request("GET", "exception")

        # FunctionCall is now a histogram metric, not an OTLP log.
        self.wait_for_function_duration_metric()
        self.wait_for_otlp_logs("aws.service_events.endpoint_summary")
        self.wait_for_otlp_logs("aws.service_events.incident_snapshot")
        self.wait_for_otlp_logs("aws.service_events.deployment_event")
