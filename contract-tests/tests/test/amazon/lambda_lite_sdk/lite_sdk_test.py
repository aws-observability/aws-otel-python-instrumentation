# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""End-to-end contract tests for the OTel Lite SDK Lambda mode.

Tests verify that:
- Spans are emitted via UDP in the correct OTLP format
- Multiple instrumentation scopes produce separate ScopeSpans
- Application Signals attributes are injected correctly
- Sampled/unsampled prefixes (T1S/T1U) are used correctly
- Parent-child span relationships are preserved
- Span flags are encoded correctly
"""

import time
from logging import INFO, Logger, getLogger
from typing import Dict, List
from unittest import TestCase

from docker import DockerClient
from docker.models.networks import Network, NetworkCollection
from docker.types import EndpointConfig
from requests import request
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

NETWORK_NAME: str = "lite-sdk-test-network"
_MOCK_UDP_COLLECTOR_ALIAS: str = "udp-collector"
_MOCK_UDP_COLLECTOR_NAME: str = "mock-udp-collector"
_MOCK_UDP_COLLECTOR_HTTP_PORT: int = 8080
_APPLICATION_NAME: str = "lambda-lite-sdk"
_APPLICATION_PORT: int = 8080

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)


class LiteSdkContractTest(TestCase):
    """E2E contract tests for the OTel Lite SDK."""

    application: DockerContainer
    mock_collector: DockerContainer
    network: Network

    @classmethod
    def setUpClass(cls) -> None:
        cls.addClassCleanup(cls.class_tear_down)
        cls.network = NetworkCollection(client=DockerClient()).create(NETWORK_NAME)

        collector_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_MOCK_UDP_COLLECTOR_ALIAS])
        }
        cls.mock_collector = (
            DockerContainer(_MOCK_UDP_COLLECTOR_NAME)
            .with_exposed_ports(_MOCK_UDP_COLLECTOR_HTTP_PORT)
            .with_name(_MOCK_UDP_COLLECTOR_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=collector_networking_config)
        )
        cls.mock_collector.start()
        wait_for_logs(cls.mock_collector, "Ready", timeout=20)

    @classmethod
    def class_tear_down(cls) -> None:
        try:
            _logger.info("MockUdpCollector stdout: %s", cls.mock_collector.get_logs()[0].decode())
            cls.mock_collector.stop()
        except Exception:
            _logger.exception("Failed to tear down mock UDP collector")
        cls.network.remove()

    def setUp(self) -> None:
        self.addCleanup(self.tear_down)
        app_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_APPLICATION_NAME])
        }
        self.application = (
            DockerContainer(_APPLICATION_NAME)
            .with_exposed_ports(_APPLICATION_PORT)
            .with_env("AWS_LAMBDA_LITE_MODE", "true")
            .with_env("AWS_LAMBDA_FUNCTION_NAME", "my-function")
            .with_env("AWS_REGION", "us-west-2")
            .with_env("OTEL_SERVICE_NAME", "my-function")
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "cloud.region=us-west-2,cloud.platform=aws_lambda,cloud.provider=aws")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "true")
            .with_env("AWS_XRAY_DAEMON_ADDRESS", f"{_MOCK_UDP_COLLECTOR_ALIAS}:2000")
            .with_name(_APPLICATION_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=app_networking_config)
        )
        self.application.start()
        wait_for_logs(self.application, "Ready", timeout=20)
        self._clear_collector()

    def tear_down(self) -> None:
        try:
            _logger.info("Application stdout: %s", self.application.get_logs()[0].decode())
            self.application.stop()
        except Exception:
            _logger.exception("Failed to tear down application")
        self._clear_collector()

    def _get_collector_url(self) -> str:
        host = self.mock_collector.get_container_host_ip()
        port = self.mock_collector.get_exposed_port(_MOCK_UDP_COLLECTOR_HTTP_PORT)
        return f"http://{host}:{port}"

    def _get_spans(self, timeout: float = 10.0) -> List[dict]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = request("GET", f"{self._get_collector_url()}/spans", timeout=5)
            spans = resp.json()
            if spans:
                return spans
            time.sleep(0.5)
        return []

    def _clear_collector(self) -> None:
        try:
            request("DELETE", f"{self._get_collector_url()}/spans", timeout=5)
        except Exception:
            pass

    def _invoke(self) -> dict:
        return self._invoke_path("/invoke")

    def _invoke_path(self, path: str, headers: dict = None) -> dict:
        host = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(_APPLICATION_PORT)
        resp = request("GET", f"http://{host}:{port}{path}", timeout=10, headers=headers)
        return resp.json()

    def test_spans_emitted_via_udp(self):
        """Verify spans arrive at the mock UDP collector."""
        self._invoke()
        spans = self._get_spans()
        self.assertGreater(len(spans), 0, "No spans received by UDP collector")

    def test_multiple_scopes_grouped_correctly(self):
        """Verify spans from different instrumentors have distinct scope names."""
        self._invoke()
        spans = self._get_spans()
        scope_names = {s["scope_name"] for s in spans}
        self.assertIn("opentelemetry.instrumentation.aws_lambda", scope_names)
        self.assertIn("opentelemetry.instrumentation.botocore", scope_names)

    def test_parent_child_relationship(self):
        """Verify child span references parent span ID."""
        self._invoke()
        spans = self._get_spans()
        server_spans = [s for s in spans if s["kind"] == 2]  # SERVER
        client_spans = [s for s in spans if s["kind"] == 3]  # CLIENT
        self.assertEqual(len(server_spans), 1)
        self.assertEqual(len(client_spans), 1)

        server = server_spans[0]
        client = client_spans[0]
        self.assertEqual(client["trace_id"], server["trace_id"])
        self.assertEqual(client["parent_span_id"], server["span_id"])

    def test_app_signals_attributes_injected(self):
        """Verify Application Signals attributes are present on spans."""
        self._invoke()
        spans = self._get_spans()
        server_spans = [s for s in spans if s["kind"] == 2]
        self.assertEqual(len(server_spans), 1)
        attrs = server_spans[0]["attributes"]
        self.assertEqual(attrs.get("aws.local.service"), "my-function")
        self.assertEqual(attrs.get("aws.local.operation"), "my-function/FunctionHandler")
        self.assertEqual(attrs.get("aws.local.environment"), "lambda:default")

    def test_client_span_remote_attributes(self):
        """Verify remote service/operation attributes on client spans."""
        self._invoke()
        spans = self._get_spans()
        client_spans = [s for s in spans if s["kind"] == 3]
        self.assertEqual(len(client_spans), 1)
        attrs = client_spans[0]["attributes"]
        self.assertEqual(attrs.get("aws.remote.service"), "AWS::S3")
        self.assertEqual(attrs.get("aws.remote.operation"), "ListBuckets")

    def test_sampled_prefix_used(self):
        """Verify T1S prefix is used for sampled spans (root spans are always sampled)."""
        self._invoke()
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        for span in spans:
            self.assertEqual(span["prefix"], "T1S")

    def test_span_flags_encoded(self):
        """Verify span flags field contains HAS_IS_REMOTE and sampled bit."""
        self._invoke()
        spans = self._get_spans()
        for span in spans:
            flags = span["flags"]
            self.assertTrue(flags & 0x100, "HAS_IS_REMOTE bit not set")
            self.assertTrue(flags & 0x01, "Sampled bit not set for root spans")

    def test_resource_attributes_present(self):
        """Verify resource attributes are encoded in the OTLP payload."""
        self._invoke()
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        resource = spans[0]["resource"]
        self.assertEqual(resource.get("service.name"), "my-function")
        self.assertEqual(resource.get("cloud.region"), "us-west-2")

    def test_span_timing(self):
        """Verify start_time and end_time are populated and sensible."""
        self._invoke()
        spans = self._get_spans()
        for span in spans:
            self.assertGreater(span["start_time"], 0)
            self.assertGreater(span["end_time"], 0)
            self.assertGreaterEqual(span["end_time"], span["start_time"])

    def test_multiple_invocations(self):
        """Verify multiple invocations each produce separate span batches."""
        self._invoke()
        self._invoke()
        self._invoke()
        spans = self._get_spans()
        trace_ids = {s["trace_id"] for s in spans}
        self.assertGreaterEqual(len(trace_ids), 3, "Expected at least 3 distinct trace IDs")

    def test_exception_recording(self):
        """Verify exception events are encoded and arrive at the collector."""
        self._invoke_path("/invoke-error")
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        span = spans[0]
        self.assertEqual(span["status_code"], 2)  # ERROR
        self.assertEqual(span["status_message"], "handler failed")
        self.assertGreater(len(span["events"]), 0)
        event_names = [e["name"] for e in span["events"]]
        self.assertIn("exception", event_names)

    def test_xray_trace_context_propagation(self):
        """Verify X-Ray trace ID is inherited from incoming header."""
        xray_header = "Root=1-5fb73311-05e8bb83207fa31d4d9cdb4c;Parent=3328b8445a6dbad2;Sampled=1"
        self._invoke_path("/invoke-with-context", headers={"X-Amzn-Trace-Id": xray_header})
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        expected_trace_id = "5fb7331105e8bb83207fa31d4d9cdb4c"
        self.assertEqual(spans[0]["trace_id"], expected_trace_id)
        self.assertEqual(spans[0]["prefix"], "T1S")

    def test_unsampled_trace_uses_t1u_prefix(self):
        """Verify T1U prefix for unsampled trace context."""
        xray_header = "Root=1-5fb73311-05e8bb83207fa31d4d9cdb4c;Parent=3328b8445a6dbad2;Sampled=0"
        self._invoke_path("/invoke-with-context", headers={"X-Amzn-Trace-Id": xray_header})
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        self.assertEqual(spans[0]["prefix"], "T1U")
        flags = spans[0]["flags"]
        self.assertTrue(flags & 0x100, "HAS_IS_REMOTE bit not set")
        self.assertFalse(flags & 0x01, "Sampled bit should not be set")

    def test_varied_attribute_types(self):
        """Verify negative ints, zero, booleans, and floats survive encode/decode."""
        self._invoke_path("/invoke-attributes")
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        attrs = spans[0]["attributes"]
        self.assertEqual(attrs["int.positive"], 42)
        self.assertEqual(attrs["int.negative"], -1)
        self.assertEqual(attrs["int.zero"], 0)
        self.assertAlmostEqual(attrs["float.value"], 3.14, places=2)
        self.assertEqual(attrs["bool.true"], True)
        self.assertEqual(attrs["bool.false"], False)
        self.assertEqual(attrs["string.value"], "hello")
        self.assertEqual(attrs["string.empty"], "")

    def test_large_payload_not_truncated(self):
        """Verify spans with many attributes are delivered without crash."""
        self._invoke_path("/invoke-large")
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        attrs = spans[0]["attributes"]
        self.assertGreaterEqual(len(attrs), 50)

    def test_app_signals_disabled_no_injection(self):
        """Verify no aws.local.* attributes when app signals is disabled."""
        self.application.stop()
        self._clear_collector()

        app_networking_config = {NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_APPLICATION_NAME])}
        self.application = (
            DockerContainer(_APPLICATION_NAME)
            .with_exposed_ports(_APPLICATION_PORT)
            .with_env("AWS_LAMBDA_LITE_MODE", "true")
            .with_env("AWS_LAMBDA_FUNCTION_NAME", "my-function")
            .with_env("AWS_REGION", "us-west-2")
            .with_env("OTEL_SERVICE_NAME", "my-function")
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "cloud.region=us-west-2,cloud.platform=aws_lambda")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false")
            .with_env("AWS_XRAY_DAEMON_ADDRESS", f"{_MOCK_UDP_COLLECTOR_ALIAS}:2000")
            .with_name(_APPLICATION_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=app_networking_config)
        )
        self.application.start()
        wait_for_logs(self.application, "Ready", timeout=20)

        self._invoke()
        spans = self._get_spans()
        self.assertGreater(len(spans), 0)
        attrs = spans[0]["attributes"]
        self.assertNotIn("aws.local.service", attrs)
        self.assertNotIn("aws.local.operation", attrs)

    def test_silent_udp_drop_no_crash(self):
        """Verify app responds normally when UDP datagrams are silently dropped."""
        self.application.stop()
        self._clear_collector()

        app_networking_config = {NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_APPLICATION_NAME])}
        self.application = (
            DockerContainer(_APPLICATION_NAME)
            .with_exposed_ports(_APPLICATION_PORT)
            .with_env("AWS_LAMBDA_LITE_MODE", "true")
            .with_env("AWS_LAMBDA_FUNCTION_NAME", "my-function")
            .with_env("AWS_REGION", "us-west-2")
            .with_env("OTEL_SERVICE_NAME", "my-function")
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "cloud.region=us-west-2,cloud.platform=aws_lambda")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false")
            .with_env("AWS_XRAY_DAEMON_ADDRESS", "192.0.2.1:2000")
            .with_name(_APPLICATION_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=app_networking_config)
        )
        self.application.start()
        wait_for_logs(self.application, "Ready", timeout=20)

        host = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(_APPLICATION_PORT)
        resp = request("GET", f"http://{host}:{port}/invoke", timeout=10)
        self.assertEqual(resp.status_code, 200)

    def test_malformed_endpoint_no_crash(self):
        """Verify app starts gracefully when daemon address is malformed."""
        self.application.stop()
        self._clear_collector()

        app_networking_config = {NETWORK_NAME: EndpointConfig(version="1.22", aliases=[_APPLICATION_NAME])}
        self.application = (
            DockerContainer(_APPLICATION_NAME)
            .with_exposed_ports(_APPLICATION_PORT)
            .with_env("AWS_LAMBDA_LITE_MODE", "true")
            .with_env("AWS_LAMBDA_FUNCTION_NAME", "my-function")
            .with_env("AWS_REGION", "us-west-2")
            .with_env("OTEL_SERVICE_NAME", "my-function")
            .with_env("OTEL_RESOURCE_ATTRIBUTES", "cloud.region=us-west-2,cloud.platform=aws_lambda")
            .with_env("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false")
            .with_env("AWS_XRAY_DAEMON_ADDRESS", "invalid-no-port")
            .with_name(_APPLICATION_NAME)
            .with_kwargs(network=NETWORK_NAME, networking_config=app_networking_config)
        )
        self.application.start()
        wait_for_logs(self.application, "Ready", timeout=20)

        host = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(_APPLICATION_PORT)
        resp = request("GET", f"http://{host}:{port}/invoke", timeout=10)
        self.assertEqual(resp.status_code, 200)
