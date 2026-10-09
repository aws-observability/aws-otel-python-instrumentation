# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Wire-level tests for OTLPAwsMetricExporter.

These assert on the headers that actually reach the transport, rather than on configuration alone.
The distinction matters: a single ``Authorization`` value is the difference between a request
authenticated as the intended principal and one that is malformed or silently re-authenticated.
``get_all`` semantics are emulated by capturing the full header mapping passed to the session.
"""

import os
from unittest import TestCase
from unittest.mock import patch

import requests
from botocore.session import Session

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.environment_variables import (
    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE,
)
from amazon.opentelemetry.distro.exporter.otlp.aws.metrics.otlp_aws_metric_exporter import OTLPAwsMetricExporter
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._configuration import _get_exporter_names, _import_exporters
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_HEADERS,
    OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TIMEOUT,
)
from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics.export import AggregationTemporality

_COMMERCIAL_ENDPOINT = "https://monitoring.us-east-1.amazonaws.com/v1/metrics"
_CHINA_ENDPOINT = "https://monitoring.cn-north-1.amazonaws.com.cn/v1/metrics"


class TestOTLPAwsMetricExporter(TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-access-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret-key",
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_CONFIG_FILE": os.devnull,
                "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            },
            clear=True,
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

        self._saved = {
            k: os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
        }
        os.environ["AWS_ACCESS_KEY_ID"] = "AKIAIOSFODNN7EXAMPLE"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        os.environ.pop("AWS_SESSION_TOKEN", None)

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _exporter(self, endpoint, region, **kwargs):
        return OTLPAwsMetricExporter(aws_region=region, session=Session(), endpoint=endpoint, **kwargs)

    def test_uses_aws_auth_session_with_monitoring_service(self):
        exporter = self._exporter(_COMMERCIAL_ENDPOINT, "us-east-1")

        # pylint: disable=protected-access
        self.assertIsInstance(exporter._session, AwsAuthSession)
        self.assertEqual(exporter._session._service, "monitoring")
        self.assertEqual(exporter._session._aws_region, "us-east-1")

    def test_forwards_temporality_and_aggregation(self):
        temporality = {Counter: AggregationTemporality.DELTA}
        exporter = self._exporter(_COMMERCIAL_ENDPOINT, "us-east-1", preferred_temporality=temporality)

        # pylint: disable=protected-access
        self.assertEqual(exporter._preferred_temporality[Counter], AggregationTemporality.DELTA)

    def test_does_not_force_compression(self):
        """Unlike the logs exporter, compression is not forced; see the open Content-Encoding question."""
        exporter = self._exporter(_COMMERCIAL_ENDPOINT, "us-east-1")

        # pylint: disable=protected-access
        self.assertNotEqual(getattr(exporter._compression, "value", None), "gzip")

    def _capture_signed_headers(self, endpoint, region):
        """Sign a request through the real session and return the headers handed to the transport."""
        exporter = self._exporter(endpoint, region)
        captured = {}

        def fake_send(self, method, url, *args, data=None, headers=None, **kwargs):  # noqa: ARG001
            captured.update(headers or {})
            response = requests.Response()
            response.status_code = 200
            response._content = b""
            return response

        with patch.object(requests.Session, "request", fake_send):
            # pylint: disable=protected-access
            exporter._session.post(endpoint, data=b"payload")
        return captured

    def test_exactly_one_authorization_header_and_it_is_sigv4(self):
        for endpoint, region in ((_COMMERCIAL_ENDPOINT, "us-east-1"), (_CHINA_ENDPOINT, "cn-north-1")):
            with self.subTest(region=region):
                headers = self._capture_signed_headers(endpoint, region)

                auth_values = [v for k, v in headers.items() if k.lower() == "authorization"]
                self.assertEqual(len(auth_values), 1, "expected exactly one Authorization header")
                self.assertTrue(auth_values[0].startswith("AWS4-HMAC-SHA256"))

    def test_signature_scope_matches_region_and_monitoring_service(self):
        for endpoint, region in ((_COMMERCIAL_ENDPOINT, "us-east-1"), (_CHINA_ENDPOINT, "cn-north-1")):
            with self.subTest(region=region):
                headers = self._capture_signed_headers(endpoint, region)
                auth = next(v for k, v in headers.items() if k.lower() == "authorization")

                self.assertIn(f"/{region}/monitoring/aws4_request", auth)
                # host must be part of the signature so the signature is bound to the endpoint
                self.assertIn("host", auth)

    # pylint: disable=protected-access
    @patch.dict(
        os.environ,
        {
            OTEL_METRICS_EXPORTER: "otlp/sigv4",
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "https://monitoring.us-west-2.amazonaws.com/v1/metrics",
        },
    )
    def test_should_load_otlp_sigv4_metric_exporter(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[1]
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsMetricExporter)
        exporter = exporters["otlp/sigv4"]()
        self.addCleanup(exporter.shutdown)
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        with patch.object(requests.Session, "request", return_value=response) as request:
            exporter._session.post(exporter._client._endpoint, data=b"payload")
        self.assertEqual(request.call_count, 1)
        headers = request.call_args.kwargs["headers"]
        authorization = [value for (key, value) in headers.items() if key.lower() == "authorization"]
        self.assertEqual(len(authorization), 1)
        self.assertTrue(authorization[0].startswith("AWS4-HMAC-SHA256"))
        self.assertIn("/us-west-2/monitoring/aws4_request", authorization[0])
        self.assertEqual(request.call_args.kwargs["url"], "https://monitoring.us-west-2.amazonaws.com/v1/metrics")
        self.assertTrue(request.call_args.kwargs["data"])

    @patch.dict(
        os.environ,
        {
            OTEL_TRACES_EXPORTER: "none",
            OTEL_METRICS_EXPORTER: "otlp/sigv4,otlp",
            OTEL_LOGS_EXPORTER: "none",
            OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
        },
    )
    def test_should_load_otlp_sigv4_metric_exporter_alongside_otlp_exporter(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[1]
        self.assertEqual(set(exporters), {"otlp/sigv4", "otlp_proto_http"})
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsMetricExporter)
        self.assertIs(exporters["otlp_proto_http"], OTLPMetricExporter)
        self.assertFalse(loaded[0])
        self.assertFalse(loaded[2])

    def test_should_resolve_otlp_sigv4_metric_exporter_region_from_endpoint_across_partitions(self):
        partitions = (
            ("us-west-2", "amazonaws.com"),
            ("cn-north-1", "amazonaws.com.cn"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
        )
        for region, suffix in partitions:
            endpoint = f"https://monitoring.{region}.{suffix}/v1/metrics"
            with self.subTest(region=region), patch.dict(os.environ, {OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: endpoint}):
                exporter = OTLPAwsMetricExporter()
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._aws_region, region)

    def test_should_use_otlp_sigv4_metric_exporter_with_generic_endpoint_and_environment_region(self):
        with patch.dict(
            os.environ,
            {OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example.com", "AWS_DEFAULT_REGION": "us-east-2"},
        ):
            exporter = OTLPAwsMetricExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-east-2")
            self.assertEqual(exporter._client._endpoint, "https://collector.example.com/v1/metrics")

    def test_should_resolve_otlp_sigv4_metric_exporter_region_from_generic_aws_endpoint(self):
        with patch.dict(os.environ, {OTEL_EXPORTER_OTLP_ENDPOINT: "https://xray.us-west-2.amazonaws.com/base/"}):
            exporter = OTLPAwsMetricExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-west-2")
            self.assertEqual(exporter._client._endpoint, "https://xray.us-west-2.amazonaws.com/base/v1/metrics")

    def test_should_apply_otlp_sigv4_metric_exporter_signing_service_precedence(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        test_cases = (
            ({}, None, "monitoring"),
            ({OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service"}, None, "shared-service"),
            (
                {
                    OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service",
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                None,
                "metrics-service",
            ),
            (
                {OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service", OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: ""},
                None,
                "shared-service",
            ),
            (
                {OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "", OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: ""},
                None,
                "monitoring",
            ),
            (
                {
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                None,
                "monitoring",
            ),
            (
                {
                    OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service",
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                "explicit-service",
                "explicit-service",
            ),
        )
        for environment, explicit_service, expected_service in test_cases:
            with self.subTest(environment=environment, explicit_service=explicit_service), patch.dict(
                os.environ,
                {
                    "AWS_REGION": "us-west-2",
                    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example.com",
                    **environment,
                },
            ):
                exporter = OTLPAwsMetricExporter(aws_service=explicit_service)
                self.addCleanup(exporter.shutdown)
                with patch.object(requests.Session, "request", return_value=response) as request:
                    exporter._session.post(exporter._client._endpoint, data=b"payload")
                self.assertEqual(request.call_count, 1)
                self.assertIn(
                    f"/us-west-2/{expected_service}/aws4_request", request.call_args.kwargs["headers"]["Authorization"]
                )

    def test_should_preserve_otlp_sigv4_metric_exporter_explicit_region_and_session(self):
        session = Session()
        exporter = OTLPAwsMetricExporter(
            aws_region="us-east-1",
            session=session,
            endpoint="https://monitoring.us-west-2.amazonaws.com/v1/metrics",
        )
        self.addCleanup(exporter.shutdown)
        self.assertEqual(exporter._aws_region, "us-east-1")
        self.assertIs(exporter._session._session, session)

    def test_should_configure_otlp_sigv4_metric_exporter_from_environment_with_constructor_overrides(self):
        with patch.dict(
            os.environ,
            {
                "AWS_REGION": "us-east-1",
                OTEL_EXPORTER_OTLP_ENDPOINT: "https://generic.example.com",
                OTEL_EXPORTER_OTLP_HEADERS: "x-custom=generic",
                OTEL_EXPORTER_OTLP_TIMEOUT: "10",
                OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "https://signal.example.com",
                OTEL_EXPORTER_OTLP_METRICS_HEADERS: "x-custom=signal",
                OTEL_EXPORTER_OTLP_METRICS_TIMEOUT: "20",
            },
        ):
            exporter = OTLPAwsMetricExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://signal.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "signal")
            self.assertEqual(exporter._client._timeout, 20)
            exporter = OTLPAwsMetricExporter(
                endpoint="https://explicit.example.com", headers={"x-custom": "explicit"}, timeout=30
            )
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://explicit.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "explicit")
            self.assertEqual(exporter._client._timeout, 30)

    def test_should_fail_to_initialize_otlp_sigv4_metric_exporter_when_region_cannot_be_resolved(self):
        session = Session()
        session.set_config_variable("region", None)
        with self.assertRaisesRegex(ValueError, "requires an AWS endpoint region"):
            OTLPAwsMetricExporter(session=session, endpoint="https://collector.example.com")
