# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from unittest import TestCase
from unittest.mock import patch

import requests

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_sigv4_session_factory import (
    AWS_SIGV4_SERVICE,
    aws_sigv4_session,
)
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_ENDPOINT as _GENERIC_ENDPOINT
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_LOGS_ENDPOINT as _LOGS_ENDPOINT
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_METRICS_ENDPOINT as _METRICS_ENDPOINT
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_TRACES_ENDPOINT as _TRACES_ENDPOINT
from opentelemetry.util._importlib_metadata import entry_points

_PROVIDER_MODULE = "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_sigv4_session_factory"


class TestAwsSigV4SessionFactory(TestCase):
    """Unit tests for the ``aws_sigv4`` credential provider factory.

    These tests bypass OTel SDK exporter wiring; signal detection is exercised
    by patching ``_detect_signal_from_stack`` directly. End-to-end exporter
    integration is covered separately by the smoke tests.
    """

    def setUp(self):
        env_keys = (
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            AWS_SIGV4_SERVICE,
            _TRACES_ENDPOINT,
            _LOGS_ENDPOINT,
            _METRICS_ENDPOINT,
            _GENERIC_ENDPOINT,
        )
        self._saved_env = {key: os.environ.get(key) for key in env_keys}
        for key in self._saved_env:
            os.environ.pop(key, None)

    def tearDown(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    # --- entry point ---------------------------------------------------

    def test_entry_point_registered(self):
        """The 'aws_sigv4' entry point resolves to the factory function."""
        eps = list(entry_points(group="opentelemetry_otlp_credential_provider", name="aws_sigv4"))
        self.assertEqual(len(eps), 1)
        self.assertIs(eps[0].load(), aws_sigv4_session)

    # --- explicit AWS_SIGV4_SERVICE override ---------------------------

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_explicit_service_env_wins_over_endpoint_inference(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[_TRACES_ENDPOINT] = "https://xray.us-east-1.amazonaws.com/v1/traces"
        os.environ[AWS_SIGV4_SERVICE] = "observe"

        session = aws_sigv4_session()

        self.assertIsInstance(session, AwsAuthSession)
        # pylint: disable=protected-access
        self.assertEqual(session._service, "observe")
        self.assertEqual(session._aws_region, "us-east-1")

    # --- per-signal endpoint inference ---------------------------------

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_traces_endpoint_xray_pattern_resolves_xray(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ[_TRACES_ENDPOINT] = "https://xray.us-west-2.amazonaws.com/v1/traces"

        session = aws_sigv4_session()

        # pylint: disable=protected-access
        self.assertEqual(session._service, "xray")

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="logs")
    def test_logs_endpoint_logs_pattern_resolves_logs(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ[_LOGS_ENDPOINT] = "https://logs.us-west-2.amazonaws.com/v1/logs"

        session = aws_sigv4_session()

        # pylint: disable=protected-access
        self.assertEqual(session._service, "logs")

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    def test_china_trace_and_log_endpoints_resolve_signing_service(self):
        cases = (
            ("traces", _TRACES_ENDPOINT, "xray"),
            ("logs", _LOGS_ENDPOINT, "logs"),
        )

        for region in ("cn-north-1", "cn-northwest-1"):
            for signal, endpoint_env, expected_service in cases:
                with self.subTest(signal=signal, region=region), patch(
                    f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value=signal
                ), patch.dict(
                    os.environ,
                    {
                        "AWS_REGION": region,
                        endpoint_env: f"https://{expected_service}.{region}.amazonaws.com.cn/v1/{signal}",
                    },
                ):
                    session = aws_sigv4_session()

                    self.assertIsInstance(session, AwsAuthSession)
                    # pylint: disable=protected-access
                    self.assertEqual(session._service, expected_service)
                    self.assertEqual(session._aws_region, region)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    def test_other_partition_endpoints_resolve_signing_service(self):
        cases = (
            ("traces", _TRACES_ENDPOINT, "xray"),
            ("logs", _LOGS_ENDPOINT, "logs"),
            ("metrics", _METRICS_ENDPOINT, "monitoring"),
        )

        for region, dns_suffix in (
            ("us-gov-west-1", "amazonaws.com"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
            ("us-isob-east-1", "sc2s.sgov.gov"),
            ("eu-isoe-west-1", "cloud.adc-e.uk"),
            ("us-isof-south-1", "csp.hci.ic.gov"),
        ):
            for signal, endpoint_env, expected_service in cases:
                with self.subTest(signal=signal, region=region), patch(
                    f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value=signal
                ), patch.dict(
                    os.environ,
                    {
                        "AWS_REGION": region,
                        endpoint_env: f"https://{expected_service}.{region}.{dns_suffix}/v1/{signal}",
                    },
                ):
                    session = aws_sigv4_session()

                    self.assertIsInstance(session, AwsAuthSession)
                    # pylint: disable=protected-access
                    self.assertEqual(session._service, expected_service)
                    self.assertEqual(session._aws_region, region)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_iso_endpoint_with_untrusted_suffix_falls_back_to_unsigned(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-iso-east-1"
        os.environ[_TRACES_ENDPOINT] = "https://xray.us-iso-east-1.c2s.ic.gov.evil/v1/traces"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_china_endpoint_with_untrusted_suffix_falls_back_to_unsigned(self, _mock_signal):
        os.environ["AWS_REGION"] = "cn-north-1"
        os.environ[_TRACES_ENDPOINT] = "https://xray.cn-north-1.amazonaws.com.cn.evil/v1/traces"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_cloudwatch_substring_resolves_cloudwatch(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[_TRACES_ENDPOINT] = "https://my-cloudwatch-collector.example.com/v1/traces"

        session = aws_sigv4_session()

        # pylint: disable=protected-access
        self.assertEqual(session._service, "cloudwatch")

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="metrics")
    def test_metrics_endpoint_monitoring_pattern_resolves_monitoring(self, _mock_signal):
        """Anchored metrics URL pattern matches CloudWatch's monitoring OTLP endpoint."""
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ[_METRICS_ENDPOINT] = "https://monitoring.us-west-2.amazonaws.com/v1/metrics"

        session = aws_sigv4_session()

        # pylint: disable=protected-access
        self.assertEqual(session._service, "monitoring")

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="metrics")
    def test_china_metrics_endpoint_resolves_monitoring(self, _mock_signal):
        """AWS China monitoring endpoints resolve the monitoring signing service and region."""
        for region in ("cn-north-1", "cn-northwest-1"):
            with self.subTest(region=region):
                os.environ["AWS_REGION"] = region
                os.environ[_METRICS_ENDPOINT] = f"https://monitoring.{region}.amazonaws.com.cn/v1/metrics"

                session = aws_sigv4_session()

                self.assertIsInstance(session, AwsAuthSession)
                # pylint: disable=protected-access
                self.assertEqual(session._service, "monitoring")
                self.assertEqual(session._aws_region, region)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="metrics")
    def test_china_metrics_endpoint_with_untrusted_suffix_falls_back_to_unsigned(self, _mock_signal):
        """A lookalike host past the China suffix must not be signed."""
        os.environ["AWS_REGION"] = "cn-north-1"
        os.environ[_METRICS_ENDPOINT] = "https://monitoring.cn-north-1.amazonaws.com.cn.evil/v1/metrics"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="metrics")
    def test_metrics_endpoint_wrong_path_falls_back_to_unsigned(self, _mock_signal):
        """Anchored monitoring rule requires exactly /v1/metrics; other paths must not match."""
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ[_METRICS_ENDPOINT] = "https://monitoring.us-west-2.amazonaws.com/wrong/path"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_other_signals_endpoint_is_ignored(self, _mock_signal):
        """When the signal is traces, only the traces endpoint should be consulted.

        Setting LOGS endpoint must not leak into traces' service inference.
        """
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[_LOGS_ENDPOINT] = "https://logs.us-east-1.amazonaws.com/v1/logs"

        session = aws_sigv4_session()

        # No traces endpoint, no override, generic also unset -> no service
        # resolved -> unsigned fallback.
        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_falls_back_to_generic_endpoint_when_signal_endpoint_unset(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[_GENERIC_ENDPOINT] = "https://xray.us-east-1.amazonaws.com/v1/traces"

        session = aws_sigv4_session()

        # pylint: disable=protected-access
        self.assertEqual(session._service, "xray")

    # --- "no service resolved" -> unsigned fallback (no default xray) --

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_unrecognized_endpoint_falls_back_to_unsigned(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[_TRACES_ENDPOINT] = "https://collector.internal.example.com/v1/traces"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value=None)
    def test_no_signal_no_endpoint_falls_back_to_unsigned(self, _mock_signal):
        os.environ["AWS_REGION"] = "us-east-1"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    # --- environment fallbacks (no botocore / no region) ---------------

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", False)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    def test_botocore_missing_falls_back_to_unsigned(self, _mock_signal):
        os.environ[_TRACES_ENDPOINT] = "https://xray.us-west-2.amazonaws.com/v1/traces"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    @patch(f"{_PROVIDER_MODULE}.IS_BOTOCORE_INSTALLED", True)
    @patch(f"{_PROVIDER_MODULE}._detect_signal_from_stack", return_value="traces")
    @patch(f"{_PROVIDER_MODULE}.get_aws_region", return_value=None)
    def test_no_region_falls_back_to_unsigned(self, _mock_region, _mock_signal):
        os.environ[_TRACES_ENDPOINT] = "https://xray.us-west-2.amazonaws.com/v1/traces"

        session = aws_sigv4_session()

        self.assertNotIsInstance(session, AwsAuthSession)
        self.assertIsInstance(session, requests.Session)

    # --- _detect_signal_from_stack -------------------------------------

    def test_detect_signal_from_stack_returns_none_outside_otlp_exporter(self):
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_sigv4_session_factory import (
            _detect_signal_from_stack,
        )

        # Calling from this test module — module name doesn't contain any
        # of the OTLP exporter substrings, so no signal should be detected.
        self.assertIsNone(_detect_signal_from_stack())
