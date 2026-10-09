# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import TestCase
from unittest.mock import patch

import requests
from botocore.credentials import Credentials
from botocore.session import Session

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_log_record_exporter import OTLPAwsLogRecordExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.metrics.otlp_aws_metric_exporter import OTLPAwsMetricExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from opentelemetry._logs import LogRecord
from opentelemetry.sdk._configuration import _get_exporter_names, _import_exporters
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

AWS_OTLP_TRACES_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
AWS_OTLP_LOGS_ENDPOINT = "https://logs.us-east-1.amazonaws.com/v1/logs"

AUTHORIZATION_HEADER = "Authorization"
X_AMZ_DATE_HEADER = "X-Amz-Date"
X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token"

mock_credentials = Credentials(access_key="test_access_key", secret_key="test_secret_key", token="test_session_token")

_SIGNALS = (
    ("traces", "xray", OTLPAwsSpanExporter),
    ("metrics", "monitoring", OTLPAwsMetricExporter),
    ("logs", "logs", OTLPAwsLogRecordExporter),
)
_CONFIG_MODULE = "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session"


class TestAwsAuthSession(TestCase):
    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=None)
    def test_aws_auth_session_no_credentials(self, _, mock_request):
        """Tests that aws_auth_session will not inject SigV4 Headers if retrieving credentials returns None."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={"test": "test"})

        actual_headers = mock_request.call_args.kwargs["headers"]
        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_aws_auth_session(self, _, mock_request):
        """Tests that aws_auth_session will inject SigV4 Headers if botocore is installed."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={"test": "test"})

        actual_headers = mock_request.call_args.kwargs["headers"]
        self.assertEqual(actual_headers["test"], "test")
        self.assertIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_aws_auth_session_does_not_mutate_caller_headers(self, _, __):
        """The upstream OTLP HTTP client reuses one headers dict across requests, so signing
        headers must not be written back into it."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        caller_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=caller_headers)

        self.assertEqual(caller_headers, {"test": "test"})

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_credentials_are_resolved_once(self, mock_get_credentials, _):
        """Credentials must be resolved only once across multiple ``request()`` calls.

        This is the hot-path mitigation for the pip_system_certs RecursionError: each
        ``get_credentials()`` call walks the credential resolver chain, which constructs
        a urllib3 SSL context. Caching the returned object (``RefreshableCredentials``
        rotates internally on attribute access) ensures the SSL context is created at
        most once per exporter, not once per export.
        """
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        for _ in range(5):
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        self.assertEqual(mock_get_credentials.call_count, 1)

    @patch("requests.Session.request", return_value=requests.Response())
    def test_credentials_retry_after_transient_failure(self, mock_request):
        """A transient ``get_credentials()`` failure must NOT latch the resolved
        flag. The next ``request()`` call must retry resolution. This preserves
        self-healing behavior on transient errors (e.g., IMDS timeouts) and matches
        the pre-fix behavior on the failure path.
        """
        # First call raises, subsequent calls succeed.
        get_credentials_mock = patch(
            "botocore.session.Session.get_credentials",
            side_effect=[RuntimeError("transient"), mock_credentials, mock_credentials],
        )
        with get_credentials_mock as mock_get_credentials:
            session = AwsAuthSession("us-east-1", "xray", get_aws_session())

            # 1st request: get_credentials raises, no auth headers added.
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})
            self.assertNotIn(AUTHORIZATION_HEADER, mock_request.call_args.kwargs["headers"])

            # 2nd request: get_credentials succeeds, auth headers must appear.
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})
            self.assertIn(AUTHORIZATION_HEADER, mock_request.call_args.kwargs["headers"])

            # 3rd request: cached credentials reused, no further get_credentials calls.
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})
            self.assertIn(AUTHORIZATION_HEADER, mock_request.call_args.kwargs["headers"])

            # Two resolution attempts: one failed, one succeeded; third request reuses cache.
            self.assertEqual(mock_get_credentials.call_count, 2)

    @patch("requests.Session.request", return_value=requests.Response())
    def test_credential_exception_logged_once_not_twice(self, _):
        """When get_credentials() raises, the failure is logged exactly once (in
        _ensure_initialized with detail), not a second time by request()'s else
        branch."""
        with patch(
            "botocore.session.Session.get_credentials",
            side_effect=RuntimeError("imds timeout"),
        ):
            session = AwsAuthSession("us-east-1", "xray", get_aws_session())
            with self.assertLogs(
                "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session",
                level="ERROR",
            ) as log_ctx:
                session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        # Exactly one error log, and it carries the exception detail.
        self.assertEqual(len(log_ctx.records), 1)
        self.assertIn("imds timeout", log_ctx.output[0])

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=None)
    def test_none_credentials_logged_once(self, _, __):
        """When get_credentials() returns None without raising (no provider
        configured), request() surfaces it with a single error log."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        with self.assertLogs(
            "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session",
            level="ERROR",
        ) as log_ctx:
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        self.assertEqual(len(log_ctx.records), 1)
        self.assertIn("Failed to load AWS Credentials", log_ctx.output[0])

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session"
        ".apply_pip_system_certs_compatibility_patch"
    )
    def test_pip_system_certs_patch_invoked_on_first_request(self, mock_apply_patch, _, __):
        """The ssl.SSLContext rebind helper is invoked on the first ``request()`` call
        and not re-invoked on subsequent calls.

        The patch itself is a no-op when pip_system_certs is not installed, so this
        test only asserts the call site, not the patch behavior."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})
        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})
        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        self.assertEqual(mock_apply_patch.call_count, 1)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session"
        ".apply_pip_system_certs_compatibility_patch",
        side_effect=RuntimeError("simulated patch failure"),
    )
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_patch_failure_does_not_break_request(self, _, __, mock_request):
        """If the SSL-context-rebind helper itself raises, the failure is logged
        but ``request()`` still proceeds and signs successfully. The patch is
        defensive infrastructure, not a hard precondition."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        self.assertIn(AUTHORIZATION_HEADER, mock_request.call_args.kwargs["headers"])

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_signing_failure_does_not_break_request(self, _, mock_request):
        """If SigV4 signing itself raises, ``request()`` still issues the
        unauthenticated request rather than crashing the caller."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session.SigV4Auth") as mock_sigv4:
            mock_sigv4.return_value.add_auth.side_effect = RuntimeError("signing boom")
            # Should not raise
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        # No auth header because signing raised before headers could be merged.
        mock_request.assert_called_once()
        self.assertNotIn(AUTHORIZATION_HEADER, mock_request.call_args.kwargs["headers"])

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_concurrent_requests_resolve_credentials_once(self, mock_get_credentials, _):
        """Two threads racing on the first request must both observe a single
        credential resolution. The double-checked locking in ``_ensure_initialized``
        is what provides this guarantee."""
        # pylint: disable=import-outside-toplevel
        from threading import Thread

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        def call():
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers={})

        threads = [Thread(target=call) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(mock_get_credentials.call_count, 1)


# pylint: disable=protected-access
class TestAwsExporterEntryPoints(TestCase):
    def setUp(self) -> None:
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

    def _emit(self, signal: str, exporter) -> None:
        if signal == "traces":
            provider = TracerProvider()
            self.addCleanup(provider.shutdown)
            provider.add_span_processor(SimpleSpanProcessor(exporter))
            with provider.get_tracer(__name__).start_as_current_span("test-span"):
                pass
        elif signal == "metrics":
            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=float("inf"))
            provider = MeterProvider(metric_readers=[reader])
            self.addCleanup(provider.shutdown)
            provider.get_meter(__name__).create_counter("test-counter").add(1)
            self.assertTrue(provider.force_flush())
        else:
            provider = LoggerProvider()
            self.addCleanup(provider.shutdown)
            provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
            provider.get_logger(__name__).emit(LogRecord(body="test-log"))

    def test_real_loader_selects_and_exports_all_signals_with_sigv4(self):
        for signal, service, _ in _SIGNALS:
            os.environ[f"OTEL_{signal.upper()}_EXPORTER"] = "otlp/sigv4"
            os.environ[f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT"] = (
                f"https://{service}.us-west-2.amazonaws.com/v1/{signal}"
            )
        os.environ["OTEL_EXPORTER_OTLP_LOGS_HEADERS"] = "x-aws-log-group=test-group,x-aws-log-stream=test-stream"
        loaded = _import_exporters(*(_get_exporter_names(signal) for signal, _, _ in _SIGNALS))

        for (signal, service, expected_class), exporters in zip(_SIGNALS, loaded):
            with self.subTest(signal=signal):
                self.assertIs(exporters["otlp/sigv4"], expected_class)
                exporter = exporters["otlp/sigv4"]()
                response = requests.Response()
                response.status_code = 200
                response._content = b""
                with patch.object(requests.Session, "request", return_value=response) as request:
                    self._emit(signal, exporter)
                self.assertEqual(request.call_count, 1)
                headers = request.call_args.kwargs["headers"]
                authorization = [value for key, value in headers.items() if key.lower() == "authorization"]
                self.assertEqual(len(authorization), 1)
                self.assertTrue(authorization[0].startswith("AWS4-HMAC-SHA256"))
                self.assertIn(f"/us-west-2/{service}/aws4_request", authorization[0])
                self.assertEqual(
                    request.call_args.kwargs["url"],
                    os.environ[f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT"],
                )
                self.assertTrue(request.call_args.kwargs["data"])
                if signal == "logs":
                    self.assertEqual(headers["x-aws-log-group"], "test-group")
                    self.assertEqual(headers["x-aws-log-stream"], "test-stream")
                    self.assertEqual(headers["Content-Encoding"], "gzip")

    def test_endpoint_region_resolution_across_partitions(self):
        partitions = (
            ("us-west-2", "amazonaws.com"),
            ("cn-north-1", "amazonaws.com.cn"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
        )
        for signal, service, exporter_class in _SIGNALS:
            for region, suffix in partitions:
                endpoint = f"https://{service}.{region}.{suffix}/v1/{signal}"
                with self.subTest(signal=signal, region=region), patch.dict(
                    os.environ,
                    {f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT": endpoint},
                ):
                    exporter = exporter_class()
                    self.addCleanup(exporter.shutdown)
                    self.assertEqual(exporter._aws_region, region)

    def test_generic_endpoint_and_aws_environment_region(self):
        for signal, _, exporter_class in _SIGNALS:
            with self.subTest(signal=signal), patch.dict(
                os.environ,
                {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example.com", "AWS_DEFAULT_REGION": "us-east-2"},
            ):
                exporter = exporter_class()
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._aws_region, "us-east-2")
                self.assertEqual(exporter._client._endpoint, f"https://collector.example.com/v1/{signal}")

    def test_generic_aws_endpoint_region_matches_upstream_resolved_endpoint(self):
        with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://xray.us-west-2.amazonaws.com/base/"}):
            for signal, _, exporter_class in _SIGNALS:
                with self.subTest(signal=signal):
                    exporter = exporter_class()
                    self.addCleanup(exporter.shutdown)
                    self.assertEqual(exporter._aws_region, "us-west-2")
                    self.assertEqual(
                        exporter._client._endpoint, f"https://xray.us-west-2.amazonaws.com/base/v1/{signal}"
                    )

    def test_signing_service_precedence_in_exported_requests(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        for signal, default_service, exporter_class in _SIGNALS:
            signal_variable = f"OTEL_EXPORTER_OTLP_{signal.upper()}_SIGV4_SERVICE"
            shared_variable = "OTEL_EXPORTER_OTLP_SIGV4_SERVICE"
            signal_overrides = {
                f"OTEL_EXPORTER_OTLP_{other_signal.upper()}_SIGV4_SERVICE": f"{other_signal}-service"
                for other_signal, _, _ in _SIGNALS
            }
            other_overrides = {key: value for key, value in signal_overrides.items() if key != signal_variable}
            test_cases = (
                ({}, None, default_service),
                ({shared_variable: "shared-service"}, None, "shared-service"),
                ({shared_variable: "shared-service", **signal_overrides}, None, f"{signal}-service"),
                ({shared_variable: "shared-service", signal_variable: ""}, None, "shared-service"),
                ({shared_variable: "", signal_variable: ""}, None, default_service),
                (other_overrides, None, default_service),
                ({shared_variable: "shared-service", **signal_overrides}, "explicit-service", "explicit-service"),
            )
            for environment, explicit_service, expected_service in test_cases:
                with self.subTest(
                    signal=signal, environment=environment, explicit_service=explicit_service
                ), patch.dict(
                    os.environ,
                    {
                        "AWS_REGION": "us-west-2",
                        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example.com",
                        **environment,
                    },
                ):
                    exporter = exporter_class(service=explicit_service)
                    with patch.object(requests.Session, "request", return_value=response) as request:
                        self._emit(signal, exporter)
                    self.assertEqual(request.call_count, 1)
                    self.assertIn(
                        f"/us-west-2/{expected_service}/aws4_request",
                        request.call_args.kwargs["headers"]["Authorization"],
                    )

    def test_signals_can_be_selected_independently_alongside_console(self):
        for selected_signal, _, _ in _SIGNALS:
            with self.subTest(signal=selected_signal):
                for signal, _, _ in _SIGNALS:
                    os.environ[f"OTEL_{signal.upper()}_EXPORTER"] = (
                        "console,otlp/sigv4" if signal == selected_signal else "none"
                    )
                loaded = _import_exporters(*(_get_exporter_names(signal) for signal, _, _ in _SIGNALS))
                for (signal, _, expected_class), exporters in zip(_SIGNALS, loaded):
                    if signal == selected_signal:
                        self.assertEqual(set(exporters), {"console", "otlp/sigv4"})
                        self.assertIs(exporters["otlp/sigv4"], expected_class)
                    else:
                        self.assertFalse(exporters)

    def test_explicit_constructor_region_and_session_are_preserved(self):
        session = Session()
        for signal, service, exporter_class in _SIGNALS:
            with self.subTest(signal=signal):
                exporter = exporter_class(
                    aws_region="us-east-1",
                    session=session,
                    endpoint=f"https://{service}.us-west-2.amazonaws.com/v1/{signal}",
                )
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._aws_region, "us-east-1")
                self.assertIs(exporter._session._session, session)

    def test_optional_auth_session_arguments_use_aws_environment(self):
        for region_env in ("AWS_REGION", "AWS_DEFAULT_REGION"):
            with self.subTest(region_env=region_env), patch.dict(os.environ, {region_env: "us-east-2"}):
                session = AwsAuthSession(service="xray")
                self.addCleanup(session.close)
                self.assertEqual(session._aws_region, "us-east-2")
                credentials = session._session.get_credentials().get_frozen_credentials()
                self.assertEqual(credentials.access_key, "test-access-key")
                self.assertEqual(credentials.secret_key, "test-secret-key")

    def test_auth_session_infers_resolved_endpoint_region(self):
        session = AwsAuthSession(service="xray", endpoint="https://xray.us-west-2.amazonaws.com/v1/traces")
        self.addCleanup(session.close)
        self.assertEqual(session._aws_region, "us-west-2")
        self.assertIsInstance(session._session, Session)

    def test_auth_session_with_no_arguments_uses_environment_signing_service(self):
        with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_SIGV4_SERVICE": "logs", "AWS_REGION": "us-east-1"}):
            session = AwsAuthSession()
            self.addCleanup(session.close)
            self.assertEqual(session._service, "logs")
            self.assertEqual(session._aws_region, "us-east-1")

    def test_auth_session_requires_signing_service(self):
        with self.assertRaisesRegex(ValueError, "requires a signing service"):
            AwsAuthSession()

    def test_optional_otlp_settings_use_environment_with_explicit_overrides(self):
        for signal, _, exporter_class in _SIGNALS:
            prefix = f"OTEL_EXPORTER_OTLP_{signal.upper()}"
            with self.subTest(signal=signal), patch.dict(
                os.environ,
                {
                    "AWS_REGION": "us-east-1",
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "https://generic.example.com",
                    "OTEL_EXPORTER_OTLP_HEADERS": "x-custom=generic",
                    "OTEL_EXPORTER_OTLP_TIMEOUT": "10",
                    f"{prefix}_ENDPOINT": "https://signal.example.com",
                    f"{prefix}_HEADERS": "x-custom=signal",
                    f"{prefix}_TIMEOUT": "20",
                },
            ):
                exporter = exporter_class()
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._client._endpoint, "https://signal.example.com")
                self.assertEqual(exporter._client._headers["x-custom"], "signal")
                self.assertEqual(exporter._client._timeout, 20)

                exporter = exporter_class(
                    endpoint="https://explicit.example.com", headers={"x-custom": "explicit"}, timeout=30
                )
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._client._endpoint, "https://explicit.example.com")
                self.assertEqual(exporter._client._headers["x-custom"], "explicit")
                self.assertEqual(exporter._client._timeout, 30)

    def test_region_precedence_with_explicit_session(self):
        session = Session()
        session.set_config_variable("region", "eu-west-1")
        endpoint = "https://xray.us-west-2.amazonaws.com/v1/traces"
        test_cases = (
            ("us-east-1", {"AWS_REGION": "us-east-2", "AWS_DEFAULT_REGION": "us-east-3"}, endpoint, "us-east-1"),
            (None, {"AWS_REGION": "us-east-2", "AWS_DEFAULT_REGION": "us-east-3"}, endpoint, "us-east-2"),
            (None, {"AWS_DEFAULT_REGION": "us-east-3"}, endpoint, "us-east-3"),
            (None, {}, endpoint, "us-west-2"),
            (None, {}, "https://collector.example.com", "eu-west-1"),
        )
        for explicit_region, environment, configured_endpoint, expected_region in test_cases:
            with self.subTest(expected_region=expected_region), patch.dict(os.environ, environment):
                auth_session = AwsAuthSession(
                    aws_region=explicit_region, service="xray", session=session, endpoint=configured_endpoint
                )
                self.addCleanup(auth_session.close)
                self.assertEqual(auth_session._aws_region, expected_region)
                self.assertIs(auth_session._session, session)

    def test_missing_region_fails_during_setup(self):
        session = Session()
        session.set_config_variable("region", None)
        for signal, _, exporter_class in _SIGNALS:
            with self.subTest(signal=signal), self.assertRaisesRegex(ValueError, "requires an AWS endpoint region"):
                exporter_class(session=session, endpoint="https://collector.example.com")

    def test_missing_botocore_session_fails_during_setup(self):
        with patch(f"{_CONFIG_MODULE}.get_aws_session", return_value=None):
            with self.assertRaisesRegex(ValueError, "requires botocore"):
                AwsAuthSession(service="xray")
