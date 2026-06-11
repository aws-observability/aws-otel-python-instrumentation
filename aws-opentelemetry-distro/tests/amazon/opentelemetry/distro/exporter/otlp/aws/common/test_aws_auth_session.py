# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import patch

import requests
from botocore.credentials import Credentials

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession

AWS_OTLP_TRACES_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
AWS_OTLP_LOGS_ENDPOINT = "https://logs.us-east-1.amazonaws.com/v1/logs"

AUTHORIZATION_HEADER = "Authorization"
X_AMZ_DATE_HEADER = "X-Amz-Date"
X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token"

mock_credentials = Credentials(access_key="test_access_key", secret_key="test_secret_key", token="test_session_token")


class TestAwsAuthSession(TestCase):
    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=None)
    def test_aws_auth_session_no_credentials(self, _, __):
        """Tests that aws_auth_session will not inject SigV4 Headers if retrieving credentials returns None."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_aws_auth_session(self, _, __):
        """Tests that aws_auth_session will inject SigV4 Headers if botocore is installed."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

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
    def test_credentials_retry_after_transient_failure(self, _):
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
            headers_first = {}
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=headers_first)
            self.assertNotIn(AUTHORIZATION_HEADER, headers_first)

            # 2nd request: get_credentials succeeds, auth headers must appear.
            headers_second = {}
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=headers_second)
            self.assertIn(AUTHORIZATION_HEADER, headers_second)

            # 3rd request: cached credentials reused, no further get_credentials calls.
            headers_third = {}
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=headers_third)
            self.assertIn(AUTHORIZATION_HEADER, headers_third)

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
    def test_patch_failure_does_not_break_request(self, _, __, ___):
        """If the SSL-context-rebind helper itself raises, the failure is logged
        but ``request()`` still proceeds and signs successfully. The patch is
        defensive infrastructure, not a hard precondition."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        actual_headers: dict = {}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertIn(AUTHORIZATION_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_signing_failure_does_not_break_request(self, _, __):
        """If SigV4 signing itself raises, ``request()`` still issues the
        unauthenticated request rather than crashing the caller."""
        session = AwsAuthSession("us-east-1", "xray", get_aws_session())

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session.SigV4Auth") as mock_sigv4:
            mock_sigv4.return_value.add_auth.side_effect = RuntimeError("signing boom")
            actual_headers: dict = {}
            # Should not raise
            session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        # No auth header because signing raised before headers could be merged.
        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)

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
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(mock_get_credentials.call_count, 1)
