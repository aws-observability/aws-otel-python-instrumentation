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
