# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import ANY, patch

import requests
from botocore.credentials import Credentials

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    DEFAULT_COMPRESSION,
    DEFAULT_TIMEOUT,
    OTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.version import __version__

AWS_OTLP_TRACES_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
AWS_OTLP_LOGS_ENDPOINT = "https://logs.us-east-1.amazonaws.com/v1/logs"

USER_AGENT = "OTel-OTLP-Exporter-Python/" + __version__
CONTENT_TYPE = "application/x-protobuf"
AUTHORIZATION_HEADER = "Authorization"
X_AMZ_DATE_HEADER = "X-Amz-Date"
X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token"

mock_credentials = Credentials(access_key="test_access_key", secret_key="test_secret_key", token="test_session_token")


class TestAwsExporter(TestCase):

    def test_sigv4_exporter_init_default(self):
        """Tests that the default exporter is is still an instance of upstream's exporter"""

        test_cases = [
            [OTLPAwsSpanExporter(endpoint=AWS_OTLP_TRACES_ENDPOINT), AWS_OTLP_TRACES_ENDPOINT, OTLPSpanExporter],
            [OTLPAwsLogExporter(endpoint=AWS_OTLP_LOGS_ENDPOINT), AWS_OTLP_LOGS_ENDPOINT, OTLPLogExporter],
        ]

        for tc in test_cases:
            self.validate_exporter_extends_http_exporter(exporter=tc[0], endpoint=tc[1], type=tc[2])

    @patch("pkg_resources.get_distribution", side_effect=ImportError("test error"))
    @patch.dict("sys.modules", {"botocore": None}, clear=False)
    @patch("requests.Session.request", return_value=requests.Response())
    def test_aws_auth_session_no_botocore(self, _, __):
        """Tests that aws_auth_session will not inject SigV4 Headers if botocore is not installed."""

        session = AwsAuthSession("us-east-1", "xray")
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=None)
    def test_aws_auth_session_no_credentials(self, _, __):
        """Tests that aws_auth_session will not inject SigV4 Headers if retrieving credentials returns None."""

        session = AwsAuthSession("us-east-1", "xray")
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_aws_auth_session(self, _, __):
        """Tests that aws_auth_session will inject SigV4 Headers if botocore is installed."""

        session = AwsAuthSession("us-east-1", "xray")
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    def validate_exporter_extends_http_exporter(self, exporter, endpoint, type):
        self.assertIsInstance(exporter, type)
        self.assertIsInstance(exporter._session, AwsAuthSession)
        self.assertEqual(exporter._endpoint, endpoint)
        self.assertEqual(exporter._certificate_file, True)
        self.assertEqual(exporter._client_certificate_file, None)
        self.assertEqual(exporter._client_key_file, None)
        self.assertEqual(exporter._timeout, DEFAULT_TIMEOUT)
        self.assertIs(exporter._compression, DEFAULT_COMPRESSION)
        self.assertEqual(exporter._headers, {})
        self.assertIn("User-Agent", exporter._session.headers)
        self.assertEqual(
            exporter._session.headers.get("Content-Type"),
            CONTENT_TYPE,
        )
        self.assertEqual(exporter._session.headers.get("User-Agent"), USER_AGENT)
