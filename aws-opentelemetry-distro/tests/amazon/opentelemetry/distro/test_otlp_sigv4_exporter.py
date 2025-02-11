# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import TestCase
from unittest.mock import ANY, MagicMock, PropertyMock, patch

import requests
from botocore.credentials import Credentials

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import OTLPAwsSigV4Exporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    DEFAULT_COMPRESSION,
    DEFAULT_ENDPOINT,
    DEFAULT_TIMEOUT,
    DEFAULT_TRACES_EXPORT_PATH,
    OTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.version import __version__
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
from opentelemetry.sdk.trace import SpanContext, _Span
from opentelemetry.trace import SpanKind, TraceFlags

OTLP_CW_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
USER_AGENT = "OTel-OTLP-Exporter-Python/" + __version__
CONTENT_TYPE = "application/x-protobuf"
AUTHORIZATION_HEADER = "Authorization"
X_AMZ_DATE_HEADER = "X-Amz-Date"
X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token"


class TestAwsSigV4Exporter(TestCase):
    def setUp(self):
        self.testing_spans = [
            self.create_span("test_span1", SpanKind.INTERNAL),
            self.create_span("test_span2", SpanKind.SERVER),
            self.create_span("test_span3", SpanKind.CLIENT),
            self.create_span("test_span4", SpanKind.PRODUCER),
            self.create_span("test_span5", SpanKind.CONSUMER),
        ]

        self.invalid_cw_otlp_tracing_endpoints = [
            "https://xray.bad-region-1.amazonaws.com/v1/traces",
            "https://xray.us-east-1.amaz.com/v1/traces",
            "https://logs.us-east-1.amazonaws.com/v1/logs",
            "https://test-endpoint123.com/test",
            "xray.us-east-1.amazonaws.com/v1/traces",
            "https://test-endpoint123.com/test https://xray.us-east-1.amazonaws.com/v1/traces",
            "https://xray.us-east-1.amazonaws.com/v1/tracesssda",
        ]

        self.expected_auth_header = "AWS4-HMAC-SHA256 Credential=test_key/some_date/us-east-1/xray/aws4_request"
        self.expected_auth_x_amz_date = "some_date"
        self.expected_auth_security_token = "test_token"

    @patch.dict(os.environ, {}, clear=True)
    def test_sigv4_exporter_init_default(self):
        """Tests that the default exporter is OTLP protobuf/http Span Exporter if no endpoint is set"""

        exporter = OTLPAwsSigV4Exporter()
        self.validate_exporter_extends_http_span_exporter(exporter, DEFAULT_ENDPOINT + DEFAULT_TRACES_EXPORT_PATH)
        self.assertIsInstance(exporter._session, requests.Session)

    @patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: OTLP_CW_ENDPOINT}, clear=True)
    @patch("botocore.session.Session")
    def test_sigv4_exporter_init_valid_cw_otlp_endpoint(self, session_mock):
        """Tests that the endpoint is validated and sets the aws_region but still uses the OTLP protobuf/http
        Span Exporter exporter constructor behavior if a valid OTLP CloudWatch endpoint is set."""

        mock_session = MagicMock()
        session_mock.return_value = mock_session

        mock_session.get_available_regions.return_value = ["us-east-1", "us-west-2"]
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)

        self.assertEqual(exporter._aws_region, "us-east-1")
        self.validate_exporter_extends_http_span_exporter(exporter, OTLP_CW_ENDPOINT)

        mock_session.get_available_regions.assert_called_once_with("xray")

    @patch.dict("sys.modules", {"botocore": None})
    def test_no_botocore_valid_xray_endpoint(self):
        """Test that exporter defaults when using OTLP CW endpoint without botocore"""

        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)

        self.assertIsNone(exporter._aws_region)

    @patch("botocore.session.Session")
    def test_sigv4_exporter_init_invalid_cw_otlp_endpoint(self, botocore_mock):
        """Tests that the exporter constructor behavior is set by OTLP protobuf/http Span Exporter
        if an invalid OTLP CloudWatch endpoint is set"""
        for bad_endpoint in self.invalid_cw_otlp_tracing_endpoints:
            with self.subTest(endpoint=bad_endpoint):
                with patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: bad_endpoint}):

                    mock_session = MagicMock()
                    botocore_mock.return_value = mock_session

                    mock_session.get_available_regions.return_value = ["us-east-1", "us-west-2"]
                    exporter = OTLPAwsSigV4Exporter(endpoint=bad_endpoint)
                    self.validate_exporter_extends_http_span_exporter(exporter, bad_endpoint)

                    self.assertIsNone(exporter._aws_region)

    @patch("botocore.session.Session.get_available_regions")
    @patch("requests.Session.post")
    @patch("botocore.auth.SigV4Auth.add_auth")
    def test_sigv4_exporter_export_does_not_add_sigv4_if_not_valid_cw_endpoint(
        self, mock_sigv4_auth, requests_mock, botocore_mock
    ):
        """Tests that if the OTLP endpoint is not a valid CW endpoint but the credentials are valid,
        SigV4 authentication method is NOT called and is NOT injected into the existing Session headers."""

        # Setting the exporter response
        mock_response = MagicMock()
        mock_response.status_code = 200
        type(mock_response).ok = PropertyMock(return_value=True)

        # Setting the request session headers to make the call to endpoint
        mock_session = MagicMock()
        mock_session.headers = {"User-Agent": USER_AGENT, "Content-Type": CONTENT_TYPE}
        requests_mock.return_value = mock_session
        mock_session.post.return_value = mock_response

        mock_botocore_session = MagicMock()
        botocore_mock.return_value = mock_botocore_session
        mock_botocore_session.get_available_regions.return_value = ["us-east-1", "us-west-2"]
        mock_botocore_session.get_credentials.return_value = Credentials(
            access_key="test_key", secret_key="test_secret", token="test_token"
        )

        # SigV4 mock authentication injection
        mock_sigv4_auth.side_effect = self.mock_add_auth

        # Initialize and call exporter
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)
        exporter.export(self.testing_spans)

        # For each invalid CW OTLP endpoint, vdalidate that the sigv4
        for bad_endpoint in self.invalid_cw_otlp_tracing_endpoints:
            with self.subTest(endpoint=bad_endpoint):
                with patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: bad_endpoint}):
                    botocore_mock.return_value = ["us-east-1", "us-west-2"]

                    exporter = OTLPAwsSigV4Exporter(endpoint=bad_endpoint)

                    self.validate_exporter_extends_http_span_exporter(exporter, bad_endpoint)

                    # Test case, should not have detected a valid region
                    self.assertIsNone(exporter._aws_region)

                    exporter.export(self.testing_spans)

                    mock_sigv4_auth.assert_not_called()

                    # Verify that SigV4 request headers were not injected
                    actual_headers = mock_session.headers
                    self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
                    self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
                    self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

                    requests_mock.assert_called_with(
                        url=bad_endpoint,
                        data=ANY,
                        verify=ANY,
                        timeout=ANY,
                        cert=ANY,
                    )

    @patch("botocore.session.Session")
    @patch("requests.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: OTLP_CW_ENDPOINT})
    def test_sigv4_exporter_export_does_not_add_sigv4_if_not_valid_credentials(
        self, mock_sigv4_auth, requests_posts_mock, botocore_mock
    ):
        """Tests that if the OTLP endpoint is a valid CW endpoint but no credentials are returned,
        SigV4 authentication method is NOT called and is NOT injected into the existing
        Session headers."""
        # Setting the exporter response
        mock_response = MagicMock()
        mock_response.status_code = 200
        type(mock_response).ok = PropertyMock(return_value=True)

        # Setting the request session headers to make the call to endpoint
        mock_session = MagicMock()
        mock_session.headers = {"User-Agent": USER_AGENT, "Content-Type": CONTENT_TYPE}
        requests_posts_mock.return_value = mock_session
        mock_session.post.return_value = mock_response

        mock_botocore_session = MagicMock()
        botocore_mock.return_value = mock_botocore_session
        mock_botocore_session.get_available_regions.return_value = ["us-east-1", "us-west-2"]

        # Test case, return None for get credentials
        mock_botocore_session.get_credentials.return_value = None

        # Initialize and call exporter
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)

        # Validate that the region is valid
        self.assertEqual(exporter._aws_region, "us-east-1")

        exporter.export(self.testing_spans)

        # Verify SigV4 auth was not called
        mock_sigv4_auth.assert_not_called()

        # Verify that SigV4 request headers were properly injected
        actual_headers = mock_session.headers
        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("botocore.session.Session")
    @patch("requests.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: OTLP_CW_ENDPOINT})
    def test_sigv4_exporter_export_adds_sigv4_authentication_if_valid_cw_endpoint(
        self, mock_sigv4_auth, requests_posts_mock, botocore_mock
    ):
        """Tests that if the OTLP endpoint is valid and credentials are valid,
        SigV4 authentication method is called and is
        injected into the existing Session headers."""

        # Setting the exporter response
        mock_response = MagicMock()
        mock_response.status_code = 200
        type(mock_response).ok = PropertyMock(return_value=True)

        # Setting the request session headers to make the call to endpoint
        mock_session = MagicMock()
        mock_session.headers = {"User-Agent": USER_AGENT, "Content-Type": CONTENT_TYPE}
        requests_posts_mock.return_value = mock_session
        mock_session.post.return_value = mock_response

        mock_botocore_session = MagicMock()
        botocore_mock.return_value = mock_botocore_session
        mock_botocore_session.get_available_regions.return_value = ["us-east-1", "us-west-2"]
        mock_botocore_session.get_credentials.return_value = Credentials(
            access_key="test_key", secret_key="test_secret", token="test_token"
        )

        # SigV4 mock authentication injection
        mock_sigv4_auth.side_effect = self.mock_add_auth

        # Initialize and call exporter
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)
        exporter.export(self.testing_spans)

        # Verify SigV4 auth was called
        mock_sigv4_auth.assert_called_once_with(ANY)

        # Verify that SigV4 request headers were properly injected
        actual_headers = mock_session.headers
        self.assertIn("Authorization", actual_headers)
        self.assertIn("X-Amz-Date", actual_headers)
        self.assertIn("X-Amz-Security-Token", actual_headers)

        self.assertEqual(actual_headers[AUTHORIZATION_HEADER], self.expected_auth_header)
        self.assertEqual(actual_headers[X_AMZ_DATE_HEADER], self.expected_auth_x_amz_date)
        self.assertEqual(actual_headers[X_AMZ_SECURITY_TOKEN_HEADER], self.expected_auth_security_token)

    def validate_exporter_extends_http_span_exporter(self, exporter, endpoint):
        self.assertIsInstance(exporter, OTLPSpanExporter)
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

    @staticmethod
    def create_span(name="test_span", kind=SpanKind.INTERNAL):
        span = _Span(
            name=name,
            context=SpanContext(
                trace_id=0x1234567890ABCDEF,
                span_id=0x9876543210,
                is_remote=False,
                trace_flags=TraceFlags(TraceFlags.SAMPLED),
            ),
            kind=kind,
        )
        return span

    def mock_add_auth(self, request):
        request.headers._headers.extend(
            [
                (AUTHORIZATION_HEADER, self.expected_auth_header),
                (X_AMZ_DATE_HEADER, self.expected_auth_x_amz_date),
                (X_AMZ_SECURITY_TOKEN_HEADER, self.expected_auth_security_token),
            ]
        )
