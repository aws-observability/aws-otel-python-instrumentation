# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import requests
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import OTLPAwsSigV4Exporter
from grpc import Compression
from opentelemetry.exporter.otlp.proto.http.version import __version__
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    DEFAULT_ENDPOINT,
    DEFAULT_TRACES_EXPORT_PATH,
    DEFAULT_TIMEOUT,
    DEFAULT_COMPRESSION,
)

from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
)

OTLP_CW_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
        
class TestAwsSigV4Exporter(TestCase):
    
    @patch.dict(os.environ, {}, clear=True) 
    def test_sigv4_exporter_init_default(self):
        exporter = OTLPAwsSigV4Exporter()
        self.assertEqual(
            exporter._endpoint, DEFAULT_ENDPOINT + DEFAULT_TRACES_EXPORT_PATH
        )
        self.assertEqual(exporter._certificate_file, True)
        self.assertEqual(exporter._client_certificate_file, None)
        self.assertEqual(exporter._client_key_file, None)
        self.assertEqual(exporter._timeout, DEFAULT_TIMEOUT)
        self.assertIs(exporter._compression, DEFAULT_COMPRESSION)
        self.assertEqual(exporter._headers, {})
        self.assertIsInstance(exporter._session, requests.Session)
        self.assertIn("User-Agent", exporter._session.headers)
        self.assertEqual(
            exporter._session.headers.get("Content-Type"),
            "application/x-protobuf",
        )
        self.assertEqual(
            exporter._session.headers.get("User-Agent"),
            "OTel-OTLP-Exporter-Python/" + __version__,
        )

    @patch.dict(os.environ, {
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: OTLP_CW_ENDPOINT
    }, clear=True) 
    @patch('botocore.session.Session')
    def test_sigv4_exporter_init_valid_cw_otlp_endpoint(self, session_mock):
        mock_session = MagicMock()
        session_mock.return_value = mock_session
        
        mock_session.get_available_regions.return_value = ['us-east-1', 'us-west-2']
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)

        self.assertEqual(
            exporter._endpoint, OTLP_CW_ENDPOINT
        )
        self.assertEqual(
            exporter._aws_region, "us-east-1"
        )
        
        mock_session.get_available_regions.assert_called_once_with('xray')

    @patch('botocore.session.Session')
    def test_sigv4_exporter_init_invalid_cw_otlp_endpoint(self, session_mock):
        invalid_otlp_endpoints = [
            "https://xray.bad-region-1.amazonaws.com/v1/traces",
            "https://xray.us-east-1.amaz.com/v1/traces"
            "https://logs.us-east-1.amazonaws.com/v1/logs"
        ]

        for bad_endpoint in invalid_otlp_endpoints:
            with self.subTest(endpoint=bad_endpoint):
                with patch.dict(os.environ, {
                    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: bad_endpoint
                }):
                    
                    mock_session = MagicMock()
                    session_mock.return_value = mock_session
                    
                    mock_session.get_available_regions.return_value = ['us-east-1', 'us-west-2']
                    exporter = OTLPAwsSigV4Exporter(endpoint=bad_endpoint)

                    self.assertIsNone(exporter._aws_region)
    
    @patch.dict(os.environ, {
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: OTLP_CW_ENDPOINT
    }, clear=True) 
    def test_sigv4_exporter_export_valid_otlp_endpoint(self):
        exporter = OTLPAwsSigV4Exporter(endpoint=OTLP_CW_ENDPOINT)


