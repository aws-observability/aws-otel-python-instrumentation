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
from amazon.opentelemetry.distro.exporter.otlp.aws.metrics.otlp_aws_metric_exporter import OTLPAwsMetricExporter
from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics.export import AggregationTemporality

_COMMERCIAL_ENDPOINT = "https://monitoring.us-east-1.amazonaws.com/v1/metrics"
_CHINA_ENDPOINT = "https://monitoring.cn-north-1.amazonaws.com.cn/v1/metrics"


class TestOTLPAwsMetricExporter(TestCase):
    def setUp(self):
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
