# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generator-level wiring tests for presigned AWS URL attribution.

These exercise _AwsMetricAttributeGenerator end to end (config gating, AWS-SDK exclusion, remote
resource reuse, and suppression of the generic HTTP operation fallback), complementing the
component tests for the parser and the S3 attributor.
"""

import os
from typing import List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_RESOURCE_IDENTIFIER,
    AWS_REMOTE_RESOURCE_TYPE,
    AWS_REMOTE_SERVICE,
)
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro.metric_attribute_generator import DEPENDENCY_METRIC
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind

_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG = "OTEL_AWS_APPLICATION_SIGNALS_PRESIGNED_URL_ATTRIBUTION_ENABLED"
_UNKNOWN_REMOTE_OPERATION = "UnknownRemoteOperation"

_GENERATOR = _AwsMetricAttributeGenerator()


def _presigned_url(host: str, path: str) -> str:
    """A realistic sanitized presigned URL: the agent redacts the credential and signature values
    before metric attribution runs. The non-redacted presigned parameters remain."""
    return (
        "https://" + host + path + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=REDACTED"
        "&X-Amz-Signature=REDACTED"
        "&X-Amz-Date=20260710T120000Z"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
    )


class TestPresignedUrlMetricAttributeGenerator(TestCase):
    def setUp(self):
        self.attributes_mock = MagicMock()
        scope_mock: InstrumentationScope = MagicMock()
        scope_mock.name = "Scope name"
        self.span_mock: ReadableSpan = MagicMock()
        self.span_mock.name = None
        self.span_mock.attributes = self.attributes_mock
        self.attributes_mock.get.return_value = None
        self.span_mock.instrumentation_scope = scope_mock
        self.span_mock.get_span_context.return_value = MagicMock()
        parent_span_context: SpanContext = MagicMock()
        parent_span_context.is_valid = True
        parent_span_context.is_remote = False
        self.span_mock.parent = parent_span_context
        self.span_mock.kind = SpanKind.CLIENT
        self.resource = _DEFAULT_RESOURCE

    def _mock_attributes(self, keys: List[str], values: List[Optional[str]]) -> None:
        def get_side_effect(get_key):
            if get_key in keys:
                return values[keys.index(get_key)]
            return None

        self.attributes_mock.get.side_effect = get_side_effect

    def _dependency_attributes(self):
        return _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )

    def test_presigned_s3_attribution_disabled_by_default(self):
        # The generic HTTP fallback in this distro reads only the legacy http.url/http.method
        # attributes, so the baseline attribution these tests compare against is set from those keys.
        os.environ.pop(_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG, None)
        self._mock_attributes(
            [SpanAttributes.HTTP_URL, SpanAttributes.HTTP_METHOD],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"), "PUT"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "example-bucket.s3.us-west-2.amazonaws.com")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "PUT /object")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, attributes)

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_attributes(self):
        self._mock_attributes(
            [SpanAttributes.URL_FULL, SpanAttributes.HTTP_REQUEST_METHOD],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"), "GET"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "AWS::S3")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "GetObject")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_TYPE], "AWS::S3::Bucket")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_IDENTIFIER], "example-bucket")

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_unknown_operation_does_not_fall_back_to_http_path(self):
        # Bucket-level GET (no object key) is ambiguous, so the resolver returns
        # UnknownRemoteOperation. The generic HTTP operation fallback must not overwrite it with a
        # high-cardinality "GET /..." value.
        self._mock_attributes(
            [SpanAttributes.URL_FULL, SpanAttributes.HTTP_REQUEST_METHOD],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/"), "GET"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "AWS::S3")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], _UNKNOWN_REMOTE_OPERATION)
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_TYPE], "AWS::S3::Bucket")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_IDENTIFIER], "example-bucket")

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_uses_legacy_http_url_fallback(self):
        self._mock_attributes(
            [SpanAttributes.HTTP_URL, SpanAttributes.HTTP_METHOD],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"), "HEAD"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "AWS::S3")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "HeadObject")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_TYPE], "AWS::S3::Bucket")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_IDENTIFIER], "example-bucket")

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_explicit_remote_attributes_win(self):
        self._mock_attributes(
            [SpanAttributes.URL_FULL, SpanAttributes.HTTP_REQUEST_METHOD, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION],
            [
                _presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"),
                "PUT",
                "AWS remote service",
                "AWS remote operation",
            ],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "AWS remote service")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "AWS remote operation")

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_does_not_attribute_aws_sdk_span(self):
        # An AWS SDK span (rpc.system=aws-api) must be excluded from presigned attribution even when
        # its rpc.service/rpc.method are absent, so it keeps the generic HTTP attribution.
        self._mock_attributes(
            [SpanAttributes.HTTP_URL, SpanAttributes.HTTP_METHOD, SpanAttributes.RPC_SYSTEM],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"), "GET", "aws-api"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "example-bucket.s3.us-west-2.amazonaws.com")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "GET /object")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, attributes)

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_peer_service_override_is_unchanged(self):
        self._mock_attributes(
            [SpanAttributes.URL_FULL, SpanAttributes.HTTP_REQUEST_METHOD, SpanAttributes.PEER_SERVICE],
            [_presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/object"), "PUT", "PeerService"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "PeerService")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "PutObject")
        # peer.service overrides the remote service but not the resource, mirroring the SDK path:
        # the S3 bucket resource stays attached even though the service is now the peer value.
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_TYPE], "AWS::S3::Bucket")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_IDENTIFIER], "example-bucket")

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_non_s3_presigned_endpoint_is_unchanged(self):
        self._mock_attributes(
            [SpanAttributes.HTTP_URL, SpanAttributes.HTTP_METHOD],
            [_presigned_url("sqs.us-west-2.amazonaws.com", "/123456789012/example-queue"), "GET"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "sqs.us-west-2.amazonaws.com")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "GET /123456789012")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, attributes)

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_presigned_s3_url_with_unrecognized_endpoint_is_unchanged(self):
        # An access-point host is not a recognized bucket-bearing S3 endpoint. Attribution fails
        # closed and the span keeps the existing generic HTTP attribution.
        self._mock_attributes(
            [SpanAttributes.HTTP_URL, SpanAttributes.HTTP_METHOD],
            [_presigned_url("example-bucket.s3-accesspoint.us-west-2.amazonaws.com", "/object"), "GET"],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_SERVICE], "example-bucket.s3-accesspoint.us-west-2.amazonaws.com")
        self.assertEqual(attributes[AWS_REMOTE_OPERATION], "GET /object")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, attributes)

    @patch.dict(os.environ, {_PRESIGNED_URL_ATTRIBUTION_ENABLED_CONFIG: "true"})
    def test_db_resource_attribution_unaffected_when_presigned_attribution_enabled(self):
        # Enabling presigned attribution must not shadow DB resource attribution.
        self._mock_attributes(
            [
                SpanAttributes.DB_SYSTEM,
                SpanAttributes.DB_NAME,
                SpanAttributes.SERVER_ADDRESS,
                SpanAttributes.SERVER_PORT,
            ],
            ["mysql", "db_name", "abc.com", 3306],
        )

        attributes = self._dependency_attributes()
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_TYPE], "DB::Connection")
        self.assertEqual(attributes[AWS_REMOTE_RESOURCE_IDENTIFIER], "db_name|abc.com|3306")
