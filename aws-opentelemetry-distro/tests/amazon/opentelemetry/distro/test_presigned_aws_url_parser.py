# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for PresignedAwsUrlParser.

The parser detects presigned SigV4/SigV4a requests from non-sensitive signals only. It must work
with the agent's default URL sanitization, which replaces the ``X-Amz-Credential`` and
``X-Amz-Signature`` values with ``REDACTED``; therefore these tests use redacted values. The
non-redacted presigned parameters (``X-Amz-Date``, ``X-Amz-Expires``, ``X-Amz-SignedHeaders``) are
required, so valid URLs include them.
"""

from unittest import TestCase

from amazon.opentelemetry.distro.presigned_aws_url_parser import PresignedAwsUrlParser

_OBJECT_URL = "https://example-bucket.s3.us-west-2.amazonaws.com/object"
_CREDENTIAL_AND_SIGNATURE = "&X-Amz-Credential=REDACTED&X-Amz-Signature=REDACTED"
_PRESIGN_PARAMS = "&X-Amz-Date=20260710T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host"


def _presigned_url(host: str, path: str, algorithm: str = "AWS4-HMAC-SHA256") -> str:
    """Builds a presigned URL with sanitized (redacted) credential and signature values."""
    return "https://" + host + path + "?X-Amz-Algorithm=" + algorithm + _CREDENTIAL_AND_SIGNATURE + _PRESIGN_PARAMS


class TestPresignedAwsUrlParser(TestCase):
    def test_detects_sigv4_presigned_request(self):
        parsed = PresignedAwsUrlParser.parse(
            _presigned_url("example-bucket.s3.us-west-2.amazonaws.com", "/photos/seed.jpg"), "GET"
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get_http_method(), "GET")
        self.assertEqual(parsed.get_host(), "example-bucket.s3.us-west-2.amazonaws.com")
        self.assertEqual(parsed.get_path(), "/photos/seed.jpg")

    def test_detects_sigv4a_presigned_request(self):
        parsed = PresignedAwsUrlParser.parse(
            _presigned_url("example-bucket.s3.amazonaws.com", "/object", "AWS4-ECDSA-P256-SHA256"), "GET"
        )
        self.assertIsNotNone(parsed)

    def test_detects_request_with_non_redacted_credential_and_signature(self):
        # Detection must also work before sanitization (e.g. when redaction is disabled), where the
        # credential and signature carry real values.
        parsed = PresignedAwsUrlParser.parse(
            "https://example-bucket.s3.us-west-2.amazonaws.com/object"
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Credential=AKIAEXAMPLE%2F20260710%2Fus-west-2%2Fs3%2Faws4_request"
            "&X-Amz-Signature=1234567890abcdef" + _PRESIGN_PARAMS,
            "GET",
        )
        self.assertIsNotNone(parsed)

    def test_parses_url_with_valueless_query_parameter_and_empty_path(self):
        parsed = PresignedAwsUrlParser.parse(
            "https://example-bucket.s3.amazonaws.com"
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256" + _CREDENTIAL_AND_SIGNATURE + _PRESIGN_PARAMS + "&x-id",
            "GET",
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get_path(), "/")

    def test_rejects_malformed_or_non_presigned_urls(self):
        cases = {
            "null url": None,
            "empty url": "",
            "plain url without SigV4 parameters": "https://example.com/object",
            "cloudfront signed url": (
                "https://d111111abcdef8.cloudfront.net/image.jpg?Policy=policy&Signature=sig&Key-Pair-Id=key"
            ),
            "missing algorithm": _OBJECT_URL + "?" + _CREDENTIAL_AND_SIGNATURE[1:] + _PRESIGN_PARAMS,
            "unsupported algorithm": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS5-FAKE"
            + _CREDENTIAL_AND_SIGNATURE
            + _PRESIGN_PARAMS,
            "missing credential": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=REDACTED"
            + _PRESIGN_PARAMS,
            "missing signature": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=REDACTED"
            + _PRESIGN_PARAMS,
            "empty credential": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=&X-Amz-Signature=REDACTED"
            + _PRESIGN_PARAMS,
            "empty signature": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=REDACTED&X-Amz-Signature="
            + _PRESIGN_PARAMS,
            "missing date": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + _CREDENTIAL_AND_SIGNATURE
            + "&X-Amz-Expires=3600&X-Amz-SignedHeaders=host",
            "missing expires": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + _CREDENTIAL_AND_SIGNATURE
            + "&X-Amz-Date=20260710T120000Z&X-Amz-SignedHeaders=host",
            "missing signed headers": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + _CREDENTIAL_AND_SIGNATURE
            + "&X-Amz-Date=20260710T120000Z&X-Amz-Expires=3600",
            "empty presigned parameter value": _OBJECT_URL
            + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + _CREDENTIAL_AND_SIGNATURE
            + "&X-Amz-Date=&X-Amz-Expires=3600&X-Amz-SignedHeaders=host",
            "url without host": "/object?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + _CREDENTIAL_AND_SIGNATURE
            + _PRESIGN_PARAMS,
        }
        for description, url in cases.items():
            with self.subTest(description):
                self.assertIsNone(PresignedAwsUrlParser.parse(url, "GET"))
