# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for S3PresignedUrlAttributor.

S3 attribution is driven purely by the endpoint hostname (the signing service cannot be read from
the redacted credential scope). Tests use realistic sanitized URLs (redacted credential and
signature).
"""

from typing import Optional
from unittest import TestCase

from amazon.opentelemetry.distro.presigned_aws_url_parser import PresignedAwsUrlParser
from amazon.opentelemetry.distro.s3_presigned_url_attributor import S3PresignedUrlAttributor


def _presigned_url(method: Optional[str], host: str, path: str, extra_query_parameters: str = ""):
    """Builds a presigned URL with sanitized (redacted) credential and signature values."""
    return PresignedAwsUrlParser.parse(
        "https://" + host + path + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=REDACTED"
        "&X-Amz-Signature=REDACTED"
        "&X-Amz-Date=20260710T120000Z"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host" + extra_query_parameters,
        method,
    )


class TestS3PresignedUrlAttributor(TestCase):
    def _attribute(self, url):
        """Unwraps an attribution expected to be present."""
        attribution = S3PresignedUrlAttributor.attribute(url)
        self.assertIsNotNone(attribution)
        return attribution

    def test_resolves_bucket_for_endpoint_variant(self):
        # host, path, expected bucket
        cases = [
            # Virtual-hosted style
            ("example-bucket.s3.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3.us-west-2.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3-us-west-2.amazonaws.com", "/object", "example-bucket"),
            ("example.s3.bucket.s3.us-west-2.amazonaws.com", "/object", "example.s3.bucket"),
            ("example-bucket.s3.cn-north-1.amazonaws.com.cn", "/object", "example-bucket"),
            ("example-bucket.s3.dualstack.us-west-2.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3-accelerate.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3-accelerate.dualstack.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3-fips.us-west-2.amazonaws.com", "/object", "example-bucket"),
            ("example-bucket.s3-fips.dualstack.us-east-1.amazonaws.com", "/object", "example-bucket"),
            # Path-style: bucket is the first path segment
            ("s3.amazonaws.com", "/example-bucket/object", "example-bucket"),
            ("s3.us-west-2.amazonaws.com", "/example-bucket/object", "example-bucket"),
            ("s3.cn-north-1.amazonaws.com.cn", "/example-bucket/object", "example-bucket"),
            ("s3-fips.us-west-2.amazonaws.com", "/example-bucket/object", "example-bucket"),
            ("s3-fips.dualstack.us-east-1.amazonaws.com", "/example-bucket/object", "example-bucket"),
        ]
        for host, path, expected_bucket in cases:
            with self.subTest(host):
                attribution = self._attribute(_presigned_url("GET", host, path))
                self.assertEqual(attribution.get_remote_service(), "AWS::S3")
                self.assertIsNotNone(attribution.get_remote_resource())
                self.assertEqual(attribution.get_remote_resource().get_type(), "AWS::S3::Bucket")
                self.assertEqual(attribution.get_remote_resource().get_identifier(), expected_bucket)

    def test_resolves_operation(self):
        # method, path, extra query params, expected operation
        cases = [
            ("GET", "/object", "", "GetObject"),
            ("PUT", "/object", "", "PutObject"),
            ("HEAD", "/object", "", "HeadObject"),
            ("DELETE", "/object", "", "DeleteObject"),
            ("PATCH", "/object", "", "UnknownRemoteOperation"),
            # ListObjectsV2 is bucket-level only
            ("GET", "/", "&list-type=2", "ListObjectsV2"),
            ("GET", "/object", "&list-type=2", "GetObject"),
            ("PUT", "/object", "&list-type=2", "PutObject"),
            # Multipart
            ("PUT", "/object", "&partNumber=1&uploadId=upload", "UploadPart"),
            ("PUT", "/object", "&uploadId=upload", "PutObject"),
            ("GET", "/object", "&uploadId=upload", "ListParts"),
            ("POST", "/object", "&uploadId=upload", "CompleteMultipartUpload"),
            ("DELETE", "/object", "&uploadId=upload", "AbortMultipartUpload"),
            ("POST", "/object", "&uploads", "CreateMultipartUpload"),
            ("GET", "/", "&uploads", "ListMultipartUploads"),
            ("GET", "/object", "&uploads", "GetObject"),
            # ACL / tagging (object- and bucket-level)
            ("GET", "/object", "&acl", "GetObjectAcl"),
            ("PUT", "/object", "&acl", "PutObjectAcl"),
            ("GET", "/", "&acl", "GetBucketAcl"),
            ("PUT", "/", "&acl", "PutBucketAcl"),
            ("GET", "/object", "&tagging", "GetObjectTagging"),
            ("PUT", "/object", "&tagging", "PutObjectTagging"),
            ("DELETE", "/object", "&tagging", "DeleteObjectTagging"),
            ("GET", "/", "&tagging", "GetBucketTagging"),
            ("PUT", "/", "&tagging", "PutBucketTagging"),
            ("DELETE", "/", "&tagging", "DeleteBucketTagging"),
            # Object-only subresources
            ("GET", "/object", "&retention", "GetObjectRetention"),
            ("PUT", "/object", "&retention", "PutObjectRetention"),
            ("GET", "/object", "&legal-hold", "GetObjectLegalHold"),
            ("PUT", "/object", "&legal-hold", "PutObjectLegalHold"),
            ("GET", "/object", "&torrent", "GetObjectTorrent"),
        ]
        for method, path, extra_query, expected_operation in cases:
            with self.subTest(f"{method} {path} {extra_query}"):
                attribution = self._attribute(
                    _presigned_url(method, "example-bucket.s3.us-west-2.amazonaws.com", path, extra_query)
                )
                self.assertEqual(attribution.get_remote_operation(), expected_operation)

    def test_resolves_path_style_operation(self):
        # method, path, extra query params, expected operation
        cases = [
            ("GET", "/example-bucket", "&list-type=2", "ListObjectsV2"),
            ("GET", "/example-bucket/object", "", "GetObject"),
            ("DELETE", "/example-bucket/object", "", "DeleteObject"),
            ("GET", "/example-bucket", "&acl", "GetBucketAcl"),
            ("GET", "/example-bucket/object", "&acl", "GetObjectAcl"),
        ]
        for method, path, extra_query, expected_operation in cases:
            with self.subTest(f"{method} {path} {extra_query}"):
                attribution = self._attribute(_presigned_url(method, "s3.us-west-2.amazonaws.com", path, extra_query))
                self.assertEqual(attribution.get_remote_operation(), expected_operation)

    def test_fails_closed_for_unrecognized_endpoint(self):
        hosts = [
            # Access point host (bucket not identifiable from the endpoint form)
            "example-bucket.s3-accesspoint.us-west-2.amazonaws.com",
            # Custom CNAME
            "s3.mycompany.com",
            # Non-S3 AWS service endpoint
            "sqs.us-west-2.amazonaws.com",
        ]
        for host in hosts:
            with self.subTest(host):
                self.assertIsNone(S3PresignedUrlAttributor.attribute(_presigned_url("GET", host, "/object")))

    def test_uses_unknown_operation_for_ambiguous_bucket_operation(self):
        attribution = self._attribute(_presigned_url("GET", "example-bucket.s3.us-west-2.amazonaws.com", "/"))

        self.assertEqual(attribution.get_remote_service(), "AWS::S3")
        self.assertEqual(attribution.get_remote_operation(), "UnknownRemoteOperation")
        self.assertIsNotNone(attribution.get_remote_resource())

    def test_missing_http_method_uses_unknown_operation(self):
        attribution = self._attribute(_presigned_url(None, "example-bucket.s3.us-west-2.amazonaws.com", "/object"))

        self.assertEqual(attribution.get_remote_service(), "AWS::S3")
        self.assertEqual(attribution.get_remote_operation(), "UnknownRemoteOperation")

    def test_path_style_without_bucket_attributes_s3_without_resource(self):
        attribution = self._attribute(_presigned_url("GET", "s3.us-west-2.amazonaws.com", "/"))

        self.assertEqual(attribution.get_remote_service(), "AWS::S3")
        self.assertIsNone(attribution.get_remote_resource())
