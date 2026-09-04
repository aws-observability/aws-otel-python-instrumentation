# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import List, Optional

from amazon.opentelemetry.distro._aws_span_processing_util import UNKNOWN_REMOTE_OPERATION
from amazon.opentelemetry.distro.presigned_aws_url import PresignedAwsUrl
from amazon.opentelemetry.distro.presigned_url_attributor import PresignedUrlAttribution, RemoteResource

_NORMALIZED_S3_SERVICE_NAME = "AWS::S3"
_S3_BUCKET_RESOURCE_TYPE = _NORMALIZED_S3_SERVICE_NAME + "::Bucket"

# Standard S3 endpoint host forms, including global, regional, legacy regional, dual-stack,
# transfer acceleration, FIPS (incl. FIPS dual-stack), and China (.com.cn). The optional segment
# after "s3" covers the mutually exclusive endpoint styles.
#
# The legacy "-<label>" alternative is intentionally broad: besides legacy regional hosts
# (s3-us-west-2) it also matches other s3-prefixed AWS hosts such as s3-website-<region>. This is
# accepted deliberately as low risk - all such hosts are S3-owned domains anchored to
# amazonaws.com, and presigned object requests do not target website/other endpoints.
# https://docs.aws.amazon.com/general/latest/gr/s3.html
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/dual-stack-endpoints.html
_S3_ENDPOINT_SUFFIX = (
    r"s3(?:"
    r"\.(?:dualstack\.)?[a-z0-9-]+"  # s3.<region> | s3.dualstack.<region>
    r"|-fips(?:\.dualstack)?\.[a-z0-9-]+"  # s3-fips.<region> | s3-fips.dualstack.<region>
    r"|-accelerate(?:\.dualstack)?"  # s3-accelerate | s3-accelerate.dualstack
    r"|-[a-z0-9-]+"  # s3-<region> (legacy regional)
    r")?\.amazonaws\.com(?:\.cn)?"
)
_VIRTUAL_HOSTED_S3_ENDPOINT = re.compile(r"^(.+)\." + _S3_ENDPOINT_SUFFIX + r"$", re.IGNORECASE)
_PATH_STYLE_S3_ENDPOINT = re.compile(r"^" + _S3_ENDPOINT_SUFFIX + r"$", re.IGNORECASE)


class S3PresignedUrlAttributor:
    """Derives ``AWS::S3`` attribution from a presigned S3 URL by recognizing S3 endpoint hostnames.

    Because the signing service cannot be read from the (redacted) credential scope, S3 is
    identified purely from the endpoint host. Only the standard virtual-hosted and path-style S3
    endpoint forms are recognized. Anything else - custom CNAMEs, access points, unknown endpoints -
    fails closed (returns None) so we never mis-attribute a non-S3 or unverifiable request.

    The remote operation is derived from the HTTP method, whether an object key is present (bucket-
    vs object-level), and the S3 subresource/multipart query parameters. Operation names follow the
    S3 REST API. References:
      - Endpoints: https://docs.aws.amazon.com/general/latest/gr/s3.html
      - Virtual-hosted vs path-style:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
      - S3 REST API operations: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html
    """

    @staticmethod
    def attribute(presigned_aws_url: PresignedAwsUrl) -> Optional[PresignedUrlAttribution]:
        host = presigned_aws_url.get_host()
        if host is None:
            return None
        path_style = _PATH_STYLE_S3_ENDPOINT.match(host) is not None

        if path_style:
            bucket = _get_path_style_bucket(presigned_aws_url)
        else:
            bucket = _get_virtual_hosted_style_bucket(host)
            if bucket is None:
                # Not a recognized S3 endpoint (custom CNAME, access point, unknown host). Fail
                # closed: the signing service cannot be recovered from a redacted credential scope.
                return None

        remote_resource = None
        if bucket is not None:
            remote_resource = RemoteResource(_S3_BUCKET_RESOURCE_TYPE, bucket)

        return PresignedUrlAttribution(
            _NORMALIZED_S3_SERVICE_NAME,
            _get_remote_operation(presigned_aws_url, path_style),
            remote_resource,
        )


# pylint: disable=too-many-return-statements,too-many-branches
def _get_remote_operation(presigned_aws_url: PresignedAwsUrl, path_style: bool) -> str:
    http_method = presigned_aws_url.get_http_method()
    if http_method is None:
        return UNKNOWN_REMOTE_OPERATION

    normalized_method = http_method.upper()
    has_object_key = _has_object_key(presigned_aws_url, path_style)

    # ListObjectsV2 is a bucket-level GET (no object key).
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    if (
        normalized_method == "GET"
        and not has_object_key
        and presigned_aws_url.get_first_query_parameter_value("list-type") == "2"
    ):
        return "ListObjectsV2"

    # S3 multipart REST API operations.
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
    if presigned_aws_url.get_first_query_parameter_value("uploadId") is not None:
        if normalized_method == "PUT" and presigned_aws_url.get_first_query_parameter_value("partNumber") is not None:
            return "UploadPart"
        if normalized_method == "GET":
            return "ListParts"
        if normalized_method == "POST":
            return "CompleteMultipartUpload"
        if normalized_method == "DELETE":
            return "AbortMultipartUpload"

    if presigned_aws_url.get_first_query_parameter_value("uploads") is not None:
        if normalized_method == "POST" and has_object_key:
            return "CreateMultipartUpload"
        if normalized_method == "GET" and not has_object_key:
            return "ListMultipartUploads"

    # Subresource operations selected by a query parameter. They are object-level when an object key
    # is present and bucket-level otherwise.
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
    if presigned_aws_url.get_first_query_parameter_value("acl") is not None:
        if normalized_method == "GET":
            return "GetObjectAcl" if has_object_key else "GetBucketAcl"
        if normalized_method == "PUT":
            return "PutObjectAcl" if has_object_key else "PutBucketAcl"
    if presigned_aws_url.get_first_query_parameter_value("tagging") is not None:
        if normalized_method == "GET":
            return "GetObjectTagging" if has_object_key else "GetBucketTagging"
        if normalized_method == "PUT":
            return "PutObjectTagging" if has_object_key else "PutBucketTagging"
        if normalized_method == "DELETE":
            return "DeleteObjectTagging" if has_object_key else "DeleteBucketTagging"

    # Object-only subresources. These operate on an object, so they require an object key.
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html
    if has_object_key:
        if presigned_aws_url.get_first_query_parameter_value("retention") is not None:
            if normalized_method == "GET":
                return "GetObjectRetention"
            if normalized_method == "PUT":
                return "PutObjectRetention"
        if presigned_aws_url.get_first_query_parameter_value("legal-hold") is not None:
            if normalized_method == "GET":
                return "GetObjectLegalHold"
            if normalized_method == "PUT":
                return "PutObjectLegalHold"
        if normalized_method == "GET" and presigned_aws_url.get_first_query_parameter_value("torrent") is not None:
            return "GetObjectTorrent"

    if not has_object_key:
        return UNKNOWN_REMOTE_OPERATION

    if normalized_method == "GET":
        return "GetObject"
    if normalized_method == "HEAD":
        return "HeadObject"
    if normalized_method == "PUT":
        return "PutObject"
    if normalized_method == "DELETE":
        return "DeleteObject"
    return UNKNOWN_REMOTE_OPERATION


def _has_object_key(presigned_aws_url: PresignedAwsUrl, path_style: bool) -> bool:
    path_segments = _get_path_segments(presigned_aws_url.get_path())
    if path_style:
        # Path-style URLs carry the bucket as the first path segment, so an object key requires a
        # second segment.
        return len(path_segments) > 1
    return len(path_segments) > 0


def _get_path_style_bucket(presigned_aws_url: PresignedAwsUrl) -> Optional[str]:
    path_segments = _get_path_segments(presigned_aws_url.get_path())
    if len(path_segments) == 0:
        return None
    return path_segments[0]


def _get_virtual_hosted_style_bucket(host: str) -> Optional[str]:
    matcher = _VIRTUAL_HOSTED_S3_ENDPOINT.match(host)
    if matcher is None:
        return None
    return matcher.group(1)


def _get_path_segments(path: str) -> List[str]:
    normalized_path = path if path is not None else ""
    normalized_path = normalized_path.lstrip("/")
    if normalized_path == "":
        return []
    # Drop empty segments so a trailing slash (e.g. path-style "/bucket/") is not misread as an
    # object key.
    return [segment for segment in normalized_path.split("/") if segment != ""]
