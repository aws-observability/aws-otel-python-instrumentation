# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from amazon.opentelemetry.distro.presigned_aws_url import PresignedAwsUrl
from amazon.opentelemetry.distro.presigned_aws_url_parser import PresignedAwsUrlParser
from opentelemetry.sdk.trace import ReadableSpan


class RemoteResource:
    def __init__(self, resource_type: str, identifier: str):
        self._type = resource_type
        self._identifier = identifier

    def get_type(self) -> str:
        return self._type

    def get_identifier(self) -> str:
        return self._identifier


class PresignedUrlAttribution:
    """The Application Signals remote attribution derived from a presigned AWS URL.

    A resource is present only when the service-specific attributor can identify it confidently.
    """

    def __init__(self, remote_service: str, remote_operation: str, remote_resource: Optional[RemoteResource]):
        self._remote_service = remote_service
        self._remote_operation = remote_operation
        self._remote_resource = remote_resource

    def get_remote_service(self) -> str:
        return self._remote_service

    def get_remote_operation(self) -> str:
        return self._remote_operation

    def get_remote_resource(self) -> Optional[RemoteResource]:
        return self._remote_resource


class PresignedUrlAttributor:
    """Derives Application Signals attribution from a presigned AWS URL.

    Parses the span's URL once, then lets each service-specific attributor try to claim it based on
    the endpoint hostname (the signing service cannot be read from the credential scope because it
    is redacted). If none claims the URL - custom CNAMEs, unknown endpoints, or non-presigned URLs -
    attribution falls back to the existing behavior.
    """

    @staticmethod
    def attribute(span: ReadableSpan) -> Optional[PresignedUrlAttribution]:
        presigned_aws_url = PresignedAwsUrlParser.parse_span(span)
        if presigned_aws_url is None:
            return None
        return PresignedUrlAttributor._attribute_url(presigned_aws_url)

    @staticmethod
    def _attribute_url(presigned_aws_url: PresignedAwsUrl) -> Optional[PresignedUrlAttribution]:
        # Only S3 is supported today. Additional services (e.g. SQS, execute-api) can be tried here
        # in turn, each claiming the URL only when it recognizes the endpoint.
        # pylint: disable=import-outside-toplevel,cyclic-import
        from amazon.opentelemetry.distro.s3_presigned_url_attributor import S3PresignedUrlAttributor

        return S3PresignedUrlAttributor.attribute(presigned_aws_url)
