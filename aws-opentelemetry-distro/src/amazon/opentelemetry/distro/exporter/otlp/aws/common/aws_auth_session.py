# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session

_logger = logging.getLogger(__name__)


class AwsAuthSession(requests.Session):
    """
    A custom requests Session that adds AWS SigV4 authentication to HTTP requests.

    This class extends the standard requests.Session to automatically sign requests
    with AWS Signature Version 4 (SigV4) authentication. It's specifically designed
    for use with the OpenTelemetry Logs and Traces exporters that send data to AWS OTLP endpoints:
    X-Ray (traces) and CloudWatch Logs.

    The session requires botocore to be installed for signing headers. If botocore
    is not available, the session will fall back to standard unauthenticated requests
    and log an error message.

    Usage:
        session = AwsAuthSession(aws_region="us-west-2", service="logs")
        response = session.request("POST", "https://logs.us-west-2.amazonaws.com/v1/logs",
                                    data=payload, headers=headers)

    Args:
        aws_region (str): The AWS region to use for signing (e.g., "us-east-1")
        service (str): The AWS service name for signing (e.g., "logs" or "xray")
    """

    def __init__(self, aws_region: str, service: str, session: Session):
        self._aws_region: str = aws_region
        self._service: str = service
        self._session: Session = session

        super().__init__()

    def request(self, method, url, *args, data=None, headers=None, **kwargs):
        credentials = self._session.get_credentials()

        if credentials:
            signer = SigV4Auth(credentials, self._service, self._aws_region)
            request = AWSRequest(
                method="POST",
                url=url,
                data=data,
                headers={"Content-Type": "application/x-protobuf"},
            )

            try:
                signer.add_auth(request)

                if headers is None:
                    headers = {}

                headers.update(dict(request.headers))

            except Exception as signing_error:  # pylint: disable=broad-except
                _logger.error("Failed to sign request: %s", signing_error)
        else:
            _logger.error("Failed to load AWS Credentials: %s")

        return super().request(method=method, url=url, *args, data=data, headers=headers, **kwargs)
