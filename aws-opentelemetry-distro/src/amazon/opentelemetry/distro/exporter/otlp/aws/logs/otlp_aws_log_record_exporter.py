# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

from botocore.session import Session

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter


class OTLPAwsLogRecordExporter(OTLPLogExporter):
    """
    This exporter extends the functionality of the OTLPLogExporter to allow logs to be exported
    to the CloudWatch Logs OTLP endpoint https://logs.[AWSRegion].amazonaws.com/v1/logs. Utilizes the aws-sdk
    library to sign and directly inject SigV4 Authentication to the exported request's headers.

    Differences from upstream OTLPLogExporter:
    1. Requests are SigV4 signed via AwsAuthSession
    2. Always compresses data with gzip before sending
    3. Optionally sets the x-aws-log-group / x-aws-log-stream headers

    Retry behavior (Retry-After header support, retrying HTTP 429/502/503/504, and interruptible
    backoff on shutdown) is provided by the upstream OTLP HTTP client.

    See: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    def __init__(
        self,
        aws_region: str,
        session: Session,
        log_group: Optional[str] = None,
        log_stream: Optional[str] = None,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ):
        self._aws_region = aws_region

        if log_group and log_stream:
            log_headers = {"x-aws-log-group": log_group, "x-aws-log-stream": log_stream}
            if headers:
                headers.update(log_headers)
            else:
                headers = log_headers

        self._session = AwsAuthSession(session=session, aws_region=self._aws_region, service="logs")
        OTLPLogExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression=Compression.Gzip,
            session=self._session,
        )
