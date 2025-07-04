# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.

import gzip
import logging
import random
from io import BytesIO
from threading import Event
from time import time
from typing import Dict, Optional, Sequence

from botocore.session import Session
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.structures import CaseInsensitiveDict

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LogData
from opentelemetry.sdk._logs.export import LogExportResult

_logger = logging.getLogger(__name__)
_MAX_RETRYS = 6


class OTLPAwsLogExporter(OTLPLogExporter):
    """
    This exporter extends the functionality of the OTLPLogExporter to allow logs to be exported
    to the CloudWatch Logs OTLP endpoint https://logs.[AWSRegion].amazonaws.com/v1/logs. Utilizes the aws-sdk
    library to sign and directly inject SigV4 Authentication to the exported request's headers.

    See: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    _RETRY_AFTER_HEADER = "Retry-After"  # See: https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling

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

        OTLPLogExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression=Compression.Gzip,
            session=AwsAuthSession(session=session, aws_region=self._aws_region, service="logs"),
        )
        self._shutdown_event = Event()

    def export(self, batch: Sequence[LogData]) -> LogExportResult:
        """
        Exports log batch with AWS-specific enhancements over the base OTLPLogExporter.

        Key differences from upstream OTLPLogExporter:
        1. Respects Retry-After header from server responses for proper throttling
        2. Treats HTTP 429 (Too Many Requests) as a retryable exception
        3. Always compresses data with gzip before sending

        Upstream implementation does not support Retry-After header:
        https://github.com/open-telemetry/opentelemetry-python/blob/acae2c232b101d3e447a82a7161355d66aa06fa2/exporter/opentelemetry-exporter-otlp-proto-http/src/opentelemetry/exporter/otlp/proto/http/_log_exporter/__init__.py#L167
        """

        if self._shutdown:
            _logger.warning("Exporter already shutdown, ignoring batch")
            return LogExportResult.FAILURE

        serialized_data = encode_logs(batch).SerializeToString()
        gzip_data = BytesIO()
        with gzip.GzipFile(fileobj=gzip_data, mode="w") as gzip_stream:
            gzip_stream.write(serialized_data)
        data = gzip_data.getvalue()

        deadline_sec = time() + self._timeout
        retry_num = 0

        # This loop will eventually terminate because:
        # 1) The export request will eventually either succeed or fail permanently
        # 2) Maximum retries (_MAX_RETRYS = 6) will be reached
        # 3) Deadline timeout will be exceeded
        # 4) Non-retryable errors (4xx except 429) immediately exit the loop
        while True:
            resp = self._send(data, deadline_sec - time())

            if resp.ok:
                return LogExportResult.SUCCESS

            backoff_seconds = self._get_retry_delay_sec(resp.headers, retry_num)
            is_retryable = self._retryable(resp)

            if not is_retryable or retry_num + 1 == _MAX_RETRYS or backoff_seconds > (deadline_sec - time()):
                _logger.error(
                    "Failed to export logs batch code: %s, reason: %s",
                    resp.status_code,
                    resp.text,
                )
                return LogExportResult.FAILURE

            _logger.warning(
                "Transient error %s encountered while exporting logs batch, retrying in %.2fs.",
                resp.reason,
                backoff_seconds,
            )
            # Use interruptible sleep that can be interrupted by shutdown
            if self._shutdown_event.wait(backoff_seconds):
                _logger.info("Export interrupted by shutdown")
                return LogExportResult.FAILURE

            retry_num += 1

    def shutdown(self) -> None:
        """Shutdown the exporter and interrupt any ongoing waits."""
        self._shutdown_event.set()
        return super().shutdown()

    def _send(self, serialized_data: bytes, timeout_sec: float):
        try:
            response = self._session.post(
                url=self._endpoint,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=timeout_sec,
                cert=self._client_cert,
            )
            return response
        except RequestsConnectionError:
            response = self._session.post(
                url=self._endpoint,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=timeout_sec,
                cert=self._client_cert,
            )
            return response

    @staticmethod
    def _retryable(resp: Response) -> bool:
        """
        Logic based on https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
        """
        # See: https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling

        return resp.status_code in (429, 503) or OTLPLogExporter._retryable(resp)

    def _get_retry_delay_sec(self, headers: CaseInsensitiveDict, retry_num: int) -> float:
        """
        Get retry delay in seconds from headers or backoff strategy.
        """
        # Check for Retry-After header first, then use exponential backoff with jitter
        retry_after_delay = self._parse_retryable_header(headers.get(self._RETRY_AFTER_HEADER))
        if retry_after_delay > -1:
            return retry_after_delay
        # multiplying by a random number between .8 and 1.2 introduces a +/-20% jitter to each backoff.
        return 2**retry_num * random.uniform(0.8, 1.2)

    @staticmethod
    def _parse_retryable_header(retry_header: Optional[str]) -> float:
        """
        Converts the given retryable header into a delay in seconds, returns -1 if there's no header
        or error with the parsing
        """
        if not retry_header:
            return -1

        try:
            val = float(retry_header)
            return val if val >= 0 else -1
        except ValueError:
            return -1
