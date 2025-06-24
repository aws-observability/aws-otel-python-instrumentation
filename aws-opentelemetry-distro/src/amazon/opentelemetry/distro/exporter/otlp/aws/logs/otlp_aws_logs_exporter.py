# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gzip
import logging
from io import BytesIO
from time import sleep
from typing import Dict, Optional, Sequence

from requests import Response
from requests.exceptions import ConnectionError
from requests.structures import CaseInsensitiveDict

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from opentelemetry.exporter.otlp.proto.common._internal import _create_exp_backoff_generator
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LogData
from opentelemetry.sdk._logs.export import LogExportResult

_logger = logging.getLogger(__name__)


class OTLPAwsLogExporter(OTLPLogExporter):

    _RETRY_AFTER_HEADER = "Retry-After"  # See: https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling

    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ):
        self._aws_region = None

        if endpoint:
            self._aws_region = endpoint.split(".")[1]

        OTLPLogExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression=Compression.Gzip,
            session=AwsAuthSession(aws_region=self._aws_region, service="logs"),
        )

    def export(self, batch: Sequence[LogData]) -> LogExportResult:
        """
        Exports log batch with AWS-specific enhancements over the base OTLPLogExporter.

        Based on upstream implementation which does not retry based on Retry-After header:
        https://github.com/open-telemetry/opentelemetry-python/blob/acae2c232b101d3e447a82a7161355d66aa06fa2/exporter/opentelemetry-exporter-otlp-proto-http/src/opentelemetry/exporter/otlp/proto/http/_log_exporter/__init__.py#L167

        Key behaviors:
        1. Always compresses data with gzip before sending
        2. Adds truncatable fields header for large Gen AI logs (>1MB)
        3. Implements Retry-After header support for throttling responses
        """

        if self._shutdown:
            _logger.warning("Exporter already shutdown, ignoring batch")
            return LogExportResult.FAILURE

        serialized_data = encode_logs(batch).SerializeToString()
        gzip_data = BytesIO()
        with gzip.GzipFile(fileobj=gzip_data, mode="w") as gzip_stream:
            gzip_stream.write(serialized_data)
        data = gzip_data.getvalue()

        backoff = _create_exp_backoff_generator(max_value=self._MAX_RETRY_TIMEOUT)

        while True:
            resp = self._send(data)

            if resp.ok:
                return LogExportResult.SUCCESS

            delay = self._get_retry_delay_sec(resp.headers, backoff)
            is_retryable = self._retryable(resp)

            if not is_retryable or delay == self._MAX_RETRY_TIMEOUT:
                if is_retryable:
                    _logger.error(
                        "Failed to export logs due to retries exhausted "
                        "after transient error %s encountered while exporting logs batch",
                        resp.reason,
                    )
                else:
                    _logger.error(
                        "Failed to export logs batch code: %s, reason: %s",
                        resp.status_code,
                        resp.text,
                    )
                return LogExportResult.FAILURE

            _logger.warning(
                "Transient error %s encountered while exporting logs batch, retrying in %ss.",
                resp.reason,
                delay,
            )

            sleep(delay)

    def _send(self, serialized_data: bytes):
        try:
            response = self._session.post(
                url=self._endpoint,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=self._timeout,
                cert=self._client_cert,
            )
            return response
        except ConnectionError:
            response = self._session.post(
                url=self._endpoint,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=self._timeout,
                cert=self._client_cert,
            )
            return response

    @staticmethod
    def _retryable(resp: Response) -> bool:
        """
        Is it a retryable response?
        """
        # See: https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling

        return resp.status_code in (429, 503) or OTLPLogExporter._retryable(resp)

    def _get_retry_delay_sec(self, headers: CaseInsensitiveDict, backoff) -> float:
        """
        Get retry delay in seconds from headers or backoff strategy.
        """
        # See: https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
        maybe_retry_after = headers.get(self._RETRY_AFTER_HEADER, None)

        # Set the next retry delay to the value of the Retry-After response in the headers.
        # If Retry-After is not present in the headers, default to the next iteration of the
        # exponential backoff strategy.

        delay = self._parse_retryable_header(maybe_retry_after)

        if delay == -1:
            delay = next(backoff, self._MAX_RETRY_TIMEOUT)

        return delay

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
