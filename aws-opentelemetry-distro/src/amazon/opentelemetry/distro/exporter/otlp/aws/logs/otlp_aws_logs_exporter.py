# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gzip
import logging
from io import BytesIO
from time import sleep
from typing import Dict, Mapping, Optional, Sequence

import requests

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.common.constants import BASE_LOG_BUFFER_BYTE_SIZE
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter, _create_exp_backoff_generator
from opentelemetry.sdk._logs import (
    LogData,
)
from opentelemetry.sdk._logs.export import (
    LogExportResult,
)

_logger = logging.getLogger(__name__)


class OTLPAwsLogExporter(OTLPLogExporter):
    COUNT = 0
    _LARGE_LOG_HEADER = {"x-aws-log-semantics": "otel"}
    _RETRY_AFTER_HEADER = "Retry-After"  # https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling

    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ):
        self._gen_ai_flag = False
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

    # Code based off of:
    # https://github.com/open-telemetry/opentelemetry-python/blob/main/exporter/opentelemetry-exporter-otlp-proto-http/src/opentelemetry/exporter/otlp/proto/http/_log_exporter/__init__.py#L167
    def export(self, batch: Sequence[LogData]) -> LogExportResult:

        print(f"Exporting batch of {len(batch)} logs")
        print("TOTAL DATA SIZE " + str(sum(self._get_size_of_log(logz) for logz in batch)))
        self.COUNT += len(batch)
        print("COUNT " + str(self.COUNT))

        """
        Exports the given batch of OTLP log data.
        Behaviors of how this export will work -

        1. Always compresses the serialized data into gzip before sending.

        2. If self._gen_ai_flag is enabled, the log data is > 1 MB a
           and the assumption is that the log is a normalized gen.ai LogEvent.
            - inject the 'x-aws-log-semantics' flag into the header.

        3. Retry behavior is now the following:
            - if the response contains a status code that is retryable and the response contains Retry-After in its
              headers, the serialized data will be exported after that set delay

            - if the response does not contain that Retry-After header, default back to the current iteration of the
              exponential backoff delay
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

            print(f"Response status: {resp.status_code}")
            print(f"Response headers: {resp.headers}")
            try:
                print(f"Response body: {resp.text}")
            except:
                print("Could not print response body")

            if resp.ok:
                return LogExportResult.SUCCESS

            if not self._retryable(resp):
                _logger.error(
                    "Failed to export logs batch code: %s, reason: %s",
                    resp.status_code,
                    resp.text,
                )
                self._gen_ai_flag = False
                return LogExportResult.FAILURE

            # https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
            maybe_retry_after = resp.headers.get(self._RETRY_AFTER_HEADER, None)

            # Set the next retry delay to the value of the Retry-After response in the headers.
            # If Retry-After is not present in the headers, default to the next iteration of the
            # exponential backoff strategy.

            delay = self._parse_retryable_header(maybe_retry_after)

            if delay == -1:
                delay = next(backoff, self._MAX_RETRY_TIMEOUT)

            if delay == self._MAX_RETRY_TIMEOUT:
                _logger.error(
                    "Transient error %s encountered while exporting logs batch. "
                    "No Retry-After header found and all backoff retries exhausted. "
                    "Logs will not be exported.",
                    resp.reason,
                )
                self._gen_ai_flag = False
                return LogExportResult.FAILURE

            _logger.warning(
                "Transient error %s encountered while exporting logs batch, retrying in %ss.",
                resp.reason,
                delay,
            )

            sleep(delay)

    def set_gen_ai_flag(self):
        """
        Sets the gen_ai flag to true to signal injecting the LLO flag to the headers of the export request.
        """
        self._gen_ai_flag = True

    def _send(self, serialized_data: bytes):
        try:
            return self._session.post(
                url=self._endpoint,
                headers=self._LARGE_LOG_HEADER if self._gen_ai_flag else None,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=self._timeout,
                cert=self._client_cert,
            )
        except ConnectionError:
            return self._session.post(
                url=self._endpoint,
                headers=self._LARGE_LOG_HEADER if self._gen_ai_flag else None,
                data=serialized_data,
                verify=self._certificate_file,
                timeout=self._timeout,
                cert=self._client_cert,
            )

    @staticmethod
    def _retryable(resp: requests.Response) -> bool:
        """
        Is it a retryable response?
        """
        if resp.status_code == 429 or resp.status_code == 503:
            return True

        return OTLPLogExporter._retryable(resp)

    def _parse_retryable_header(self, retry_header: Optional[str]) -> float:
        """
        Converts the given retryable header into a delay in seconds, returns -1 if there's no header
        or error with the parsing
        """

        if not retry_header:
            return -1

        try:
            return float(retry_header)
        except ValueError:
            return -1

    def _get_size_of_log(self, log_data: LogData):
        # Rough estimate of the size of the LogData based on size of
        # the content body + a buffer to account for other information in logs.
        size = BASE_LOG_BUFFER_BYTE_SIZE
        body = log_data.log_record.body

        if body:
            size += self._get_size_of_any_value(log_data.log_record.body)

        return size

    def _get_size_of_any_value(self, val) -> int:
        size = 0

        if isinstance(val, str) or isinstance(val, bytes):
            return len(val)

        if isinstance(val, bool):
            if val:
                return 4  # len(True) = 4
            return 5  # len(False) = 5

        if isinstance(val, int) or isinstance(val, float):
            return len(str(val))

        if isinstance(val, Sequence):
            for content in val:
                size += self._get_size_of_any_value(content)

        if isinstance(val, Mapping):
            for _, content in val.items():
                size += self._get_size_of_any_value(content)

        return size
