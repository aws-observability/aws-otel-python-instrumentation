# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gzip
from io import BytesIO
import logging
from time import sleep
from typing import Dict, Mapping, Optional, Sequence

from email.utils import parsedate_to_datetime
from datetime import datetime

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.common.constants import MAX_LOG_REQUEST_BYTE_SIZE, BASE_LOG_BUFFER_BYTE_SIZE
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter, _create_exp_backoff_generator
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.sdk._logs.export import (
    LogExportResult,
)
from opentelemetry.sdk._logs import (
    LogData,
)

import requests


_logger = logging.getLogger(__name__)

class OTLPAwsLogExporter(OTLPLogExporter):
    _LARGE_LOG_HEADER = {'x-aws-log-semantics': 'otel'}
    _RETRY_AFTER_HEADER = 'Retry-After'
    
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
    
    def export(self, batch: Sequence[LogData]) -> LogExportResult:
        print(f"Exporting batch of {len(batch)} logs")
        print("TOTAL DATA SIZE " + str(sum(self._get_size_of_log(logz) for logz in batch)))
        print("GEN_AI_FLAG " + str(self._gen_ai_flag))

        return super().export(batch)
    
    def set_gen_ai_flag(self):
        self._gen_ai_flag = True

    @staticmethod
    def _retryable(resp: requests.Response) -> bool:
        if resp.status_code == 429 or resp.status_code == 503:
            return True

        return OTLPLogExporter._retryable(resp)

    def _export(self, serialized_data: bytes) -> requests.Response:
        """
        Exports the given serialized OTLP log data. Behaviors of how this export will work.

        1. Always compresses the serialized data into gzip before sending.
        
        2. If self._gen_ai_flag is enabled, the log data is > 1 MB and we assume that the log contains normalized gen.ai attributes.
            - in this case we inject the 'x-aws-log-semantics' flag into the header.
        
        3. Retry behavior is now the following: 
            - if the response contains a status code that is retryable and the response contains Retry-After in its headers, 
              the serialized data will be exported after that set delay

            - if the reponse does not contain that Retry-After header, default back to the current iteration of the
              exponential backoff delay
        """
        gzip_data = BytesIO()
        with gzip.GzipFile(fileobj=gzip_data, mode="w") as gzip_stream:
            gzip_stream.write(serialized_data)
        
        data = gzip_data.getvalue()

        def send():
            try:
                return self._session.post(
                    url=self._endpoint,
                    headers=self._LARGE_LOG_HEADER if self._gen_ai_flag else None,
                    data=data,
                    verify=self._certificate_file,
                    timeout=self._timeout,
                    cert=self._client_cert,
                )
            except ConnectionError:
                return self._session.post(
                    url=self._endpoint,
                    headers=self._LARGE_LOG_HEADER if self._gen_ai_flag else None,
                    data=data,
                    verify=self._certificate_file,
                    timeout=self._timeout,
                    cert=self._client_cert,
                )
                
        backoff = list(_create_exp_backoff_generator(self._MAX_RETRY_TIMEOUT))
        
        while True:
            resp = send()
            
            if not self._retryable(resp) or not backoff:
                return resp
                        
            retry_after = resp.headers.get(self._RETRY_AFTER_HEADER, None)
            delay = backoff.pop(0) if retry_after == None else self._parse_retryable_header(retry_after)

            _logger.warning(
                "Transient error %s encountered while exporting logs batch, retrying in %ss.",
                resp.reason,
                delay,
            )

            sleep(delay)           
            continue
            

    def _parse_retryable_header(self, retry_header: str) -> float:
        "Converts the given retryable header into a delay in seconds, returns -1 if there's an error with the parsing"      
        try:
            return float(retry_header)
        except ValueError:
            return -1.0

    def _get_size_of_log(self, log_data: LogData):
        # Rough estimate of the size of the LogData based on size of the content body + a buffer to account for other information in logs.
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
                return 4 #len(True) = 4
            return 5 #len(False) = 5
        
        if isinstance(val, int) or isinstance(val, float):
            return len(str(val))
        
        if isinstance(val, Sequence):
            for content in val:
                size += self._get_size_of_any_value(content)
        
        if isinstance(val, Mapping):
            for _, content in val.items():
                size += self._get_size_of_any_value(content)
        
        return size

    

            

