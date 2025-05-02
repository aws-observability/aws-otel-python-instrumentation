import logging
import os
from typing import Dict, Optional, Sequence

import requests

from amazon.opentelemetry.distro._utils import is_installed
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LogRecord
from opentelemetry.sdk._logs.export import LogExportResult

# For CloudWatch Logs, the service name is 'logs' not 'xray'
AWS_SERVICE = "logs"
AWS_CLOUDWATCH_LOG_GROUP_ENV = "AWS_CLOUDWATCH_LOG_GROUP"
AWS_CLOUDWATCH_LOG_STREAM_ENV = "AWS_CLOUDWATCH_LOG_STREAM"
# Set up more verbose logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
# Add a console handler if not already present
if not _logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    _logger.addHandler(console_handler)

class OTLPAwsLogExporter(OTLPLogExporter):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
        rsession: Optional[requests.Session] = None,
    ):
        self._aws_region = None
        self._has_required_dependencies = False

        if endpoint and is_installed("botocore"):
            # pylint: disable=import-outside-toplevel
            from botocore import auth, awsrequest, session
            self.boto_auth = auth
            self.boto_aws_request = awsrequest
            self.boto_session = session.Session()

            # For logs endpoint https://logs.[region].amazonaws.com/v1/logs
            self._aws_region = endpoint.split(".")[1]
            self._has_required_dependencies = True
        else:
            _logger.error(
                "botocore is required to export logs to %s. Please install it using `pip install botocore`",
                endpoint,
            )

        super().__init__(
            endpoint=endpoint,
            certificate_file=certificate_file,
            client_key_file=client_key_file,
            client_certificate_file=client_certificate_file,
            headers=headers,
            timeout=timeout,
            compression=compression,
            session=rsession,
        )

    def _export(self, serialized_data: bytes):
        try:
            if self._has_required_dependencies:
                request = self.boto_aws_request.AWSRequest(
                    method="POST",
                    url=self._endpoint,
                    data=serialized_data,
                    headers={"Content-Type": "application/x-protobuf"},
                )

                credentials = self.boto_session.get_credentials()

                if credentials is not None:
                    signer = self.boto_auth.SigV4Auth(credentials, AWS_SERVICE, self._aws_region)

                    try:
                        signer.add_auth(request)
                        self._session.headers.update(dict(request.headers))
                    except Exception as signing_error: # pylint: disable=broad-except
                        _logger.error(f"Failed to sign request: {signing_error}")
                else:
                    _logger.error("Failed to obtain AWS credentials for SigV4 signing")
            else:
                _logger.warning(f"SigV4 authentication not available for {self._endpoint}. Falling back to unsigned request.")

            result = super()._export(serialized_data)
            return result
        except Exception as e:
            _logger.exception(f"Exception in _export: {str(e)}")
            # Still try to call the parent method in case it can handle the error
            return super()._export(serialized_data)
