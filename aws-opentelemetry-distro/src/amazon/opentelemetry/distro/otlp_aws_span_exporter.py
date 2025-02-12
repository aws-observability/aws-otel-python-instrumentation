# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Dict, Optional

import requests

from amazon.opentelemetry.distro._utils import is_installed, is_xray_otlp_endpoint
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

AWS_SERVICE = "xray"
_logger = logging.getLogger(__name__)


class OTLPAwsSpanExporter(OTLPSpanExporter):
    """
    This exporter extends the functionality of the OTLPSpanExporter to allow spans to be exported to the
    XRay OTLP endpoint https://xray.[AWSRegion].amazonaws.com/v1/traces. Utilizes the botocore
    library to sign and directly inject SigV4 Authentication to the exported request's headers.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

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

        # Represents the region of the CloudWatch OTLP endpoint to send the traces to.
        # If the endpoint has been verified to be valid, this should not be None

        self._aws_region = None

        if endpoint and is_xray_otlp_endpoint(endpoint):

            if is_installed("botocore"):
                # pylint: disable=import-outside-toplevel
                from botocore import auth, awsrequest, session

                self.boto_auth = auth
                self.boto_aws_request = awsrequest
                self.boto_session = session.Session()
                self._aws_region = endpoint.split(".")[1]

            else:
                _logger.error(
                    "botocore is required to export traces to %s. Please install it using `pip install botocore`",
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
        if self._aws_region:
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

                except self.boto_auth.NoCredentialsError as signing_error:
                    _logger.error("Failed to sign request: %s", signing_error)

            else:
                _logger.error("Failed to get credentials to export span to OTLP CloudWatch endpoint")

        return super()._export(serialized_data)
