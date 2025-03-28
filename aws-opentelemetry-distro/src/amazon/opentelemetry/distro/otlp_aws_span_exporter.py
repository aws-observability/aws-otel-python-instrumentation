# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Dict, Optional

import requests

from amazon.opentelemetry.distro._utils import is_installed
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

        self._aws_region = None
        self._has_required_dependencies = False
        # Requires botocore to be installed to sign the headers. However,
        # some users might not need to use this exporter. In order not conflict
        # with existing behavior, we check for botocore before initializing this exporter.

        if endpoint and is_installed("botocore"):
            # pylint: disable=import-outside-toplevel
            from botocore import auth, awsrequest, session

            self.boto_auth = auth
            self.boto_aws_request = awsrequest
            self.boto_session = session.Session()

            # Assumes only valid endpoints passed are of XRay OTLP format.
            # The only usecase for this class would be for ADOT Python Auto Instrumentation and that already validates
            # the endpoint to be an XRay OTLP endpoint.
            self._aws_region = endpoint.split(".")[1]
            self._has_required_dependencies = True

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

    # Overrides upstream's private implementation of _export. All behaviors are
    # the same except if the endpoint is an XRay OTLP endpoint, we will sign the request
    # with SigV4 in headers before sending it to the endpoint. Otherwise, we will skip signing.
    def _export(self, serialized_data: bytes):
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

                except Exception as signing_error:  # pylint: disable=broad-except
                    _logger.error("Failed to sign request: %s", signing_error)
        else:
            _logger.error("SigV4 authentication headers not injected to export spans to %s endpoint.", self._endpoint)

        return super()._export(serialized_data)
