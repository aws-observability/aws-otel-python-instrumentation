# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Dict, Optional

import requests
from grpc import Compression

from amazon.opentelemetry.distro._utils import is_otlp_endpoint_cloudwatch
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

AWS_SERVICE = "xray"
_logger = logging.getLogger(__name__)

"""The OTLPAwsSigV4Exporter extends the functionality of the OTLPSpanExporter to allow SigV4 authentication if the
   configured traces endpoint is a CloudWatch OTLP endpoint https://xray.[AWSRegion].amazonaws.com/v1/traces"""


class OTLPAwsSigV4Exporter(OTLPSpanExporter):

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

        if endpoint and is_otlp_endpoint_cloudwatch(endpoint):
            try:
                # Defensive check to verify that the application being auto instrumented has
                # botocore installed.

                from botocore import auth, awsrequest, session

                self.boto_auth = auth
                self.boto_aws_request = awsrequest
                self.boto_session = session.Session()
                self._aws_region = self._validate_exporter_endpoint(endpoint)

            except ImportError:
                _logger.error(
                    "botocore is required to export traces to %s. " "Please install it using `pip install botocore`",
                    endpoint,
                )

        else:
            _logger.error(
                "Invalid XRay traces endpoint: %s. Resolving to OTLPSpanExporter to handle exporting. "
                "The traces endpoint follows the pattern https://xray.[AWSRegion].amazonaws.com/v1/traces. "
                "For example, for the US West (Oregon) (us-west-2) Region, the endpoint will be "
                "https://xray.us-west-2.amazonaws.com/v1/traces.",
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

    def _validate_exporter_endpoint(self, endpoint: str) -> Optional[str]:
        if not endpoint:
            return None

        region = endpoint.split(".")[1]
        xray_regions = self.boto_session.get_available_regions(AWS_SERVICE)

        if region not in xray_regions:

            _logger.error(
                "Invalid AWS region: %s. Valid regions are %s. Resolving to default endpoint.", region, xray_regions
            )

            return None

        return region
