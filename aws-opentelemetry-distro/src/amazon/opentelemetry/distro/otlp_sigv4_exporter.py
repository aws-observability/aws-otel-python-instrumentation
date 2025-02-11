# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import re
from typing import Dict, Optional

import requests
from botocore import session
from botocore.auth import NoCredentialsError, SigV4Auth
from botocore.awsrequest import AWSRequest
from grpc import Compression

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

AWS_SERVICE = "xray"

_logger = logging.getLogger(__name__)


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

        self._aws_region = self._validate_exporter_endpoint(endpoint)
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
            request = AWSRequest(
                method="POST",
                url=self._endpoint,
                data=serialized_data,
                headers={"Content-Type": "application/x-protobuf"},
            )

            botocore_session = session.Session()
            credentials = botocore_session.get_credentials()
            if credentials is not None:
                signer = SigV4Auth(credentials, AWS_SERVICE, self._aws_region)

                try:
                    signer.add_auth(request)
                    self._session.headers.update(dict(request.headers))

                except NoCredentialsError as signing_error:
                    _logger.error(f"Failed to sign request: {signing_error}")

            else:
                _logger.error("Failed to get credentials to export span to OTLP CloudWatch endpoint")

        return super()._export(serialized_data)

    @staticmethod
    def _validate_exporter_endpoint(endpoint: str) -> Optional[str]:
        if not endpoint:
            return None

        match = re.search(rf"{AWS_SERVICE}\.([a-z0-9-]+)\.amazonaws\.com", endpoint)

        if match:
            region = match.group(1)
            xray_regions = session.Session().get_available_regions(AWS_SERVICE)
            if region in xray_regions:
                return region

            _logger.error(
                "Invalid AWS region: %s. Valid regions are %s. Resolving to default endpoint.", region, xray_regions
            )
            return None

        _logger.error(
            "Invalid XRay traces endpoint: %s. Resolving to default endpoint. "
            "The traces endpoint follows the pattern https://xray.[AWSRegion].amazonaws.com/v1/traces. "
            "For example, for the US West (Oregon) (us-west-2) Region, the endpoint will be "
            "https://xray.us-west-2.amazonaws.com/v1/traces.",
            endpoint,
        )

        return None
