# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Optional

import requests

from amazon.opentelemetry.distro.exporter.otlp.aws.common.otlp_aws_exporter import OTLPBaseAwsExporter
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


class OTLPAwsSpanExporter(OTLPSpanExporter, OTLPBaseAwsExporter):
    """
    This exporter extends the functionality of the OTLPSpanExporter to allow spans to be exported to the
    XRay Traces OTLP endpoint https://xray.[AWSRegion].amazonaws.com/v1/traces. Utilizes the botocore
    library to sign and directly inject SigV4 Authentication to the exported request's headers.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    # pylint: disable=too-many-arguments
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

        OTLPBaseAwsExporter.__init__(self, endpoint, rsession)
        OTLPSpanExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression,
            rsession,
        )

    # pylint: disable=no-self-use
    def get_service(self):
        return "xray"

    # Overrides upstream's private implementation of _export. All behaviors are
    # the same except if the endpoint is an XRay OTLP endpoint, we will sign the request
    # with SigV4 in headers before sending it to the endpoint.
    def _export(self, serialized_data: bytes):
        self.inject_sigv4_auth(serialized_data)
        return OTLPSpanExporter._export(self, serialized_data)
