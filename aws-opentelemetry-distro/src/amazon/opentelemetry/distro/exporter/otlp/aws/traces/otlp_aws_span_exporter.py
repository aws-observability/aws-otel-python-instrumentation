# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


class OTLPAwsSpanExporter(OTLPSpanExporter):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
    ):
        self._aws_region = None

        if endpoint:
            self._aws_region = endpoint.split(".")[1]

        OTLPSpanExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression,
            session=AwsAuthSession(aws_region=self._aws_region, service="xray"),
        )
