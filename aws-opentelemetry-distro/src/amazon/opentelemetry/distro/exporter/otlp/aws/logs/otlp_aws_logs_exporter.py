# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

import requests
from opentelemetry.exporter.otlp.proto.http import Compression

from amazon.opentelemetry.distro.exporter.otlp.aws.common.otlp_aws_exporter import OTLPBaseAwsExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter


class OTLPAwsLogExporter(OTLPLogExporter, OTLPBaseAwsExporter):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
        session: Optional[requests.Session] = None,
    ):
        OTLPBaseAwsExporter.__init__(self, endpoint, session)
        OTLPLogExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression,
            session,
        )

    def get_service(self):
        return "logs"

    def _export(self, serialized_data: bytes):
        self.sigv4_auth(serialized_data)
        return OTLPLogExporter._export(self, serialized_data)
