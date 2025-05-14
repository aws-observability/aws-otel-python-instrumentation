# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional, Sequence

from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.llo_handler import LLOHandler
from amazon.opentelemetry.distro._utils import is_agent_observability_enabled
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

AGENT_OBSERVABILITY_ENABLED = "AGENT_OBSERVABILITY_ENABLED"


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
        logger_provider: Optional[LoggerProvider] = None
    ):
        self._aws_region = None

        if logger_provider:
            self._llo_handler = LLOHandler(logger_provider)

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

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if is_agent_observability_enabled():
            llo_processed_spans = self._llo_handler.process_spans(spans)
            return super().export(llo_processed_spans)

        return super().export(spans)
