# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Optional, Sequence

from botocore.session import Session

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.llo_handler import LLOHandler
from opentelemetry._logs import get_logger_provider
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

_logger = logging.getLogger(__name__)


class OTLPAwsSpanExporter(OTLPSpanExporter):
    """
    This exporter extends the functionality of the OTLPSpanExporter to allow spans to be exported
    to the XRay OTLP endpoint https://xray.[AWSRegion].amazonaws.com/v1/traces. Utilizes the
    AwsAuthSession to sign and directly inject SigV4 Authentication to the exported request's headers.

    See: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    def __init__(
        self,
        aws_region: str,
        session: Session,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
        logger_provider: Optional[LoggerProvider] = None,
    ):
        self._aws_region = aws_region
        self._logger_provider = logger_provider
        self._llo_handler = None

        OTLPSpanExporter.__init__(
            self,
            endpoint,
            certificate_file,
            client_key_file,
            client_certificate_file,
            headers,
            timeout,
            compression,
            session=AwsAuthSession(session=session, aws_region=self._aws_region, service="xray"),
        )

    def _ensure_llo_handler(self):
        """Lazily initialize LLO handler when needed to avoid initialization order issues"""
        if self._llo_handler is None and is_agent_observability_enabled():
            if self._logger_provider is None:
                try:
                    self._logger_provider = get_logger_provider()
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _logger.debug("Failed to get logger provider: %s", exc)
                    return False

            if self._logger_provider:
                self._llo_handler = LLOHandler(self._logger_provider)
                return True

        return self._llo_handler is not None

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            if is_agent_observability_enabled() and self._ensure_llo_handler():
                llo_processed_spans = self._llo_handler.process_spans(spans)
                return super().export(llo_processed_spans)
        except Exception:  # pylint: disable=broad-exception-caught
            return SpanExportResult.FAILURE

        return super().export(spans)
