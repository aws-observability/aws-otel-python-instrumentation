# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in OTLP trace delivery independent of the primary exporter configuration."""

import copy
import logging
import math
import os
from typing import Mapping
from urllib.parse import urlsplit

from requests import Session
from requests.auth import AuthBase
from requests.exceptions import InvalidHeader
from requests.utils import check_header_validity

from amazon.opentelemetry.distro.attribute_redacting_span_processor import AttributeRedactingSpanProcessor
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.llo_handler import LLOHandler
from opentelemetry._logs import get_logger_provider
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, SynchronousMultiSpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.util.re import _LIBERAL_HEADER_PATTERN, parse_env_headers

OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT = "OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT"
OTEL_AWS_ADDITIONAL_TRACES_HEADERS = "OTEL_AWS_ADDITIONAL_TRACES_HEADERS"
OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT = "OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT"
OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION = "OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION"
OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE = "OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE"
OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED = "OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED"
OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED = "OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED"

_logger = logging.getLogger(__name__)
_RESERVED_HEADERS = {"content-type", "content-encoding", "content-length", "host", "transfer-encoding"}


class _NoAuthentication(AuthBase):
    # pylint: disable=no-self-use
    def __call__(self, request):
        return request


class _AdditionalSession(Session):
    def __init__(self, headers: Mapping[str, str], compression: Compression):
        super().__init__()
        self.auth = _NoAuthentication()
        self._additional_headers = headers
        self._compression = compression

    def request(self, method, url, *args, headers=None, **kwargs):
        # The upstream exporter merges primary environment headers even when
        # explicit headers are supplied. Do not forward those credentials.
        additional_headers = {"Content-Type": "application/x-protobuf", **self._additional_headers}
        if self._compression == Compression.Gzip:
            additional_headers["Content-Encoding"] = "gzip"
        return super().request(method, url, *args, headers=additional_headers, **kwargs)


class _AdditionalSpanProcessor(SpanProcessor):
    def __init__(self, processors: SynchronousMultiSpanProcessor, redact: bool, filter_content: bool):
        self._processors = processors
        self._redactor = AttributeRedactingSpanProcessor() if redact else None
        self._content_filter = LLOHandler(get_logger_provider()) if filter_content else None

    def on_start(self, span: Span, parent_context: Context = None) -> None:
        self._processors.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        # Snapshot before primary redaction/export can mutate shared spans.
        additional_span = copy.copy(span)
        additional_span._attributes = copy.deepcopy(span._attributes)
        additional_span._events = copy.deepcopy(span._events)
        additional_span._links = copy.deepcopy(span._links)
        if self._redactor is not None:
            self._redactor.on_end(additional_span)
        if self._content_filter is not None:
            additional_span = self._content_filter.filter_span(additional_span)
        self._processors.on_end(additional_span)

    def shutdown(self) -> None:
        self._processors.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._processors.force_flush(timeout_millis)


def _parse_flag(name: str) -> bool:
    value = os.environ.get(name, "false").strip().lower()
    if value not in {"true", "false"}:
        raise ValueError("Invalid additional trace flag")
    return value == "true"


def _parse_additional_headers(value: str) -> Mapping[str, str]:
    # Validate before the shared parser, which logs malformed header values.
    if any(item.strip() and not _LIBERAL_HEADER_PATTERN.fullmatch(item.strip()) for item in value.split(",")):
        raise ValueError("Invalid additional trace headers")
    headers = dict(parse_env_headers(value, liberal=True))
    for name, header_value in headers.items():
        check_header_validity((name, header_value))
        if name in _RESERVED_HEADERS or any(ord(char) < 32 or ord(char) == 127 for char in name + header_value):
            raise ValueError("Invalid additional trace headers")
        name.encode("ascii")
        header_value.encode("latin-1")
    return headers


def add_additional_span_processors(provider: TracerProvider, include_unsampled: bool = False) -> None:
    """Register before primary redaction, after any span-kind adjustment."""
    endpoint = os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT)
    if endpoint is None:
        return

    try:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.fragment
            or any(char.isspace() for char in endpoint)
        ):
            raise ValueError("Invalid additional trace endpoint")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Invalid additional trace endpoint")
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("Invalid additional trace port")
        headers = _parse_additional_headers(os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_HEADERS, ""))
        timeout = float(os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT, "10000")) / 1000
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Invalid additional trace timeout")
        compression = {
            "none": Compression.NoCompression,
            "gzip": Compression.Gzip,
        }.get(os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION, "none").lower())
        if compression is None:
            raise ValueError("Invalid additional trace compression")
        redact = _parse_flag(OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED)
        filter_content = _parse_flag(OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED)
    except (ValueError, InvalidHeader):
        _logger.warning("Invalid additional OTLP trace configuration; additional export is disabled.")
        return

    processor_types = [BatchSpanProcessor]
    if include_unsampled:
        processor_types.append(BatchUnsampledSpanProcessor)

    processors = SynchronousMultiSpanProcessor()
    for processor_type in processor_types:
        session = _AdditionalSession(headers, compression)
        exporter = OTLPSpanExporter(
            endpoint=endpoint, headers=headers, timeout=timeout, compression=compression, session=session
        )
        # The upstream transport applies the primary OTLP TLS environment to
        # supplied sessions. Keep this destination's trust and identity separate.
        session.verify = os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE) or True
        session.cert = None
        processors.add_span_processor(processor_type(exporter))
    provider.add_span_processor(_AdditionalSpanProcessor(processors, redact, filter_content))
