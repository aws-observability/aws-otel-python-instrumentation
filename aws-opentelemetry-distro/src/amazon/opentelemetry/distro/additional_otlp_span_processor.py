# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in OTLP trace delivery independent of the primary exporter configuration."""

import copy
import logging
import math
import os
import threading
import time
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Mapping, NamedTuple, Optional
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


class _AdditionalSpanSnapshotProcessor(SpanProcessor):
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


class _AdditionalExportLifecycle(SpanProcessor):
    """Register after primary processors so optional flushes cannot starve them."""

    def __init__(self, processors: SynchronousMultiSpanProcessor):
        self._processors = processors
        self._shutdown = False
        self._flush_future: Optional[Future] = None
        self._reset_flush_state()
        if hasattr(os, "register_at_fork"):
            weak_reset = weakref.WeakMethod(self._reset_flush_state)

            def after_fork():
                reset = weak_reset()
                if reset is not None:
                    reset()

            os.register_at_fork(after_in_child=after_fork)

    def _reset_flush_state(self) -> None:
        self._flush_lock = threading.Lock()
        self._flush_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="AdditionalOtlpFlush")
        self._flush_future = None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        deadline = time.monotonic() + max(timeout_millis, 0) / 1000
        while time.monotonic() < deadline:
            with self._flush_lock:
                if self._shutdown:
                    return False
                future = self._flush_future
                owns_flush = future is None
                if future is None:
                    # The pinned BatchSpanProcessor ignores its flush timeout.
                    future = self._flush_executor.submit(
                        self._processors.force_flush, max(0, int((deadline - time.monotonic()) * 1000))
                    )
                    self._flush_future = future
            try:
                flushed = future.result(timeout=max(0, deadline - time.monotonic()))
            except FutureTimeoutError:
                _logger.warning("Additional OTLP trace flush timed out.")
                return False
            finally:
                with self._flush_lock:
                    if self._flush_future is future and future.done():
                        self._flush_future = None
            if owns_flush or not flushed:
                return flushed
            # An older timed-out flush may not cover spans queued for this call.
            # Finish it before starting another, keeping at most one flush task.
        return False

    def shutdown(self) -> None:
        with self._flush_lock:
            if self._shutdown:
                return
            self._shutdown = True
        try:
            self._processors.shutdown()
        finally:
            self._flush_executor.shutdown(wait=True)


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


class _AdditionalTraceConfiguration(NamedTuple):
    endpoint: str
    headers: Mapping[str, str]
    timeout: float
    compression: Compression
    redact: bool
    filter_content: bool


def _read_additional_configuration() -> Optional[_AdditionalTraceConfiguration]:
    endpoint = os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT)
    if endpoint is None:
        return None

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
        return _AdditionalTraceConfiguration(endpoint, headers, timeout, compression, redact, filter_content)
    except (ValueError, InvalidHeader):
        _logger.warning("Invalid additional OTLP trace configuration; additional export is disabled.")
        return None


def add_additional_span_processors(
    provider: TracerProvider, include_unsampled: bool = False, max_export_batch_size: Optional[int] = None
) -> Optional[SpanProcessor]:
    """Register snapshot capture and return lifecycle to register after primary."""
    config = _read_additional_configuration()
    if config is None:
        return None
    processor_types = [BatchSpanProcessor]
    if include_unsampled:
        processor_types.append(BatchUnsampledSpanProcessor)

    processors = SynchronousMultiSpanProcessor()
    session = None
    exporter = None
    lifecycle = None
    registered = False
    try:
        for processor_type in processor_types:
            session = _AdditionalSession(config.headers, config.compression)
            exporter = OTLPSpanExporter(
                endpoint=config.endpoint,
                headers=config.headers,
                timeout=config.timeout,
                compression=config.compression,
                session=session,
            )
            # The upstream transport applies the primary OTLP TLS environment to
            # supplied sessions. Keep this destination's trust and identity separate.
            session.verify = os.environ.get(OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE) or True
            session.cert = None
            processors.add_span_processor(processor_type(exporter, max_export_batch_size=max_export_batch_size))
            session = None
            exporter = None
        snapshot = _AdditionalSpanSnapshotProcessor(processors, config.redact, config.filter_content)
        lifecycle = _AdditionalExportLifecycle(processors)
        provider.add_span_processor(snapshot)
        registered = True
        return lifecycle
    except (ValueError, InvalidHeader):
        _logger.warning("Failed to initialize additional OTLP trace export; additional export is disabled.")
        return None
    finally:
        if not registered:
            try:
                if exporter is not None:
                    exporter.shutdown()
                elif session is not None:
                    session.close()
            finally:
                if lifecycle is not None:
                    lifecycle.shutdown()
                else:
                    processors.shutdown()
