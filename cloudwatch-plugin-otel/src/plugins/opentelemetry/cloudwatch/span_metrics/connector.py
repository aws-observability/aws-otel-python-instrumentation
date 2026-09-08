# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import os
from typing import Any, Dict, Optional

from plugins.opentelemetry.cloudwatch.span_metrics._constants import (
    DB_CASSANDRA_TABLE,
    DB_COLLECTION_NAME,
    DB_COSMOSDB_CONTAINER,
    DB_MONGODB_COLLECTION,
    DB_OPERATION,
    DB_OPERATION_NAME,
    DB_SQL_TABLE,
    DB_SYSTEM,
    DB_SYSTEM_NAME,
    ERROR_TYPE,
    HTTP_METHOD,
    HTTP_REQUEST_METHOD,
    HTTP_RESPONSE_STATUS_CODE,
    HTTP_ROUTE,
    HTTP_STATUS_CODE,
    MESSAGING_DESTINATION,
    MESSAGING_DESTINATION_ANONYMOUS,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_TEMPORARY,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    RPC_METHOD,
    RPC_SERVICE,
    RPC_SYSTEM,
    RPC_SYSTEM_NAME,
    _SpanMetrics,
)
from plugins.opentelemetry.cloudwatch.version import __version__
from typing_extensions import override

from opentelemetry.context import Context
from opentelemetry.environment_variables import OTEL_METRICS_EXPORTER
from opentelemetry.metrics import Meter, MeterProvider, NoOpCounter, NoOpHistogram, get_meter
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

_logger = logging.getLogger(__name__)


class SpanMetricsConnector(SpanProcessor):
    """Span metrics processor implementation.

    `SpanMetricsConnector` is an implementation of `SpanProcessor` that derives
    metrics from ended spans, dimensioned by `span.name`, `span.kind`, and
    `status.code` (plus copied low-cardinality HTTP, RPC, database, and
    messaging semantic-convention attributes); `service.name` is carried by
    the metric's resource, not duplicated on each datapoint:

    - `traces.span.metrics.calls`: a counter incremented once per span.
    - `traces.span.metrics.duration`: a histogram of span durations, in seconds.

    Args:
        meter_provider: The `MeterProvider` used to obtain the meter. Falls back to
            the global `MeterProvider` when omitted.
    """

    def __init__(self, meter_provider: Optional[MeterProvider] = None) -> None:
        self.enabled = True
        meter: Meter = get_meter(_SpanMetrics.SCOPE_NAME, __version__, meter_provider)
        self._calls_counter = meter.create_counter(_SpanMetrics.CALLS_NAME, unit=_SpanMetrics.CALLS_UNIT)
        self._duration_histogram = meter.create_histogram(
            _SpanMetrics.DURATION_NAME,
            unit=_SpanMetrics.DURATION_UNIT,
            explicit_bucket_boundaries_advisory=_SpanMetrics.DURATION_BUCKET_BOUNDARIES,
        )
        self.is_recording = not isinstance(self._calls_counter, NoOpCounter) or not isinstance(
            self._duration_histogram, NoOpHistogram
        )
        # OTLP is the default metrics exporter when OTEL_METRICS_EXPORTER is unset.
        configured_metrics_exporters = {
            exporter.strip().lower() for exporter in os.environ.get(OTEL_METRICS_EXPORTER, "otlp").split(",")
        }
        is_metrics_exporter_configured = "otlp" in configured_metrics_exporters
        self.is_metrics_active = self.is_recording and is_metrics_exporter_configured

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not self.enabled or not self.is_metrics_active:
            return
        try:
            span.set_attribute(_SpanMetrics.SCHEMA, _SpanMetrics.SCHEMA_VERSION)
            span.set_attribute(_SpanMetrics.LIB_VERSION, __version__)
        # pylint: disable=broad-exception-caught
        except Exception:
            _logger.debug("Failed to stamp span metrics attributes", exc_info=True)

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if not self.enabled or not self.is_recording:
            return
        try:
            attributes = self._build_metric_attributes(span)
            self._calls_counter.add(1, attributes)
            if span.end_time is not None and span.start_time is not None:
                duration_seconds = (span.end_time - span.start_time) / _SpanMetrics.NANOS_PER_SECOND
                self._duration_histogram.record(duration_seconds, attributes)
        # pylint: disable=broad-exception-caught
        except Exception:
            _logger.debug("Failed to record span metrics", exc_info=True)

    @override
    def shutdown(self) -> None:
        self.force_flush()
        self.enabled = False

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True

    def _build_metric_attributes(self, span: ReadableSpan) -> Dict[str, Any]:
        span_attributes = span.attributes or {}

        status_code = StatusCode.UNSET.name
        if span.status is not None and span.status.status_code is not None:
            status_code = span.status.status_code.name

        attributes: Dict[str, Any] = {
            _SpanMetrics.SPAN_NAME: span.name,
            _SpanMetrics.SPAN_KIND: span.kind.name if span.kind is not None else SpanKind.INTERNAL.name,
            _SpanMetrics.STATUS_CODE: status_code,
            _SpanMetrics.SCHEMA: _SpanMetrics.SCHEMA_VERSION,
            _SpanMetrics.LIB_VERSION: __version__,
        }

        # service.name is deliberately NOT a datapoint attribute: the metrics are recorded into the
        # host SDK's MeterProvider, whose resource already carries service.name, so duplicating it
        # on every datapoint would add a redundant dimension. Consumers read it from the metric
        # resource. (Intentional divergence from the collector spanmetrics connector, which flattens
        # it into datapoint attributes because collector-side consumers may drop the resource.)

        self._copy(attributes, span_attributes, HTTP_REQUEST_METHOD, HTTP_METHOD)
        self._copy(attributes, span_attributes, HTTP_RESPONSE_STATUS_CODE, HTTP_STATUS_CODE)
        self._copy(attributes, span_attributes, HTTP_ROUTE)
        self._copy(attributes, span_attributes, ERROR_TYPE)
        self._copy(attributes, span_attributes, RPC_SYSTEM_NAME, RPC_SYSTEM)
        self._copy(attributes, span_attributes, RPC_SERVICE)
        self._copy(attributes, span_attributes, RPC_METHOD)
        self._copy(attributes, span_attributes, DB_SYSTEM_NAME, DB_SYSTEM)
        self._copy(attributes, span_attributes, DB_OPERATION_NAME, DB_OPERATION)
        self._copy(
            attributes,
            span_attributes,
            DB_COLLECTION_NAME,
            DB_SQL_TABLE,
            DB_MONGODB_COLLECTION,
            DB_CASSANDRA_TABLE,
            DB_COSMOSDB_CONTAINER,
        )
        self._copy(attributes, span_attributes, MESSAGING_SYSTEM)
        self._copy(attributes, span_attributes, MESSAGING_OPERATION_NAME)

        if (
            span_attributes.get(MESSAGING_DESTINATION_TEMPORARY) is not True
            and span_attributes.get(MESSAGING_DESTINATION_ANONYMOUS) is not True
        ):
            self._copy(
                attributes,
                span_attributes,
                MESSAGING_DESTINATION_NAME,
                MESSAGING_DESTINATION,
            )

        return attributes

    @staticmethod
    def _copy(metric_attributes: Dict[str, Any], span_attributes, *keys: str) -> None:
        """Copies the first attribute found in the span to metrics."""
        for key in keys:
            if key in span_attributes:
                metric_attributes[key] = span_attributes[key]
                return
