# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Dict, Optional

from plugins.opentelemetry.cloudwatch.connector._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.version import __version__
from typing_extensions import override

from opentelemetry.context import Context
from opentelemetry.metrics import Meter, MeterProvider, get_meter
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CASSANDRA_TABLE,
    DB_COLLECTION_NAME,
    DB_COSMOSDB_CONTAINER,
    DB_MONGODB_COLLECTION,
    DB_OPERATION,
    DB_OPERATION_NAME,
    DB_SQL_TABLE,
    DB_SYSTEM,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv._incubating.attributes.http_attributes import HTTP_METHOD, HTTP_STATUS_CODE
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_ANONYMOUS,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_TEMPORARY,
    MESSAGING_OPERATION,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_METHOD, RPC_SERVICE, RPC_SYSTEM
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.http_attributes import (
    HTTP_REQUEST_METHOD,
    HTTP_RESPONSE_STATUS_CODE,
    HTTP_ROUTE,
)
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

try:
    from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_SYSTEM_NAME
except ImportError:
    RPC_SYSTEM_NAME = "rpc.system.name"

_logger = logging.getLogger(__name__)


class SpanMetricsConnector(SpanProcessor):
    def __init__(self, meter_provider: Optional[MeterProvider] = None) -> None:
        self._lib_version = __version__
        self.enabled = True
        meter: Meter = get_meter(_SpanMetrics.SCOPE_NAME, __version__, meter_provider)
        self._calls_counter = meter.create_counter(_SpanMetrics.CALLS_NAME)
        self._duration_histogram = meter.create_histogram(
            _SpanMetrics.DURATION_NAME,
            unit=_SpanMetrics.DURATION_UNIT,
            explicit_bucket_boundaries_advisory=_SpanMetrics.DURATION_BUCKET_BOUNDARIES,
        )

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not self.enabled:
            return
        try:
            span.set_attribute(_SpanMetrics.SCHEMA, _SpanMetrics.SCHEMA_VERSION)
            span.set_attribute(_SpanMetrics.LIB_VERSION, self._lib_version)
        # pylint: disable=broad-exception-caught
        except Exception:
            _logger.debug("Failed to stamp span metrics attributes", exc_info=True)

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if not self.enabled:
            return
        try:
            attributes = self._build_metric_attributes(span)
            duration_seconds = (span.end_time - span.start_time) / _SpanMetrics.NANOS_PER_SECOND
            self._calls_counter.add(1, attributes)
            self._duration_histogram.record(duration_seconds, attributes)
        # pylint: disable=broad-exception-caught
        except Exception:
            _logger.debug("Failed to record span metrics", exc_info=True)

    @override
    def shutdown(self) -> None:
        self.force_flush()
        self.enabled = False

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def _build_metric_attributes(self, span: ReadableSpan) -> Dict[str, Any]:
        span_attributes = span.attributes or {}

        status_code = _SpanMetrics.DEFAULT_STATUS_CODE
        if span.status is not None and span.status.status_code is not None:
            status_code = span.status.status_code.name

        attributes: Dict[str, Any] = {
            _SpanMetrics.SPAN_NAME: span.name,
            _SpanMetrics.SPAN_KIND: span.kind.name if span.kind is not None else _SpanMetrics.DEFAULT_SPAN_KIND,
            _SpanMetrics.STATUS_CODE: status_code,
            _SpanMetrics.SCHEMA: _SpanMetrics.SCHEMA_VERSION,
            _SpanMetrics.LIB_VERSION: self._lib_version,
        }

        if span.resource is not None:
            service_name = span.resource.attributes.get(SERVICE_NAME)
            if service_name is not None:
                attributes[SERVICE_NAME] = service_name

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
        self._copy(attributes, span_attributes, MESSAGING_OPERATION_TYPE, MESSAGING_OPERATION)

        if (
            span_attributes.get(MESSAGING_DESTINATION_TEMPORARY) is not True
            and span_attributes.get(MESSAGING_DESTINATION_ANONYMOUS) is not True
        ):
            self._copy(
                attributes,
                span_attributes,
                MESSAGING_DESTINATION_NAME,
                _SpanMetrics.MESSAGING_DESTINATION,
            )

        return attributes

    @staticmethod
    def _copy(attributes: Dict[str, Any], span_attributes, canonical_key: str, *legacy_keys: str) -> None:
        for source_key in (canonical_key, *legacy_keys):
            if source_key in span_attributes:
                attributes[canonical_key] = span_attributes[source_key]
                return
