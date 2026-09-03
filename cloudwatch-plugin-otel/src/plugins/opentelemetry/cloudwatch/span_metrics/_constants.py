# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from importlib import import_module
from typing import Any, Final


def _semconv(module_path: str, name: str, literal: str) -> str:
    try:
        resolved: Any = import_module(module_path)
        for part in name.split("."):
            resolved = getattr(resolved, part)
        return resolved
    except (ImportError, AttributeError):
        return literal


_HTTP = "opentelemetry.semconv.attributes.http_attributes"
HTTP_REQUEST_METHOD = _semconv(_HTTP, "HTTP_REQUEST_METHOD", "http.request.method")
HTTP_RESPONSE_STATUS_CODE = _semconv(_HTTP, "HTTP_RESPONSE_STATUS_CODE", "http.response.status_code")
HTTP_ROUTE = _semconv(_HTTP, "HTTP_ROUTE", "http.route")

_HTTP_INCUBATING = "opentelemetry.semconv._incubating.attributes.http_attributes"
HTTP_METHOD = _semconv(_HTTP_INCUBATING, "HTTP_METHOD", "http.method")
HTTP_STATUS_CODE = _semconv(_HTTP_INCUBATING, "HTTP_STATUS_CODE", "http.status_code")

_ERROR = "opentelemetry.semconv.attributes.error_attributes"
ERROR_TYPE = _semconv(_ERROR, "ERROR_TYPE", "error.type")

_RPC_INCUBATING = "opentelemetry.semconv._incubating.attributes.rpc_attributes"
RPC_SYSTEM_NAME = _semconv(_RPC_INCUBATING, "RPC_SYSTEM_NAME", "rpc.system.name")
RPC_SYSTEM = _semconv(_RPC_INCUBATING, "RPC_SYSTEM", "rpc.system")
RPC_SERVICE = _semconv(_RPC_INCUBATING, "RPC_SERVICE", "rpc.service")
RPC_METHOD = _semconv(_RPC_INCUBATING, "RPC_METHOD", "rpc.method")

_DB_INCUBATING = "opentelemetry.semconv._incubating.attributes.db_attributes"
DB_SYSTEM_NAME = _semconv(_DB_INCUBATING, "DB_SYSTEM_NAME", "db.system.name")
DB_SYSTEM = _semconv(_DB_INCUBATING, "DB_SYSTEM", "db.system")
DB_OPERATION_NAME = _semconv(_DB_INCUBATING, "DB_OPERATION_NAME", "db.operation.name")
DB_OPERATION = _semconv(_DB_INCUBATING, "DB_OPERATION", "db.operation")
DB_COLLECTION_NAME = _semconv(_DB_INCUBATING, "DB_COLLECTION_NAME", "db.collection.name")
DB_SQL_TABLE = _semconv(_DB_INCUBATING, "DB_SQL_TABLE", "db.sql.table")
DB_MONGODB_COLLECTION = _semconv(_DB_INCUBATING, "DB_MONGODB_COLLECTION", "db.mongodb.collection")
DB_CASSANDRA_TABLE = _semconv(_DB_INCUBATING, "DB_CASSANDRA_TABLE", "db.cassandra.table")
DB_COSMOSDB_CONTAINER = _semconv(_DB_INCUBATING, "DB_COSMOSDB_CONTAINER", "db.cosmosdb.container")

_MESSAGING_INCUBATING = "opentelemetry.semconv._incubating.attributes.messaging_attributes"
MESSAGING_SYSTEM = _semconv(_MESSAGING_INCUBATING, "MESSAGING_SYSTEM", "messaging.system")
MESSAGING_OPERATION_NAME = _semconv(_MESSAGING_INCUBATING, "MESSAGING_OPERATION_NAME", "messaging.operation.name")
MESSAGING_DESTINATION_NAME = _semconv(_MESSAGING_INCUBATING, "MESSAGING_DESTINATION_NAME", "messaging.destination.name")
MESSAGING_DESTINATION = "messaging.destination"
MESSAGING_DESTINATION_TEMPORARY = _semconv(
    _MESSAGING_INCUBATING, "MESSAGING_DESTINATION_TEMPORARY", "messaging.destination.temporary"
)
MESSAGING_DESTINATION_ANONYMOUS = _semconv(
    _MESSAGING_INCUBATING, "MESSAGING_DESTINATION_ANONYMOUS", "messaging.destination.anonymous"
)


class _SpanMetrics:
    SCOPE_NAME: Final = "cloudwatch.plugin.otel.span_metrics"

    CALLS_NAME: Final = "traces.span.metrics.calls"
    # {call} is the UCUM annotation for counted things, matching OTel semconv counter conventions
    # (cf. {request}, {operation}). The collector spanmetrics connector leaves the unit unset; the
    # annotation is preferred because it states what is being counted.
    CALLS_UNIT: Final = "{call}"
    DURATION_NAME: Final = "traces.span.metrics.duration"
    DURATION_UNIT: Final = "s"
    DURATION_BUCKET_BOUNDARIES: Final = [
        0.002,
        0.004,
        0.006,
        0.008,
        0.01,
        0.05,
        0.1,
        0.2,
        0.4,
        0.8,
        1.0,
        1.4,
        2.0,
        5.0,
        10.0,
        15.0,
    ]
    NANOS_PER_SECOND: Final = 1_000_000_000.0

    SPAN_NAME: Final = "span.name"
    SPAN_KIND: Final = "span.kind"
    STATUS_CODE: Final = "status.code"
    SCHEMA: Final = "aws.otel.span.metrics.schema"
    SCHEMA_VERSION: Final = "v1"
    LIB_VERSION: Final = "aws.otel.extension.lib.version"
