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

# Messaging (https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
_MESSAGING_INCUBATING = "opentelemetry.semconv._incubating.attributes.messaging_attributes"
MESSAGING_SYSTEM = _semconv(_MESSAGING_INCUBATING, "MESSAGING_SYSTEM", "messaging.system")
MESSAGING_OPERATION_NAME = _semconv(_MESSAGING_INCUBATING, "MESSAGING_OPERATION_NAME", "messaging.operation.name")
MESSAGING_OPERATION_TYPE = _semconv(_MESSAGING_INCUBATING, "MESSAGING_OPERATION_TYPE", "messaging.operation.type")
MESSAGING_CONSUMER_GROUP_NAME = _semconv(
    _MESSAGING_INCUBATING, "MESSAGING_CONSUMER_GROUP_NAME", "messaging.consumer.group.name"
)
MESSAGING_DESTINATION_NAME = _semconv(_MESSAGING_INCUBATING, "MESSAGING_DESTINATION_NAME", "messaging.destination.name")
MESSAGING_DESTINATION = "messaging.destination"
MESSAGING_DESTINATION_TEMPORARY = _semconv(
    _MESSAGING_INCUBATING, "MESSAGING_DESTINATION_TEMPORARY", "messaging.destination.temporary"
)
MESSAGING_DESTINATION_ANONYMOUS = _semconv(
    _MESSAGING_INCUBATING, "MESSAGING_DESTINATION_ANONYMOUS", "messaging.destination.anonymous"
)

# Peer (https://opentelemetry.io/docs/specs/semconv/registry/attributes/server/)
_SERVER = "opentelemetry.semconv.attributes.server_attributes"
SERVER_ADDRESS = _semconv(_SERVER, "SERVER_ADDRESS", "server.address")
SERVER_PORT = _semconv(_SERVER, "SERVER_PORT", "server.port")
_NETWORK_INCUBATING = "opentelemetry.semconv._incubating.attributes.network_attributes"
NET_PEER_NAME = _semconv(_NETWORK_INCUBATING, "NET_PEER_NAME", "net.peer.name")
NET_HOST_NAME = _semconv(_NETWORK_INCUBATING, "NET_HOST_NAME", "net.host.name")
NET_PEER_PORT = _semconv(_NETWORK_INCUBATING, "NET_PEER_PORT", "net.peer.port")
NET_HOST_PORT = _semconv(_NETWORK_INCUBATING, "NET_HOST_PORT", "net.host.port")

# GenAI (https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/)
_GEN_AI_INCUBATING = "opentelemetry.semconv._incubating.attributes.gen_ai_attributes"
GEN_AI_REQUEST_MODEL = _semconv(_GEN_AI_INCUBATING, "GEN_AI_REQUEST_MODEL", "gen_ai.request.model")
GEN_AI_PROVIDER_NAME = _semconv(_GEN_AI_INCUBATING, "GEN_AI_PROVIDER_NAME", "gen_ai.provider.name")
GEN_AI_OPERATION_NAME = _semconv(_GEN_AI_INCUBATING, "GEN_AI_OPERATION_NAME", "gen_ai.operation.name")

# AWS resource identity (https://opentelemetry.io/docs/specs/semconv/registry/attributes/aws/)
_AWS_INCUBATING = "opentelemetry.semconv._incubating.attributes.aws_attributes"
AWS_S3_BUCKET = _semconv(_AWS_INCUBATING, "AWS_S3_BUCKET", "aws.s3.bucket")
AWS_DYNAMODB_TABLE_NAMES = _semconv(_AWS_INCUBATING, "AWS_DYNAMODB_TABLE_NAMES", "aws.dynamodb.table_names")
AWS_LAMBDA_INVOKED_ARN = _semconv(_AWS_INCUBATING, "AWS_LAMBDA_INVOKED_ARN", "aws.lambda.invoked_arn")
AWS_SNS_TOPIC_ARN = _semconv(_AWS_INCUBATING, "AWS_SNS_TOPIC_ARN", "aws.sns.topic.arn")
AWS_SQS_QUEUE_URL = _semconv(_AWS_INCUBATING, "AWS_SQS_QUEUE_URL", "aws.sqs.queue.url")

# FaaS (https://opentelemetry.io/docs/specs/semconv/registry/attributes/faas/)
_FAAS_INCUBATING = "opentelemetry.semconv._incubating.attributes.faas_attributes"
FAAS_INVOKED_NAME = _semconv(_FAAS_INCUBATING, "FAAS_INVOKED_NAME", "faas.invoked_name")
FAAS_INVOKED_PROVIDER = _semconv(_FAAS_INCUBATING, "FAAS_INVOKED_PROVIDER", "faas.invoked_provider")
FAAS_INVOKED_REGION = _semconv(_FAAS_INCUBATING, "FAAS_INVOKED_REGION", "faas.invoked_region")
FAAS_TRIGGER = _semconv(_FAAS_INCUBATING, "FAAS_TRIGGER", "faas.trigger")


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
