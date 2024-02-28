# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
from logging import DEBUG, Logger, getLogger
from typing import Match, Optional
from urllib.parse import ParseResult, urlparse

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_QUEUE_NAME,
    AWS_QUEUE_URL,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_REMOTE_TARGET,
    AWS_SPAN_KIND,
    AWS_STREAM_NAME,
)
from amazon.opentelemetry.distro._aws_span_processing_util import (
    LOCAL_ROOT,
    MAX_KEYWORD_LENGTH,
    SQL_KEYWORD_PATTERN,
    UNKNOWN_OPERATION,
    UNKNOWN_REMOTE_OPERATION,
    UNKNOWN_REMOTE_SERVICE,
    UNKNOWN_SERVICE,
    extract_api_path_value,
    get_egress_operation,
    get_ingress_operation,
    is_aws_sdk_span,
    is_key_present,
    is_local_root,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from amazon.opentelemetry.distro.metric_attribute_generator import (
    DEPENDENCY_METRIC,
    SERVICE_METRIC,
    MetricAttributeGenerator,
)
from amazon.opentelemetry.distro.sqs_url_parser import SqsUrlParser
from opentelemetry.sdk.resources import Resource, ResourceAttributes
from opentelemetry.sdk.trace import BoundedAttributes, ReadableSpan
from opentelemetry.semconv.trace import SpanAttributes

# Pertinent OTEL attribute keys
_SERVICE_NAME: str = ResourceAttributes.SERVICE_NAME
_DB_OPERATION: str = SpanAttributes.DB_OPERATION
_DB_STATEMENT: str = SpanAttributes.DB_STATEMENT
_DB_SYSTEM: str = SpanAttributes.DB_SYSTEM
_FAAS_INVOKED_NAME: str = SpanAttributes.FAAS_INVOKED_NAME
_FAAS_TRIGGER: str = SpanAttributes.FAAS_TRIGGER
_GRAPHQL_OPERATION_TYPE: str = SpanAttributes.GRAPHQL_OPERATION_TYPE
_HTTP_METHOD: str = SpanAttributes.HTTP_METHOD
_HTTP_URL: str = SpanAttributes.HTTP_URL
_MESSAGING_OPERATION: str = SpanAttributes.MESSAGING_OPERATION
_MESSAGING_SYSTEM: str = SpanAttributes.MESSAGING_SYSTEM
_NET_PEER_NAME: str = SpanAttributes.NET_PEER_NAME
_NET_PEER_PORT: str = SpanAttributes.NET_PEER_PORT
_NET_SOCK_PEER_ADDR: str = SpanAttributes.NET_SOCK_PEER_ADDR
_NET_SOCK_PEER_PORT: str = SpanAttributes.NET_SOCK_PEER_PORT
_PEER_SERVICE: str = SpanAttributes.PEER_SERVICE
_RPC_METHOD: str = SpanAttributes.RPC_METHOD
_RPC_SERVICE: str = SpanAttributes.RPC_SERVICE
_AWS_TABLE_NAMES: str = SpanAttributes.AWS_DYNAMODB_TABLE_NAMES
_AWS_BUCKET_NAME: str = SpanAttributes.AWS_S3_BUCKET

# Special DEPENDENCY attribute value if GRAPHQL_OPERATION_TYPE attribute key is present.
_GRAPHQL: str = "graphql"

# As per https://opentelemetry.io/docs/specs/semconv/resource/#service, if service name is not specified, SDK defaults
# the service name to unknown_service:<process name> or just unknown_service.
_OTEL_UNKNOWN_SERVICE_PREFIX: str = "unknown_service"

_logger: Logger = getLogger(__name__)


class _AwsMetricAttributeGenerator(MetricAttributeGenerator):
    """AwsMetricAttributeGenerator generates specific metric attributes for incoming and outgoing traffic.

    AwsMetricAttributeGenerator generates very specific metric attributes based on low-cardinality span and resource
    attributes. If such attributes are not present, we fallback to default values.

    The goal of these particular metric attributes is to get metrics for incoming and outgoing traffic for a service.
    Namely, SpanKind#SERVER and SpanKind#CONSUMER spans represent "incoming" traffic, SpanKind#CLIENT and
    SpanKind#PRODUCER spans represent "outgoing" traffic, and SpanKind#INTERNAL spans are ignored.
    """

    @staticmethod
    def generate_metric_attributes_dict_from_span(span: ReadableSpan, resource: Resource) -> [str, BoundedAttributes]:
        """This method is used by the AwsSpanMetricsProcessor to generate service and dependency metrics"""
        attributes_dict: [str, BoundedAttributes] = {}
        if should_generate_service_metric_attributes(span):
            attributes_dict[SERVICE_METRIC] = _generate_service_metric_attributes(span, resource)
        if should_generate_dependency_metric_attributes(span):
            attributes_dict[DEPENDENCY_METRIC] = _generate_dependency_metric_attributes(span, resource)
        return attributes_dict


def _generate_service_metric_attributes(span: ReadableSpan, resource: Resource) -> BoundedAttributes:
    attributes: BoundedAttributes = BoundedAttributes(immutable=False)
    _set_service(resource, span, attributes)
    _set_ingress_operation(span, attributes)
    _set_span_kind_for_service(span, attributes)
    return attributes


def _generate_dependency_metric_attributes(span: ReadableSpan, resource: Resource) -> BoundedAttributes:
    attributes: BoundedAttributes = BoundedAttributes(immutable=False)
    _set_service(resource, span, attributes)
    _set_egress_operation(span, attributes)
    _set_remote_service_and_operation(span, attributes)
    _set_remote_target(span, attributes)
    _set_span_kind_for_dependency(span, attributes)
    return attributes


def _set_service(resource: Resource, span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """Service is always derived from SERVICE_NAME"""
    service: str = resource.attributes.get(_SERVICE_NAME)

    # In practice the service name is never None, but we can be defensive here.
    if service is None or service.startswith(_OTEL_UNKNOWN_SERVICE_PREFIX):
        _log_unknown_attribute(AWS_LOCAL_SERVICE, span)
        service = UNKNOWN_SERVICE

    attributes[AWS_LOCAL_SERVICE] = service


def _set_ingress_operation(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """
    Ingress operation (i.e. operation for Server and Consumer spans) will be generated from "http.method + http.target/
    with the first API path parameter" if the default span name is None, UnknownOperation or http.method value.
    """
    operation: str = get_ingress_operation(None, span)
    if operation == UNKNOWN_OPERATION:
        _log_unknown_attribute(AWS_LOCAL_OPERATION, span)

    attributes[AWS_LOCAL_OPERATION] = operation


def _set_span_kind_for_service(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """Span kind is needed for differentiating metrics in the EMF exporter"""
    span_kind: str = span.kind.name
    if is_local_root(span):
        span_kind = LOCAL_ROOT

    attributes[AWS_SPAN_KIND] = span_kind


def _set_egress_operation(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """
    Egress operation (i.e. operation for Client and Producer spans) is always derived from a special span attribute,
    AwsAttributeKeys.AWS_LOCAL_OPERATION. This attribute is generated with a separate SpanProcessor,
    AttributePropagatingSpanProcessor
    """
    operation: str = get_egress_operation(span)
    if operation is None:
        _log_unknown_attribute(AWS_LOCAL_OPERATION, span)
        operation = UNKNOWN_OPERATION

    attributes[AWS_LOCAL_OPERATION] = operation


def _set_remote_service_and_operation(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """
    Remote attributes (only for Client and Producer spans) are generated based on low-cardinality span attributes, in
    priority order.

    The first priority is the AWS Remote attributes, which are generated from manually instrumented span attributes, and
     are clear indications of customer intent. If AWS Remote attributes are not present, the next highest priority span
     attribute is Peer Service, which is also a reliable indicator of customer intent. If this is set, it will override
     AWS_REMOTE_SERVICE identified from any other span attribute, other than AWS Remote attributes.

    After this, we look for the following low-cardinality span attributes that can be used to determine the remote
    metric attributes:
    * RPC
    * DB
    * FAAS
    * Messaging
    * GraphQL - Special case, if GRAPHQL_OPERATION_TYPE is present, we use it for RemoteOperation and set RemoteService
      to GRAPHQL.

    In each case, these span attributes were selected from the OpenTelemetry trace semantic convention specifications as
     they adhere to the three following criteria:

    * Attributes are meaningfully indicative of remote service/operation names.
    * Attributes are defined in the specification to be low cardinality, usually with a low-cardinality list of values.
    * Attributes are confirmed to have low-cardinality values, based on code analysis.

    if the selected attributes are still producing the UnknownRemoteService or UnknownRemoteOperation, `net.peer.name`,
    `net.peer.port`, `net.peer.sock.addr`, `net.peer.sock.port`and 'http.url' will be used to derive the RemoteService.
    And `http.method` and `http.url` will be used to derive the RemoteOperation.
    """
    remote_service: str = UNKNOWN_REMOTE_SERVICE
    remote_operation: str = UNKNOWN_REMOTE_OPERATION
    if is_key_present(span, AWS_REMOTE_SERVICE) or is_key_present(span, AWS_REMOTE_OPERATION):
        remote_service = _get_remote_service(span, AWS_REMOTE_SERVICE)
        remote_operation = _get_remote_operation(span, AWS_REMOTE_OPERATION)
    elif is_key_present(span, _RPC_SERVICE) or is_key_present(span, _RPC_METHOD):
        remote_service = _normalize_service_name(span, _get_remote_service(span, _RPC_SERVICE))
        remote_operation = _get_remote_operation(span, _RPC_METHOD)
    elif is_key_present(span, _DB_SYSTEM) or is_key_present(span, _DB_OPERATION) or is_key_present(span, _DB_STATEMENT):
        remote_service = _get_remote_service(span, _DB_SYSTEM)
        if is_key_present(span, _DB_OPERATION):
            remote_operation = _get_remote_operation(span, _DB_OPERATION)
        else:
            remote_operation = _get_db_statement_remote_operation(span, _DB_STATEMENT)
    elif is_key_present(span, _FAAS_INVOKED_NAME) or is_key_present(span, _FAAS_TRIGGER):
        remote_service = _get_remote_service(span, _FAAS_INVOKED_NAME)
        remote_operation = _get_remote_operation(span, _FAAS_TRIGGER)
    elif is_key_present(span, _MESSAGING_SYSTEM) or is_key_present(span, _MESSAGING_OPERATION):
        remote_service = _get_remote_service(span, _MESSAGING_SYSTEM)
        remote_operation = _get_remote_operation(span, _MESSAGING_OPERATION)
    elif is_key_present(span, _GRAPHQL_OPERATION_TYPE):
        remote_service = _GRAPHQL
        remote_operation = _get_remote_operation(span, _GRAPHQL_OPERATION_TYPE)

    # Peer service takes priority as RemoteService over everything but AWS Remote.
    if is_key_present(span, _PEER_SERVICE) and not is_key_present(span, AWS_REMOTE_SERVICE):
        remote_service = _get_remote_service(span, _PEER_SERVICE)

    # Try to derive RemoteService and RemoteOperation from the other related attributes.
    if remote_service == UNKNOWN_REMOTE_SERVICE:
        remote_service = _generate_remote_service(span)
    if remote_operation == UNKNOWN_REMOTE_OPERATION:
        remote_operation = _generate_remote_operation(span)

    attributes[AWS_REMOTE_SERVICE] = remote_service
    attributes[AWS_REMOTE_OPERATION] = remote_operation


def _get_remote_service(span: ReadableSpan, remote_service_key: str) -> str:
    remote_service: str = span.attributes.get(remote_service_key)
    if remote_service is None:
        remote_service = UNKNOWN_REMOTE_SERVICE

    return remote_service


def _get_remote_operation(span: ReadableSpan, remote_operation_key: str) -> str:
    remote_operation: str = span.attributes.get(remote_operation_key)
    if remote_operation is None:
        remote_operation = UNKNOWN_REMOTE_OPERATION

    return remote_operation


def _get_db_statement_remote_operation(span: ReadableSpan, statement_key: str) -> str:
    """
    If no db.operation attribute provided in the span,
    we use db.statement to compute a valid remote operation in a best-effort manner.
    To do this, we take the first substring of the statement
    and compare to a regex list of known SQL keywords.
    The substring length is determined by the longest known SQL keywords.
    """
    remote_operation: str = span.attributes.get(statement_key)

    if remote_operation is None:
        return UNKNOWN_REMOTE_OPERATION

    # Remove all whitespace and newline characters from the beginning of remote_operation
    # and retrieve the first MAX_KEYWORD_LENGTH characters
    remote_operation = remote_operation.lstrip()[:MAX_KEYWORD_LENGTH]
    match: Optional[Match[str]] = re.match(SQL_KEYWORD_PATTERN, remote_operation.upper())
    remote_operation = match.group(0) if match else UNKNOWN_REMOTE_OPERATION

    return remote_operation


def _normalize_service_name(span: ReadableSpan, service_name: str) -> str:
    if is_aws_sdk_span(span):
        return "AWS.SDK." + service_name

    return service_name


def _generate_remote_service(span: ReadableSpan) -> str:
    remote_service: str = UNKNOWN_REMOTE_SERVICE
    if is_key_present(span, _NET_PEER_NAME):
        remote_service = _get_remote_service(span, _NET_PEER_NAME)
        if is_key_present(span, _NET_PEER_PORT):
            port: str = str(span.attributes.get(_NET_PEER_PORT))
            remote_service += ":" + port
    elif is_key_present(span, _NET_SOCK_PEER_ADDR):
        remote_service = _get_remote_service(span, _NET_SOCK_PEER_ADDR)
        if is_key_present(span, _NET_SOCK_PEER_PORT):
            port: str = str(span.attributes.get(_NET_SOCK_PEER_PORT))
            remote_service += ":" + port
    elif is_key_present(span, _HTTP_URL):
        http_url: str = span.attributes.get(_HTTP_URL)
        if http_url:
            url: ParseResult = urlparse(http_url)
            if url and url.netloc:
                remote_service = url.netloc
    else:
        _log_unknown_attribute(AWS_REMOTE_SERVICE, span)

    return remote_service


def _generate_remote_operation(span: ReadableSpan) -> str:
    """
    When the remote call operation is undetermined for http use cases, will try to extract the remote operation name
    from http url string
    """
    remote_operation: str = UNKNOWN_REMOTE_OPERATION
    if is_key_present(span, _HTTP_URL):
        http_url: str = span.attributes.get(_HTTP_URL)
        url: ParseResult = urlparse(http_url)
        remote_operation = extract_api_path_value(url.path)

    if is_key_present(span, _HTTP_METHOD):
        http_method: str = span.attributes.get(_HTTP_METHOD)
        remote_operation = http_method + " " + remote_operation

    if remote_operation == UNKNOWN_REMOTE_OPERATION:
        _log_unknown_attribute(AWS_REMOTE_OPERATION, span)

    return remote_operation


def _set_remote_target(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    remote_target: Optional[str] = _get_remote_target(span)
    if remote_target is not None:
        attributes[AWS_REMOTE_TARGET] = remote_target


def _get_remote_target(span: ReadableSpan) -> Optional[str]:
    """
    RemoteTarget attribute AWS_REMOTE_TARGET is used to store the resource
    name of the remote invokes, such as S3 bucket name, mysql table name, etc.
    TODO: currently only support AWS resource name, will be extended to support
    the general remote targets, such as ActiveMQ name, etc.
    """
    if is_key_present(span, _AWS_BUCKET_NAME):
        return "::s3:::" + span.attributes.get(_AWS_BUCKET_NAME)

    if is_key_present(span, AWS_QUEUE_URL):
        arn = SqsUrlParser.get_sqs_remote_target(span.attributes.get(AWS_QUEUE_URL))
        if arn:
            return arn

    if is_key_present(span, AWS_QUEUE_NAME):
        return "::sqs:::" + span.attributes.get(AWS_QUEUE_NAME)

    if is_key_present(span, AWS_STREAM_NAME):
        return "::kinesis:::stream/" + span.attributes.get(AWS_STREAM_NAME)

    # Only extract the table name when _AWS_TABLE_NAMES has size equals to one
    if is_key_present(span, _AWS_TABLE_NAMES) and len(span.attributes.get(_AWS_TABLE_NAMES)) == 1:
        return "::dynamodb:::table/" + span.attributes.get(_AWS_TABLE_NAMES)[0]

    return None


def _set_span_kind_for_dependency(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    span_kind: str = span.kind.name
    attributes[AWS_SPAN_KIND] = span_kind


def _log_unknown_attribute(attribute_key: str, span: ReadableSpan) -> None:
    message: str = "No valid %s value found for %s span %s"
    _logger.log(DEBUG, message, attribute_key, span.kind.name, str(span.context.span_id))
