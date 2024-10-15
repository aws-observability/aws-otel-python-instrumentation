# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
from logging import DEBUG, Logger, getLogger
from typing import Match, Optional
from urllib.parse import ParseResult, urlparse

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_BEDROCK_AGENT_ID,
    AWS_BEDROCK_DATA_SOURCE_ID,
    AWS_BEDROCK_GUARDRAIL_ARN,
    AWS_BEDROCK_GUARDRAIL_ID,
    AWS_BEDROCK_KNOWLEDGE_BASE_ID,
    AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER,
    AWS_KINESIS_STREAM_NAME,
    AWS_LAMBDA_RESOURCEMAPPING_ID,
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_DB_USER,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_RESOURCE_IDENTIFIER,
    AWS_REMOTE_RESOURCE_TYPE,
    AWS_REMOTE_SERVICE,
    AWS_SECRETSMANAGER_SECRET_ARN,
    AWS_SNS_TOPIC_ARN,
    AWS_SPAN_KIND,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
    AWS_STEPFUNCTIONS_ACTIVITY_ARN,
    AWS_STEPFUNCTIONS_STATEMACHINE_ARN,
)
from amazon.opentelemetry.distro._aws_resource_attribute_configurator import get_service_attribute
from amazon.opentelemetry.distro._aws_span_processing_util import (
    GEN_AI_REQUEST_MODEL,
    LOCAL_ROOT,
    MAX_KEYWORD_LENGTH,
    SQL_KEYWORD_PATTERN,
    UNKNOWN_OPERATION,
    UNKNOWN_REMOTE_OPERATION,
    UNKNOWN_REMOTE_SERVICE,
    extract_api_path_value,
    get_egress_operation,
    get_ingress_operation,
    is_aws_sdk_span,
    is_db_span,
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
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import BoundedAttributes, ReadableSpan
from opentelemetry.semconv.trace import SpanAttributes

# Pertinent OTEL attribute keys
_DB_CONNECTION_STRING: str = SpanAttributes.DB_CONNECTION_STRING
_DB_NAME: str = SpanAttributes.DB_NAME
_DB_OPERATION: str = SpanAttributes.DB_OPERATION
_DB_STATEMENT: str = SpanAttributes.DB_STATEMENT
_DB_SYSTEM: str = SpanAttributes.DB_SYSTEM
_DB_USER: str = SpanAttributes.DB_USER
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
_SERVER_ADDRESS: str = SpanAttributes.SERVER_ADDRESS
_SERVER_PORT: str = SpanAttributes.SERVER_PORT
_SERVER_SOCKET_ADDRESS: str = SpanAttributes.SERVER_SOCKET_ADDRESS
_SERVER_SOCKET_PORT: str = SpanAttributes.SERVER_SOCKET_PORT
_AWS_TABLE_NAMES: str = SpanAttributes.AWS_DYNAMODB_TABLE_NAMES
_AWS_BUCKET_NAME: str = SpanAttributes.AWS_S3_BUCKET

# Normalized remote service names for supported AWS services
_NORMALIZED_DYNAMO_DB_SERVICE_NAME: str = "AWS::DynamoDB"
_NORMALIZED_KINESIS_SERVICE_NAME: str = "AWS::Kinesis"
_NORMALIZED_S3_SERVICE_NAME: str = "AWS::S3"
_NORMALIZED_SQS_SERVICE_NAME: str = "AWS::SQS"
_NORMALIZED_BEDROCK_SERVICE_NAME: str = "AWS::Bedrock"
_NORMALIZED_BEDROCK_RUNTIME_SERVICE_NAME: str = "AWS::BedrockRuntime"
_NORMALIZED_SECRETSMANAGER_SERVICE_NAME: str = "AWS::SecretsManager"
_NORMALIZED_SNS_SERVICE_NAME: str = "AWS::SNS"
_NORMALIZED_STEPFUNCTIONS_SERVICE_NAME: str = "AWS::StepFunctions"
_NORMALIZED_LAMBDA_SERVICE_NAME: str = "AWS::Lambda"
_DB_CONNECTION_STRING_TYPE: str = "DB::Connection"

# Special DEPENDENCY attribute value if GRAPHQL_OPERATION_TYPE attribute key is present.
_GRAPHQL: str = "graphql"

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
    _set_remote_type_and_identifier(span, attributes)
    _set_remote_db_user(span, attributes)
    _set_span_kind_for_dependency(span, attributes)
    return attributes


def _set_service(resource: Resource, span: ReadableSpan, attributes: BoundedAttributes) -> None:
    service_name, is_unknown = get_service_attribute(resource)
    if is_unknown:
        _log_unknown_attribute(AWS_LOCAL_SERVICE, span)

    attributes[AWS_LOCAL_SERVICE] = service_name


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
        remote_service = _normalize_remote_service_name(span, _get_remote_service(span, _RPC_SERVICE))
        remote_operation = _get_remote_operation(span, _RPC_METHOD)
    elif is_db_span(span):
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


def _normalize_remote_service_name(span: ReadableSpan, service_name: str) -> str:
    """
    If the span is an AWS SDK span, normalize the name to align with <a
    href="https://docs.aws.amazon.com/cloudcontrolapi/latest/userguide/supported-resources.html">AWS Cloud Control
    resource format</a> as much as possible. Long term, we would like to normalize service name in the upstream.

    For Bedrock, Bedrock Agent, and Bedrock Agent Runtime, we can align with AWS Cloud Control and use
    AWS::Bedrock for RemoteService. For BedrockRuntime, we are using AWS::BedrockRuntime
    as the associated remote resource (Model) is not listed in Cloud Control.
    """
    if is_aws_sdk_span(span):
        aws_sdk_service_mapping = {
            "Bedrock Agent": _NORMALIZED_BEDROCK_SERVICE_NAME,
            "Bedrock Agent Runtime": _NORMALIZED_BEDROCK_SERVICE_NAME,
            "Bedrock Runtime": _NORMALIZED_BEDROCK_RUNTIME_SERVICE_NAME,
            "Secrets Manager": _NORMALIZED_SECRETSMANAGER_SERVICE_NAME,
            "SNS": _NORMALIZED_SNS_SERVICE_NAME,
            "SFN": _NORMALIZED_STEPFUNCTIONS_SERVICE_NAME,
        }
        return aws_sdk_service_mapping.get(service_name, "AWS::" + service_name)
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


# pylint: disable=too-many-branches,too-many-statements
def _set_remote_type_and_identifier(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    """
    Remote resource attributes {@link AwsAttributeKeys#AWS_REMOTE_RESOURCE_TYPE} and {@link
    AwsAttributeKeys#AWS_REMOTE_RESOURCE_IDENTIFIER} are used to store information about the resource associated with
    the remote invocation, such as S3 bucket name, etc. We should only ever set both type and identifier or neither.
    If any identifier value contains | or ^ , they will be replaced with ^| or ^^.

    AWS resources type and identifier adhere to <a
    href="https://docs.aws.amazon.com/cloudcontrolapi/latest/userguide/supported-resources.html">AWS Cloud Control
    resource format</a>.
    """
    remote_resource_type: Optional[str] = None
    remote_resource_identifier: Optional[str] = None
    cloudformation_primary_identifier: Optional[str] = None

    if is_aws_sdk_span(span):
        # Only extract the table name when _AWS_TABLE_NAMES has size equals to one
        if is_key_present(span, _AWS_TABLE_NAMES) and len(span.attributes.get(_AWS_TABLE_NAMES)) == 1:
            remote_resource_type = _NORMALIZED_DYNAMO_DB_SERVICE_NAME + "::Table"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(_AWS_TABLE_NAMES)[0])
        elif is_key_present(span, AWS_KINESIS_STREAM_NAME):
            remote_resource_type = _NORMALIZED_KINESIS_SERVICE_NAME + "::Stream"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_KINESIS_STREAM_NAME))
        elif is_key_present(span, _AWS_BUCKET_NAME):
            remote_resource_type = _NORMALIZED_S3_SERVICE_NAME + "::Bucket"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(_AWS_BUCKET_NAME))
        elif is_key_present(span, AWS_SQS_QUEUE_NAME):
            remote_resource_type = _NORMALIZED_SQS_SERVICE_NAME + "::Queue"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_SQS_QUEUE_NAME))
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_SQS_QUEUE_URL))
        elif is_key_present(span, AWS_SQS_QUEUE_URL):
            remote_resource_type = _NORMALIZED_SQS_SERVICE_NAME + "::Queue"
            remote_resource_identifier = _escape_delimiters(
                SqsUrlParser.get_queue_name(span.attributes.get(AWS_SQS_QUEUE_URL))
            )
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_SQS_QUEUE_URL))
        elif is_key_present(span, AWS_BEDROCK_AGENT_ID):
            remote_resource_type = _NORMALIZED_BEDROCK_SERVICE_NAME + "::Agent"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_BEDROCK_AGENT_ID))
        elif is_key_present(span, AWS_BEDROCK_DATA_SOURCE_ID):
            remote_resource_type = _NORMALIZED_BEDROCK_SERVICE_NAME + "::DataSource"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_BEDROCK_DATA_SOURCE_ID))
            cloudformation_primary_identifier = (
                _escape_delimiters(span.attributes.get(AWS_BEDROCK_KNOWLEDGE_BASE_ID))
                + "|"
                + remote_resource_identifier
            )
        elif is_key_present(span, AWS_BEDROCK_GUARDRAIL_ID):
            remote_resource_type = _NORMALIZED_BEDROCK_SERVICE_NAME + "::Guardrail"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_BEDROCK_GUARDRAIL_ID))
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_BEDROCK_GUARDRAIL_ARN))
        elif is_key_present(span, AWS_BEDROCK_KNOWLEDGE_BASE_ID):
            remote_resource_type = _NORMALIZED_BEDROCK_SERVICE_NAME + "::KnowledgeBase"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_BEDROCK_KNOWLEDGE_BASE_ID))
        elif is_key_present(span, GEN_AI_REQUEST_MODEL):
            remote_resource_type = _NORMALIZED_BEDROCK_SERVICE_NAME + "::Model"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(GEN_AI_REQUEST_MODEL))
        elif is_key_present(span, AWS_SECRETSMANAGER_SECRET_ARN):
            remote_resource_type = _NORMALIZED_SECRETSMANAGER_SERVICE_NAME + "::Secret"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_SECRETSMANAGER_SECRET_ARN)).split(
                ":"
            )[-1]
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_SECRETSMANAGER_SECRET_ARN))
        elif is_key_present(span, AWS_SNS_TOPIC_ARN):
            remote_resource_type = _NORMALIZED_SNS_SERVICE_NAME + "::Topic"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_SNS_TOPIC_ARN)).split(":")[-1]
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_SNS_TOPIC_ARN))
        elif is_key_present(span, AWS_STEPFUNCTIONS_STATEMACHINE_ARN):
            remote_resource_type = _NORMALIZED_STEPFUNCTIONS_SERVICE_NAME + "::StateMachine"
            remote_resource_identifier = _escape_delimiters(
                span.attributes.get(AWS_STEPFUNCTIONS_STATEMACHINE_ARN)
            ).split(":")[-1]
            cloudformation_primary_identifier = _escape_delimiters(
                span.attributes.get(AWS_STEPFUNCTIONS_STATEMACHINE_ARN)
            )
        elif is_key_present(span, AWS_STEPFUNCTIONS_ACTIVITY_ARN):
            remote_resource_type = _NORMALIZED_STEPFUNCTIONS_SERVICE_NAME + "::Activity"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_STEPFUNCTIONS_ACTIVITY_ARN)).split(
                ":"
            )[-1]
            cloudformation_primary_identifier = _escape_delimiters(span.attributes.get(AWS_STEPFUNCTIONS_ACTIVITY_ARN))
        elif is_key_present(span, AWS_LAMBDA_RESOURCEMAPPING_ID):
            remote_resource_type = _NORMALIZED_LAMBDA_SERVICE_NAME + "::EventSourceMapping"
            remote_resource_identifier = _escape_delimiters(span.attributes.get(AWS_LAMBDA_RESOURCEMAPPING_ID))
    elif is_db_span(span):
        remote_resource_type = _DB_CONNECTION_STRING_TYPE
        remote_resource_identifier = _get_db_connection(span)

    # If the CFN Primary Id is still None here, that means it is not an edge case.
    # Then, we can just assign it the same value as remote_resource_identifier
    if cloudformation_primary_identifier is None:
        cloudformation_primary_identifier = remote_resource_identifier

    if (
        remote_resource_type is not None
        and remote_resource_identifier is not None
        and cloudformation_primary_identifier is not None
    ):
        attributes[AWS_REMOTE_RESOURCE_TYPE] = remote_resource_type
        attributes[AWS_REMOTE_RESOURCE_IDENTIFIER] = remote_resource_identifier
        attributes[AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER] = cloudformation_primary_identifier


def _get_db_connection(span: ReadableSpan) -> None:
    """
    RemoteResourceIdentifier is populated with rule:
        ^[{db.name}|]?{address}[|{port}]?

    {address} attribute is retrieved in priority order:
    - {SpanAttributes.SERVER_ADDRESS},
    - {SpanAttributes.NET_PEER_NAME},
    - {SpanAttributes.SERVER_SOCKET_ADDRESS},
    - {SpanAttributes.DB_CONNECTION_STRING}-Hostname

    {port} attribute is retrieved in priority order:
    - {SpanAttributes.SERVER_PORT},
    - {SpanAttributes.NET_PEER_PORT},
    - {SpanAttributes.SERVER_SOCKET_PORT},
    - {SpanAttributes.DB_CONNECTION_STRING}-Port

    If address is not present, neither RemoteResourceType nor RemoteResourceIdentifier will be provided.
    """
    db_name: Optional[str] = span.attributes.get(_DB_NAME)
    db_connection: Optional[str] = None

    if is_key_present(span, _SERVER_ADDRESS):
        server_address: Optional[str] = span.attributes.get(_SERVER_ADDRESS)
        server_port: Optional[int] = span.attributes.get(_SERVER_PORT)
        db_connection = _build_db_connection(server_address, server_port)
    elif is_key_present(span, _NET_PEER_NAME):
        network_peer_address: Optional[str] = span.attributes.get(_NET_PEER_NAME)
        network_peer_port: Optional[int] = span.attributes.get(_NET_PEER_PORT)
        db_connection = _build_db_connection(network_peer_address, network_peer_port)
    elif is_key_present(span, _SERVER_SOCKET_ADDRESS):
        server_socket_address: Optional[str] = span.attributes.get(_SERVER_SOCKET_ADDRESS)
        server_socket_port: Optional[int] = span.attributes.get(_SERVER_SOCKET_PORT)
        db_connection = _build_db_connection(server_socket_address, server_socket_port)
    elif is_key_present(span, _DB_CONNECTION_STRING):
        connection_string: Optional[str] = span.attributes.get(_DB_CONNECTION_STRING)
        db_connection = _build_db_connection_string(connection_string)

    if db_connection and db_name:
        db_connection = _escape_delimiters(db_name) + "|" + db_connection

    return db_connection


def _build_db_connection(address: str, port: int) -> Optional[str]:
    return _escape_delimiters(address) + ("|" + str(port) if port else "")


def _build_db_connection_string(connection_string: str) -> Optional[str]:
    uri = urlparse(connection_string)
    address = uri.hostname
    try:
        port = uri.port
    except ValueError:
        port = None

    if address is None:
        return None

    port_str = "|" + str(port) if port is not None and port != -1 else ""
    return _escape_delimiters(address) + port_str


def _escape_delimiters(input_str: str) -> Optional[str]:
    if input_str is None:
        return None
    return input_str.replace("^", "^^").replace("|", "^|")


def _set_remote_db_user(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    if is_db_span(span) and is_key_present(span, _DB_USER):
        attributes[AWS_REMOTE_DB_USER] = span.attributes.get(_DB_USER)


def _set_span_kind_for_dependency(span: ReadableSpan, attributes: BoundedAttributes) -> None:
    span_kind: str = span.kind.name
    attributes[AWS_SPAN_KIND] = span_kind


def _log_unknown_attribute(attribute_key: str, span: ReadableSpan) -> None:
    message: str = "No valid %s value found for %s span %s"
    if _logger.isEnabledFor(DEBUG):
        _logger.log(DEBUG, message, attribute_key, span.kind.name, str(span.context.span_id))
