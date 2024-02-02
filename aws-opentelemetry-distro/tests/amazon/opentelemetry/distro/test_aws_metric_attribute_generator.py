# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_CONSUMER_PARENT_SPAN_KIND,
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
)
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro._aws_span_processing_util import (
    INTERNAL_OPERATION,
    LOCAL_ROOT,
    UNKNOWN_OPERATION,
    UNKNOWN_REMOTE_OPERATION,
    UNKNOWN_REMOTE_SERVICE,
    UNKNOWN_SERVICE,
)
from amazon.opentelemetry.distro.metric_attribute_generator import DEPENDENCY_METRIC, SERVICE_METRIC
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE, SERVICE_NAME
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.util.types import Attributes

# Protected constants with uppercase naming and type annotations

_AWS_LOCAL_OPERATION_VALUE: str = "AWS local operation"
_AWS_REMOTE_SERVICE_VALUE: str = "AWS remote service"
_AWS_REMOTE_OPERATION_VALUE: str = "AWS remote operation"
_SERVICE_NAME_VALUE: str = "Service name"
_SPAN_NAME_VALUE: str = "Span name"

_UNKNOWN_SERVICE: str = "UnknownService"
_UNKNOWN_OPERATION: str = "UnknownOperation"
_UNKNOWN_REMOTE_SERVICE: str = "UnknownRemoteService"
_UNKNOWN_REMOTE_OPERATION: str = "UnknownRemoteOperation"

_INTERNAL_OPERATION: str = "InternalOperation"
_LOCAL_ROOT: str = "LOCAL_ROOT"

_GENERATOR = _AwsMetricAttributeGenerator()


# class ThrowableWithMethodGetStatusCode(Exception):
#     """
#     A custom exception class that includes an HTTP status code.
#     """
#
#     def __init__(self, http_status_code: int) -> None:
#         """
#         Initialize the exception with an HTTP status code.
#
#         Args:
#         http_status_code (int): The HTTP status code associated with this exception.
#         """
#         super().__init__()
#         self._http_status_code: int = http_status_code
#
#     def get_status_code(self) -> int:
#         """
#         Return the HTTP status code associated with this exception.
#
#         Returns:
#         int: The HTTP status code.
#         """
#         return self._http_status_code


class TestAwsMetricAttributeGenerator(TestCase):
    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.instrumentation_scope_info_mock: InstrumentationScope = MagicMock()
        self.instrumentation_scope_info_mock._name = "Scope name"
        self.span_mock: ReadableSpan = MagicMock()
        self.span_mock.name = None
        self.span_mock.attributes = self.attributes_mock
        self.attributes_mock.get.return_value = None
        self.span_mock._instrumentation_scope = self.instrumentation_scope_info_mock
        self.span_mock.get_span_context.return_value = MagicMock()
        self.parent_span_context: SpanContext = MagicMock()
        self.parent_span_context.is_valid = True
        self.parent_span_context.is_remote = False
        self.span_mock.parent = self.parent_span_context

        self.resource: Resource = _DEFAULT_RESOURCE

    def test_span_attributes_for_empty_resource(self):
        self.resource = Resource.get_empty()
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_consumer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_producer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    def test_internal_span(self):
        # Spans with internal span kind should not produce any attributes.
        self._validate_attributes_produced_for_non_local_root_span_of_kind({}, SpanKind.INTERNAL)

    def test_local_root_server_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: {str: Attributes} = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE,
            }
        }

        self.span_mock.kind = SpanKind.SERVER
        actual_attributes_map: {str: Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_internal_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: {str: Attributes} = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
            }
        }

        self.span_mock.kind = SpanKind.INTERNAL
        actual_attributes_map = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_client_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: {str: Attributes} = {
            SERVICE_METRIC: BoundedAttributes(
                attributes={
                    AWS_SPAN_KIND: LOCAL_ROOT,
                    AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                    AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                },
                maxlen=None,
            ),
            DEPENDENCY_METRIC: BoundedAttributes(
                attributes={
                    AWS_SPAN_KIND: SpanKind.CLIENT.name,
                    AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                    AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                    AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                    AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
                },
                maxlen=None,
            ),
        }

        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_consumer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: {str: Attributes} = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CONSUMER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def _validate_attributes_produced_for_non_local_root_span_of_kind(
        self, expected_attributes: Attributes, kind: SpanKind
    ):
        self.span_mock.kind = kind

        attribute_map: {str, Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        service_attributes: BoundedAttributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: BoundedAttributes = attribute_map.get(DEPENDENCY_METRIC)
        if attribute_map is not None and len(attribute_map) > 0:
            if kind == SpanKind.PRODUCER or kind == SpanKind.CLIENT or kind == SpanKind.CONSUMER:
                self.assertIsNone(service_attributes)
                self.assertEqual(len(dependency_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(dependency_attributes, BoundedAttributes(attributes=expected_attributes))
            else:
                self.assertIsNone(dependency_attributes)
                self.assertEqual(len(service_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(service_attributes, BoundedAttributes(attributes=expected_attributes))

    def test_local_root_producer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: {str: Attributes} = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.PRODUCER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_consumer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_null_span_name(self):
        self._update_resource_with_service_name()
        self.span_mock.name = None

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_span_name_as_http_method(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "GET"
        self._mock_attribute([SpanAttributes.HTTP_METHOD], ["GET"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)

    def test_server_span_with_span_name_with_http_target(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_TARGET], ["POST", "/payment/123"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /payment",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_TARGET, None)

    def test_producer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute(
            [AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION],
            [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE],
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute(
            [AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION],
            [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE],
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    def test_remote_attributes_combinations(self):
        keys: [str] = [
            AWS_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION,
            SpanAttributes.RPC_SERVICE,
            SpanAttributes.RPC_METHOD,
            SpanAttributes.DB_SYSTEM,
            SpanAttributes.DB_OPERATION,
            SpanAttributes.FAAS_INVOKED_PROVIDER,
            SpanAttributes.FAAS_INVOKED_NAME,
            SpanAttributes.MESSAGING_SYSTEM,
            SpanAttributes.MESSAGING_OPERATION,
            SpanAttributes.GRAPHQL_OPERATION_TYPE,
            "unknown.service.key",
            "unknown.operation.key",
        ]
        values: [str] = [
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
        ]
        self._mock_attribute(keys, values)

        keys, values = self._validate_and_remove_remote_attributes(
            AWS_REMOTE_SERVICE,
            _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION,
            _AWS_REMOTE_OPERATION_VALUE,
            keys,
            values,
        )

        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.RPC_SERVICE, "RPC service", SpanAttributes.RPC_METHOD, "RPC method", keys, values
        )

        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.DB_SYSTEM, "DB system", SpanAttributes.DB_OPERATION, "DB operation", keys, values
        )

        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.FAAS_INVOKED_NAME,
            "FAAS invoked name",
            SpanAttributes.FAAS_TRIGGER,
            "FAAS trigger name",
            keys,
            values,
        )

        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.MESSAGING_SYSTEM,
            "Messaging system",
            SpanAttributes.MESSAGING_OPERATION,
            "Messaging operation",
            keys,
            values,
        )

        keys, values = self._mock_attribute(
            [SpanAttributes.GRAPHQL_OPERATION_TYPE], ["GraphQL operation type"], keys, values
        )
        self._validate_expected_remote_attributes("graphql", "GraphQL operation type")
        keys, values = self._mock_attribute([SpanAttributes.GRAPHQL_OPERATION_TYPE], [None], keys, values)

        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], ["www.example.com"], keys, values)
        self._validate_expected_remote_attributes("www.example.com", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], [None], keys, values)

        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR, SpanAttributes.NET_SOCK_PEER_PORT],
            ["192.168.0.0", "8081"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("192.168.0.0:8081", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR, SpanAttributes.NET_SOCK_PEER_PORT], [None, None], keys, values
        )

        keys, values = self._mock_attribute(
            [SpanAttributes.HTTP_URL], ["http://www.example.com/payment/123"], keys, values
        )
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/payment")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["http://www.example.com"], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["abc"], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], ["Peer service"], keys, values)
        self._validate_expected_remote_attributes("Peer service", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], [None], keys, values)

        self._validate_expected_remote_attributes(UNKNOWN_REMOTE_SERVICE, UNKNOWN_REMOTE_OPERATION)

    def test_peer_service_does_override_other_remote_services(self):
        self._validate_peer_service_does_override(SpanAttributes.RPC_SERVICE)
        self._validate_peer_service_does_override(SpanAttributes.DB_SYSTEM)
        self._validate_peer_service_does_override(SpanAttributes.FAAS_INVOKED_PROVIDER)
        self._validate_peer_service_does_override(SpanAttributes.MESSAGING_SYSTEM)
        self._validate_peer_service_does_override(SpanAttributes.GRAPHQL_OPERATION_TYPE)
        self._validate_peer_service_does_override(SpanAttributes.NET_PEER_NAME)
        self._validate_peer_service_does_override(SpanAttributes.NET_SOCK_PEER_ADDR)
        # Actually testing that peer service overrides "UnknownRemoteService".
        self._validate_peer_service_does_override("unknown.service.key")

    def test_peer_service_does_not_override_aws_remote_service(self):
        self._mock_attribute([AWS_REMOTE_SERVICE, SpanAttributes.PEER_SERVICE], ["TestString", "PeerService"])
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "TestString")

    # Exception handling and remote target handling seems not really available not
    # testClientSpanWithRemoteTargetAttributes && testHttpStatusAttributeXXXX are not available here

    def test_normalize_service_name_non_aws_sdk_span(self):
        service_name: str = "non aws service"
        self._mock_attribute([SpanAttributes.RPC_SERVICE], [service_name])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), service_name)

    def test_normalize_service_name_aws_sdk_v1_span(self):
        service_name: str = "Amazon S3"
        self._mock_attribute([SpanAttributes.RPC_SYSTEM, SpanAttributes.RPC_SERVICE], ["aws-api", service_name])
        self.span_mock.instrumentation_scope = InstrumentationScope("io.opentelemetry.aws-sdk-1.11 1.28.0-alpha")
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), service_name)

    def test_normalize_service_name_aws_sdk_v2_span(self):
        service_name: str = "DynamoDb"
        self._mock_attribute([SpanAttributes.RPC_SYSTEM, SpanAttributes.RPC_SERVICE], ["aws-api", service_name])
        self.span_mock.instrumentation_scope = InstrumentationScope("io.opentelemetry.aws-sdk-2.2 1.28.0-alpha")
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), service_name)

    def test_no_metric_when_consumer_process_with_consumer_parent(self):
        self._mock_attribute(
            [AWS_CONSUMER_PARENT_SPAN_KIND, SpanAttributes.MESSAGING_OPERATION],
            [SpanKind.CONSUMER, MessagingOperationValues.PROCESS],
        )
        self.span_mock.kind = SpanKind.CONSUMER

        attribute_map: {str: Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )

        service_attributes: Attributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: Attributes = attribute_map.get(DEPENDENCY_METRIC)

        self.assertIsNone(service_attributes)
        self.assertIsNone(dependency_attributes)

    def test_both_metric_when_local_root_consumer_process(self):
        self._mock_attribute(
            [AWS_CONSUMER_PARENT_SPAN_KIND, SpanAttributes.MESSAGING_OPERATION],
            [SpanKind.CONSUMER, MessagingOperationValues.PROCESS],
        )
        self.span_mock.kind = SpanKind.CONSUMER
        self.parent_span_context.is_valid = False

        attribute_map: {str: Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )

        service_attributes: Attributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: Attributes = attribute_map.get(DEPENDENCY_METRIC)

        self.assertIsNotNone(service_attributes)
        self.assertIsNotNone(dependency_attributes)

    def _update_resource_with_service_name(self):
        self.resource: Resource = Resource(attributes={SERVICE_NAME: _SERVICE_NAME_VALUE})

    def _mock_attribute(
        self,
        keys: [str],
        values: [Optional[str]],
        exist_keys: Optional[str] = None,
        exist_values: Optional[Optional[str]] = None,
    ):
        if exist_keys is not None and exist_values is not None:
            for key in exist_keys:
                if key not in keys:
                    keys = keys + [key]
                    values = values + [exist_values[exist_keys.index(key)]]

        def get_side_effect(get_key):
            if get_key in keys:
                return values[keys.index(get_key)]
            return None

        self.attributes_mock.get.side_effect = get_side_effect

        return keys, values

    def _validate_expected_remote_attributes(self, expected_remote_service, expected_remote_operation):
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

    def _validate_and_remove_remote_attributes(
        self, remote_service_key, remote_service_value, remote_operation_key, remote_operation_value, keys, values
    ):
        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [None, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(UNKNOWN_REMOTE_SERVICE, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, None], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, UNKNOWN_REMOTE_OPERATION)

        keys, values = self._mock_attribute([remote_service_key, remote_operation_key], [None, None], keys, values)
        return keys, values

    def _validate_peer_service_does_override(self, remote_service_key):
        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], ["TestString", "PeerService"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "PeerService")

        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], [None, None])

    def _validate_remote_target_attributes(self, remote_target_key: str, remote_target: str):
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(remote_target_key), remote_target)

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(remote_target_key), remote_target)

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(remote_target_key), remote_target)

        # Server span should not generate RemoteTarget attribute
        self.span_mock.kind = SpanKind.SERVER
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertIsNone(actual_attributes.get(remote_target_key))
