# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_BUCKET_NAME,
    AWS_CONSUMER_PARENT_SPAN_KIND,
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_QUEUE_NAME,
    AWS_QUEUE_URL,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_REMOTE_TARGET,
    AWS_SPAN_KIND,
    AWS_STREAM_NAME,
    AWS_TABLE_NAME,
)
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
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

_GENERATOR: _AwsMetricAttributeGenerator = _AwsMetricAttributeGenerator()


# pylint: disable=too-many-public-methods
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

        # OTel strongly recommends to start out with the default instead of Resource.empty()
        self.resource: Resource = _DEFAULT_RESOURCE

    def test_span_attributes_for_empty_resource(self):
        self.resource = Resource.get_empty()
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_consumer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_producer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    def test_internal_span(self):
        # Spans with internal span kind should not produce any attributes.
        self._validate_attributes_produced_for_non_local_root_span_of_kind({}, SpanKind.INTERNAL)

    def test_local_root_server_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE,
            }
        }

        self.span_mock.kind = SpanKind.SERVER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_internal_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            }
        }

        self.span_mock.kind = SpanKind.INTERNAL
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_client_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CLIENT.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
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

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CONSUMER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_producer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.PRODUCER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_consumer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
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
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_span_name_as_http_method(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "GET"
        self._mock_attribute([SpanAttributes.HTTP_METHOD], ["GET"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
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
        # Set all expected fields to a test string, we will overwrite them in descending order to test
        # the priority-order logic in AwsMetricAttributeGenerator remote attribute methods.
        keys: List[str] = [
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
            # Do not set dummy value for PEER_SERVICE, since it has special behaviour.
            # Two unused attributes to show that we will not make use of unrecognized attributes
            "unknown.service.key",
            "unknown.operation.key",
        ]
        values: List[str] = [
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

        # Validate behaviour of various combinations of AWS remote attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            AWS_REMOTE_SERVICE,
            _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION,
            _AWS_REMOTE_OPERATION_VALUE,
            keys,
            values,
        )

        # Validate behaviour of various combinations of RPC attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.RPC_SERVICE, "RPC service", SpanAttributes.RPC_METHOD, "RPC method", keys, values
        )

        # Validate behaviour of various combinations of DB attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.DB_SYSTEM, "DB system", SpanAttributes.DB_OPERATION, "DB operation", keys, values
        )

        # Validate behaviour of various combinations of FAAS attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.FAAS_INVOKED_NAME,
            "FAAS invoked name",
            SpanAttributes.FAAS_TRIGGER,
            "FAAS trigger name",
            keys,
            values,
        )

        # Validate behaviour of various combinations of Messaging attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.MESSAGING_SYSTEM,
            "Messaging system",
            SpanAttributes.MESSAGING_OPERATION,
            "Messaging operation",
            keys,
            values,
        )

        # Validate behaviour of GraphQL operation type attribute, then remove it.
        keys, values = self._mock_attribute(
            [SpanAttributes.GRAPHQL_OPERATION_TYPE], ["GraphQL operation type"], keys, values
        )
        self._validate_expected_remote_attributes("graphql", "GraphQL operation type")
        keys, values = self._mock_attribute([SpanAttributes.GRAPHQL_OPERATION_TYPE], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.name
        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], ["www.example.com"], keys, values)
        self._validate_expected_remote_attributes("www.example.com", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.name and net.peer.port
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT],
            ["192.168.0.0", "8081"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("192.168.0.0:8081", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT], [None, None], keys, values
        )

        # Validate behaviour of extracting Remote Service from net.peer.socket.addr
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR],
            ["www.example.com"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("www.example.com", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.NET_SOCK_PEER_ADDR], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.socket.addr and net.sock.peer.port
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

        # Validate behavior of Remote Operation from HttpTarget - with 1st api part, then remove it
        keys, values = self._mock_attribute(
            [SpanAttributes.HTTP_URL], ["http://www.example.com/payment/123"], keys, values
        )
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/payment")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behavior of Remote Operation from HttpTarget - without 1st api part, then remove it
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["http://www.example.com"], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behavior of Remote Operation from HttpTarget - invalid url, then remove it
        # We designed to let Generator return a "/" rather than UNKNOWN OPERATION when receive invalid HTTP URL
        # We basically expect url to be well formed, both / or unknown is acceptable since it should never really happen
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["abc"], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of Peer service attribute, then remove it.
        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], ["Peer service"], keys, values)
        self._validate_expected_remote_attributes("Peer service", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], [None], keys, values)

        # Once we have removed all usable metrics, we only have "unknown" attributes, which are unused.
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, _UNKNOWN_REMOTE_OPERATION)

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

    def _update_resource_with_service_name(self) -> None:
        self.resource: Resource = Resource(attributes={SERVICE_NAME: _SERVICE_NAME_VALUE})

    def _mock_attribute(
        self,
        keys: List[str],
        values: Optional[List[str]],
        exist_keys: Optional[List[str]] = None,
        exist_values: Optional[List[str]] = None,
    ) -> (Optional[List[str]], Optional[List[str]]):
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

    def _validate_expected_remote_attributes(
        self, expected_remote_service: str, expected_remote_operation: str
    ) -> None:
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
        self,
        remote_service_key: str,
        remote_service_value: str,
        remote_operation_key: str,
        remote_operation_value: str,
        keys: Optional[List[str]],
        values: Optional[List[str]],
    ):
        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [None, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, None], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, _UNKNOWN_REMOTE_OPERATION)

        keys, values = self._mock_attribute([remote_service_key, remote_operation_key], [None, None], keys, values)
        return keys, values

    def _validate_peer_service_does_override(self, remote_service_key: str) -> None:
        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], ["TestString", "PeerService"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "PeerService")

        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], [None, None])

    def test_client_span_with_remote_target_attributes(self):
        # Validate behaviour of aws bucket name attribute, then remove it.
        self._mock_attribute([AWS_BUCKET_NAME], ["aws_s3_bucket_name"])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "::s3:::aws_s3_bucket_name")
        self._mock_attribute([AWS_BUCKET_NAME], [None])

        # Validate behaviour of AWS_QUEUE_NAME attribute, then remove it
        self._mock_attribute([AWS_QUEUE_NAME], ["aws_queue_name"])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "::sqs:::aws_queue_name")
        self._mock_attribute([AWS_QUEUE_NAME], [None])

        # Validate behaviour of having both AWS_QUEUE_NAME and AWS_QUEUE_URL attribute, then remove them
        self._mock_attribute(
            [AWS_QUEUE_URL, AWS_QUEUE_NAME],
            ["https://sqs.us-east-2.amazonaws.com/123456789012/Queue", "aws_queue_name"],
        )
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "arn:aws:sqs:us-east-2:123456789012:Queue")
        self._mock_attribute([AWS_QUEUE_URL, AWS_QUEUE_NAME], [None, None])

        # Valid queue name with invalid queue URL, we should default to using the queue name.
        self._mock_attribute([AWS_QUEUE_URL, AWS_QUEUE_NAME], ["invalidUrl", "aws_queue_name"])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "::sqs:::aws_queue_name")
        self._mock_attribute([AWS_QUEUE_URL, AWS_QUEUE_NAME], [None, None])

        # Validate behaviour of AWS_STREAM_NAME attribute, then remove it.
        self._mock_attribute([AWS_STREAM_NAME], ["aws_stream_name"])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "::kinesis:::stream/aws_stream_name")
        self._mock_attribute([AWS_STREAM_NAME], [None])

        # Validate behaviour of AWS_TABLE_NAME attribute, then remove it.
        self._mock_attribute([AWS_TABLE_NAME], ["aws_stream_name"])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, "::dynamodb:::table/aws_table_name")
        self._mock_attribute([AWS_TABLE_NAME], [None])

    def test_sqs_client_span_basic_urls(self):
        self._test_sqs_url(
            "https://sqs.us-east-1.amazonaws.com/123412341234/Q_Name-5", "arn:aws:sqs:us-east-1:123412341234:Q_Name-5"
        )
        self._test_sqs_url(
            "https://sqs.af-south-1.amazonaws.com/999999999999/-_ThisIsValid",
            "arn:aws:sqs:af-south-1:999999999999:-_ThisIsValid",
        )
        self._test_sqs_url(
            "http://sqs.eu-west-3.amazonaws.com/000000000000/FirstQueue",
            "arn:aws:sqs:eu-west-3:000000000000:FirstQueue",
        )
        self._test_sqs_url(
            "sqs.sa-east-1.amazonaws.com/123456781234/SecondQueue", "arn:aws:sqs:sa-east-1:123456781234:SecondQueue"
        )

    def test_sqs_client_span_us_gov_urls(self):
        self._test_sqs_url(
            "https://sqs.us-gov-east-1.amazonaws.com/123456789012/MyQueue",
            "arn:aws-us-gov:sqs:us-gov-east-1:123456789012:MyQueue",
        )
        self._test_sqs_url(
            "sqs.us-gov-west-1.amazonaws.com/112233445566/Queue", "arn:aws-us-gov:sqs:us-gov-west-1:112233445566:Queue"
        )

    def test_sqs_client_span_legacy_format_urls(self):
        self._test_sqs_url(
            "https://ap-northeast-2.queue.amazonaws.com/123456789012/MyQueue",
            "arn:aws:sqs:ap-northeast-2:123456789012:MyQueue",
        )
        self._test_sqs_url(
            "http://cn-northwest-1.queue.amazonaws.com/123456789012/MyQueue",
            "arn:aws-cn:sqs:cn-northwest-1:123456789012:MyQueue",
        )
        self._test_sqs_url(
            "http://cn-north-1.queue.amazonaws.com/123456789012/MyQueue",
            "arn:aws-cn:sqs:cn-north-1:123456789012:MyQueue",
        )
        self._test_sqs_url(
            "ap-south-1.queue.amazonaws.com/123412341234/MyLongerQueueNameHere",
            "arn:aws:sqs:ap-south-1:123412341234:MyLongerQueueNameHere",
        )
        self._test_sqs_url(
            "https://us-gov-east-1.queue.amazonaws.com/123456789012/MyQueue",
            "arn:aws-us-gov:sqs:us-gov-east-1:123456789012:MyQueue",
        )

    def test_sqs_client_span_north_virginia_legacy_url(self):
        self._test_sqs_url(
            "https://queue.amazonaws.com/123456789012/MyQueue", "arn:aws:sqs:us-east-1:123456789012:MyQueue"
        )

    def test_sqs_client_span_custom_urls(self):
        self._test_sqs_url("http://127.0.0.1:1212/123456789012/MyQueue", "::sqs::123456789012:MyQueue")
        self._test_sqs_url("https://127.0.0.1:1212/123412341234/RRR", "::sqs::123412341234:RRR")
        self._test_sqs_url("127.0.0.1:1212/123412341234/QQ", "::sqs::123412341234:QQ")
        self._test_sqs_url("https://amazon.com/123412341234/BB", "::sqs::123412341234:BB")

    def test_sqs_client_span_long_urls(self):
        queue_name = "a" * 80
        self._test_sqs_url("http://127.0.0.1:1212/123456789012/" + queue_name, "::sqs::123456789012:" + queue_name)

        queue_name_too_long = "a" * 81
        self._test_sqs_url("http://127.0.0.1:1212/123456789012/" + queue_name_too_long, None)

    def test_client_span_sqs_invalid_or_empty_urls(self):
        self._test_sqs_url(None, None)
        self._test_sqs_url("", None)
        self._test_sqs_url("invalidUrl", None)
        self._test_sqs_url("https://www.amazon.com", None)
        self._test_sqs_url("https://sqs.us-east-1.amazonaws.com/123412341234/.", None)
        self._test_sqs_url("https://sqs.us-east-1.amazonaws.com/12/Queue", None)
        self._test_sqs_url("https://sqs.us-east-1.amazonaws.com/A/A", None)
        self._test_sqs_url("https://sqs.us-east-1.amazonaws.com/123412341234/A/ThisShouldNotBeHere", None)

    def _test_sqs_url(self, sqs_url, expected_remote_target):
        self._mock_attribute([AWS_QUEUE_URL], [sqs_url])
        self._validate_remote_target_attributes(AWS_REMOTE_TARGET, expected_remote_target)
        self._mock_attribute([AWS_QUEUE_URL], [None])

    def _validate_remote_target_attributes(self, remote_target_key, remote_target) -> None:
        # Client, Producer, and Consumer spans should generate the expected RemoteTarget attribute
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        if not remote_target_key in actual_attributes:
            self.assertEqual(remote_target, None)
        else:
            self.assertEqual(actual_attributes[remote_target_key], remote_target)

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        if not remote_target_key in actual_attributes:
            self.assertEqual(remote_target, None)
        else:
            self.assertEqual(actual_attributes[remote_target_key], remote_target)

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        if not remote_target_key in actual_attributes:
            self.assertEqual(remote_target, None)
        else:
            self.assertEqual(actual_attributes[remote_target_key], remote_target)

        # Server span should not generate RemoteTarget attribute
        self.span_mock.kind = SpanKind.SERVER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            SERVICE_METRIC
        )

        self.assertNotIn(remote_target_key, actual_attributes)

    def _validate_attributes_produced_for_non_local_root_span_of_kind(
        self, expected_attributes: Attributes, kind: SpanKind
    ) -> None:
        self.span_mock.kind = kind

        attribute_map: {str, BoundedAttributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        service_attributes: BoundedAttributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: BoundedAttributes = attribute_map.get(DEPENDENCY_METRIC)
        if attribute_map is not None and len(attribute_map) > 0:
            if kind in [SpanKind.PRODUCER, SpanKind.CLIENT, SpanKind.CONSUMER]:
                self.assertIsNone(service_attributes)
                self.assertEqual(len(dependency_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(dependency_attributes, BoundedAttributes(attributes=expected_attributes))
            else:
                self.assertIsNone(dependency_attributes)
                self.assertEqual(len(service_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(service_attributes, BoundedAttributes(attributes=expected_attributes))
