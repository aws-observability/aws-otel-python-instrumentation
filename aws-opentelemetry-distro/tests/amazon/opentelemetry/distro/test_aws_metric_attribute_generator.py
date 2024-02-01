# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.util.types import Attributes

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_SPAN_KIND, AWS_LOCAL_SERVICE, AWS_LOCAL_OPERATION, \
    AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE, SERVICE_NAME

from amazon.opentelemetry.distro._aws_span_processing_util import UNKNOWN_SERVICE, UNKNOWN_OPERATION, \
    UNKNOWN_REMOTE_OPERATION, UNKNOWN_REMOTE_SERVICE, LOCAL_ROOT, INTERNAL_OPERATION
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator, SERVICE_METRIC, \
    DEPENDENCY_METRIC

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


class ThrowableWithMethodGetStatusCode(Exception):
    """
    A custom exception class that includes an HTTP status code.
    """

    def __init__(self, http_status_code: int) -> None:
        """
        Initialize the exception with an HTTP status code.

        Args:
        http_status_code (int): The HTTP status code associated with this exception.
        """
        super().__init__()
        self._http_status_code: int = http_status_code

    def get_status_code(self) -> int:
        """
        Return the HTTP status code associated with this exception.

        Returns:
        int: The HTTP status code.
        """
        return self._http_status_code


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
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_consumer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_without_attributes(self):
        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_producer_span_without_attributes(self):
        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_without_attributes(self):
        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    def test_internal_span(self):
        # Spans with internal span kind should not produce any attributes.
        self._validate_attributes_produced_for_non_local_root_span_of_kind({}, SpanKind.INTERNAL)

    def test_local_root_server_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE
            }
        }

        self.span_mock.kind = SpanKind.SERVER
        actual_attributes_map = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_internal_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION
            }
        }

        self.span_mock.kind = SpanKind.INTERNAL
        actual_attributes_map = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_client_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute([AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE])

        expected_attributes_map = {
            SERVICE_METRIC: BoundedAttributes(attributes={
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION
            }, maxlen=None),
            DEPENDENCY_METRIC: BoundedAttributes(attributes={
                AWS_SPAN_KIND: SpanKind.CLIENT.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE
            }, maxlen=None)
        }

        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_consumer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute([AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE])

        expected_attributes_map = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CONSUMER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE
            }
        }

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def _validate_attributes_produced_for_non_local_root_span_of_kind(self, expected_attributes: Attributes, kind: SpanKind):
        self.span_mock._kind = kind
        self.span_mock.kind = kind

        attribute_map: {str, Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
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
        self._mock_attribute([AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE])

        expected_attributes_map = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.PRODUCER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE
            }
        }

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes_map: BoundedAttributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_consumer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_null_span_name(self):
        self._update_resource_with_service_name()
        self.span_mock.name = None

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_span_name_as_http_method(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "GET"
        self._mock_attribute([SpanAttributes.HTTP_METHOD], ["GET"])

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)

    def test_server_span_with_span_name_with_http_target(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_TARGET], ["POST", "/payment/123"])

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /payment"
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_TARGET, None)

    def test_producer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute([AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE])

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute([AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE])

        expected_attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)


    def _update_resource_with_service_name(self):
        self.resource: Resource = Resource(attributes={SERVICE_NAME: _SERVICE_NAME_VALUE})

    def _mock_attribute(self, keys: [str], values: [Optional[str]]):
        def get_side_effect(get_key):
            if get_key in keys:
                return values[keys.index(get_key)]
            return None

        self.attributes_mock.get.side_effect = get_side_effect

    def _validate_expected_remote_attributes(self, expected_remote_service, expected_remote_operation):
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

    def _validate_and_remove_remote_attributes(self, remote_service_key, remote_service_value, remote_operation_key, remote_operation_value):
        self._mock_attribute([remote_service_key, remote_operation_key], [remote_service_value, remote_operation_value])
        self._validate_expected_remote_attributes(remote_service_value, remote_operation_value)

        self._mock_attribute([remote_service_key, remote_operation_key], [None, remote_operation_value])
        self._validate_expected_remote_attributes(UNKNOWN_REMOTE_SERVICE, remote_operation_value)

        self._mock_attribute([remote_service_key, remote_operation_key], [remote_service_value, None])
        self._validate_expected_remote_attributes(remote_service_value, UNKNOWN_REMOTE_OPERATION)

        self._mock_attribute([remote_service_key, None], [remote_service_value, None])

