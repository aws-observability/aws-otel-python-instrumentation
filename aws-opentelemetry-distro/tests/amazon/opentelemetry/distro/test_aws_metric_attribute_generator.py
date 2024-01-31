# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.util.types import Attributes

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_SPAN_KIND, AWS_LOCAL_SERVICE, AWS_LOCAL_OPERATION, \
    AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE

from amazon.opentelemetry.distro._aws_span_processing_util import UNKNOWN_SERVICE, UNKNOWN_OPERATION, \
    UNKNOWN_REMOTE_OPERATION, UNKNOWN_REMOTE_SERVICE
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
            AWS_SPAN_KIND: 'SERVER',
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_consumer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: 'CONSUMER',
            AWS_LOCAL_SERVICE: UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: UNKNOWN_REMOTE_OPERATION
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)


    def _validate_attributes_produced_for_non_local_root_span_of_kind(self, expected_attributes: Attributes, kind: SpanKind):
        self.span_mock._kind = kind
        self.span_mock.kind = kind

        attribute_map: {str, Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource)
        service_attributes: BoundedAttributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: BoundedAttributes = attribute_map.get(DEPENDENCY_METRIC)
        if attribute_map is not None:
            if kind == SpanKind.PRODUCER or kind == SpanKind.CLIENT or kind == SpanKind.CONSUMER:
                self.assertIsNone(service_attributes)
                self.assertEqual(len(dependency_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(dependency_attributes, BoundedAttributes(attributes=expected_attributes))
            else:
                self.assertIsNone(dependency_attributes)
                self.assertEqual(len(service_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(service_attributes, BoundedAttributes(attributes=expected_attributes))

