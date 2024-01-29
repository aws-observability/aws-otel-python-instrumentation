# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.util.types import Attributes

from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.trace import SpanContext
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE

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
    def test_basic(self):
        self.attributes_mock: Attributes = MagicMock()
        self.instrumentation_scope_info_mock: InstrumentationScope = MagicMock()
        self.instrumentation_scope_info_mock.name = "Scope name"
        self.span_mock: ReadableSpan = MagicMock()
        self.span_mock.attributes = self.attributes_mock
        self.span_mock.instrumentation_scope = self.instrumentation_scope_info_mock
        self.span_mock.get_span_context.return_value = MagicMock()
        self.parent_span_context: SpanContext = MagicMock()
        self.parent_span_context.is_valid = True
        self.parent_span_context.is_remote = False
        self.span_mock.parent = self.parent_span_context

        self.resource: Resource = _DEFAULT_RESOURCE

    def test_span_attributes_for_empty_resource(self):
        resource = Resource.get_empty()
        expected_attributes = {

        }
