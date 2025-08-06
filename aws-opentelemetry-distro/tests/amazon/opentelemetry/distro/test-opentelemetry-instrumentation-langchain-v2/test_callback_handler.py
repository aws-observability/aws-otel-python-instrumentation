# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler import (
    OpenTelemetryCallbackHandler,
    SpanHolder,
    _sanitize_metadata_value,
    _set_request_params,
    _set_span_attribute,
)
from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.span_attributes import SpanAttributes
from opentelemetry.trace import SpanKind


class TestOpenTelemetryCallbackHandler(unittest.TestCase):
    def setUp(self):
        self.mock_tracer = MagicMock()
        self.mock_span = MagicMock()
        self.mock_tracer.start_span.return_value = self.mock_span
        self.handler = OpenTelemetryCallbackHandler(self.mock_tracer)
        self.run_id = uuid.uuid4()
        self.parent_run_id = uuid.uuid4()

    def test_set_span_attribute(self):
        """Test the _set_span_attribute function with various inputs."""
        # Value is not None
        _set_span_attribute(self.mock_span, "test.attribute", "test_value")
        self.mock_span.set_attribute.assert_called_with("test.attribute", "test_value")

        # Value is None
        self.mock_span.reset_mock()
        _set_span_attribute(self.mock_span, "test.attribute", None)
        self.mock_span.set_attribute.assert_not_called()

        # Value is empty string
        self.mock_span.reset_mock()
        _set_span_attribute(self.mock_span, "test.attribute", "")
        self.mock_span.set_attribute.assert_not_called()

        # Value is number
        self.mock_span.reset_mock()
        _set_span_attribute(self.mock_span, "test.attribute", 123)
        self.mock_span.set_attribute.assert_called_with("test.attribute", 123)

    def test_sanitize_metadata_value(self):
        """Test _sanitize_metadata_value function with various inputs."""
        # Basic types
        self.assertEqual(_sanitize_metadata_value(None), None)
        self.assertEqual(_sanitize_metadata_value("string"), "string")
        self.assertEqual(_sanitize_metadata_value(123), 123)
        self.assertEqual(_sanitize_metadata_value(123.45), 123.45)
        self.assertEqual(_sanitize_metadata_value(True), True)

        # List type
        self.assertEqual(_sanitize_metadata_value([1, 2, 3]), ["1", "2", "3"])
        self.assertEqual(_sanitize_metadata_value(["a", "b", "c"]), ["a", "b", "c"])

        # Complex object
        class TestClass:
            def __str__(self):
                return "TestClass"

        self.assertEqual(_sanitize_metadata_value(TestClass()), "TestClass")

        # Nested list
        self.assertEqual(_sanitize_metadata_value([1, [2, 3], 4]), ["1", "['2', '3']", "4"])

    @patch("time.time", return_value=12345.0)
    def test_set_request_params(self, mock_time):
        """Test _set_request_params function."""
        span = MagicMock()

        # Create SpanHolder manually with fields to avoid factory issue
        span_holder = SpanHolder(span=span, children=[], start_time=12345.0, request_model=None)

        # Test with model_id in kwargs
        kwargs = {"model_id": "gpt-4", "temperature": 0.7, "max_tokens": 100, "top_p": 0.9}
        _set_request_params(span, kwargs, span_holder)

        self.assertEqual(span_holder.request_model, "gpt-4")

        # Verify the appropriate attributes were set
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_REQUEST_MODEL, "gpt-4")
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_RESPONSE_MODEL, "gpt-4")
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_REQUEST_TEMPERATURE, 0.7)
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_REQUEST_MAX_TOKENS, 100)
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_REQUEST_TOP_P, 0.9)

        # Test with invocation_params
        span.reset_mock()
        span_holder = SpanHolder(span=span, children=[], start_time=12345.0, request_model=None)

        kwargs = {"invocation_params": {"model_id": "claude-3", "temperature": 0.5, "max_tokens": 200, "top_p": 0.8}}
        _set_request_params(span, kwargs, span_holder)

        self.assertEqual(span_holder.request_model, "claude-3")
        span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_REQUEST_MODEL, "claude-3")

    def test_create_span(self):
        """Test _create_span method."""
        # Test creating span without parent
        with patch("time.time", return_value=12345.0):
            span = self.handler._create_span(self.run_id, None, "test_span", metadata={"key": "value"})

        self.assertEqual(span, self.mock_span)
        # Fix: Use SpanKind.INTERNAL instead of the integer value
        self.mock_tracer.start_span.assert_called_with("test_span", kind=SpanKind.INTERNAL)
        self.assertIn(self.run_id, self.handler.span_mapping)
        self.assertEqual(self.handler.span_mapping[self.run_id].span, self.mock_span)
        self.assertEqual(self.handler.span_mapping[self.run_id].children, [])

        # Test creating span with parent
        with patch("time.time", return_value=12345.0):
            parent_run_id = uuid.uuid4()
            parent_span = MagicMock()
            self.handler.span_mapping[parent_run_id] = SpanHolder(
                span=parent_span, children=[], start_time=12345.0, request_model=None
            )

            span = self.handler._create_span(self.run_id, parent_run_id, "child_span")

            self.assertEqual(len(self.handler.span_mapping[parent_run_id].children), 1)
            self.assertEqual(self.handler.span_mapping[parent_run_id].children[0], self.run_id)

    def test_get_name_from_callback(self):
        """Test _get_name_from_callback method."""
        # Test with name in kwargs
        serialized = {"kwargs": {"name": "test_name"}}
        name = self.handler._get_name_from_callback(serialized)
        self.assertEqual(name, "test_name")

        # Test with name in direct kwargs
        name = self.handler._get_name_from_callback({}, kwargs={"name": "direct_name"})
        self.assertEqual(name, "unknown")

        # Test with name in serialized
        name = self.handler
