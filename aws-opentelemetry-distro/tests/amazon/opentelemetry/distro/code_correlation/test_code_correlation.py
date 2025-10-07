# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.code_correlation import (
    CODE_FILE_PATH,
    CODE_FUNCTION_NAME,
    CODE_LINE_NUMBER,
    add_code_attributes_to_span,
    record_code_attributes,
)
from opentelemetry.trace import Span


class TestCodeCorrelationConstants(TestCase):
    """Test code correlation attribute constants."""

    def test_constants_values(self):
        """Test that constants have the expected values."""
        self.assertEqual(CODE_FUNCTION_NAME, "code.function.name")
        self.assertEqual(CODE_FILE_PATH, "code.file.path")
        self.assertEqual(CODE_LINE_NUMBER, "code.line.number")


class TestAddCodeAttributesToSpan(TestCase):
    """Test the add_code_attributes_to_span function."""

    def test_add_code_attributes_to_recording_span_with_function(self):
        """Test adding code attributes to a recording span with a regular function."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        def test_function():
            pass

        add_code_attributes_to_span(mock_span, test_function)

        # Verify function name attribute is set
        mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_function")

        # Verify file path attribute is set
        expected_file_path = test_function.__code__.co_filename
        mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, expected_file_path)

        # Verify line number attribute is set
        expected_line_number = test_function.__code__.co_firstlineno
        mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, expected_line_number)

    def test_add_code_attributes_to_recording_span_with_class(self):
        """Test adding code attributes to a recording span with a class."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        class TestClass:
            pass

        add_code_attributes_to_span(mock_span, TestClass)

        # Verify class name attribute is set
        mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "TestClass")

        # Verify file path attribute is set (classes have file paths too)
        mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, __file__)

    def test_add_code_attributes_to_non_recording_span(self):
        """Test that no attributes are added to a non-recording span."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = False

        def test_function():
            pass

        add_code_attributes_to_span(mock_span, test_function)

        # Verify no attributes are set
        mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_function_without_code(self):
        """Test handling of functions without __code__ attribute."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        # Create a mock function without __code__ attribute
        mock_func = MagicMock()
        mock_func.__name__ = "mock_function"
        delattr(mock_func, "__code__")

        add_code_attributes_to_span(mock_span, mock_func)

        # Functions without __code__ attribute don't get any attributes set
        mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_builtin_function(self):
        """Test handling of built-in functions."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        # Use a built-in function like len
        add_code_attributes_to_span(mock_span, len)

        # Built-in functions don't have __code__ attribute, so no attributes are set
        mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_exception_handling(self):
        """Test that exceptions are handled gracefully."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        # Create a function that will cause an exception when accessing __name__
        mock_func = MagicMock()
        mock_func.__name__ = MagicMock(side_effect=Exception("Test exception"))

        # This should not raise an exception
        add_code_attributes_to_span(mock_span, mock_func)

        # No attributes should be set due to exception
        mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_inspect_isclass_exception(self):
        """Test exception handling when inspect.isclass raises an exception."""
        # Create independent mock_span for this test
        mock_span = MagicMock(spec=Span)
        mock_span.is_recording.return_value = True

        # Create a mock object that will cause inspect.isclass to raise an exception
        with patch("amazon.opentelemetry.distro.code_correlation.inspect.isclass") as mock_isclass:
            mock_isclass.side_effect = Exception("Test exception")

            def test_function():
                pass

            # This should not raise an exception
            add_code_attributes_to_span(mock_span, test_function)

            # No attributes should be set due to exception
            mock_span.set_attribute.assert_not_called()


class TestRecordCodeAttributesDecorator(TestCase):
    """Test the record_code_attributes decorator."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_span = MagicMock(spec=Span)
        self.mock_span.is_recording.return_value = True

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_sync_function(self, mock_get_current_span):
        """Test decorator with synchronous function."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        def test_sync_function(arg1, arg2=None):
            return f"sync result: {arg1}, {arg2}"

        # Call the decorated function
        result = test_sync_function("test_arg", arg2="test_kwarg")

        # Verify the function still works correctly
        self.assertEqual(result, "sync result: test_arg, test_kwarg")

        # Verify span attributes were set
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_sync_function")

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_async_function(self, mock_get_current_span):
        """Test decorator with asynchronous function."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        async def test_async_function(arg1, arg2=None):
            return f"async result: {arg1}, {arg2}"

        # Call the decorated async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_async_function("test_arg", arg2="test_kwarg"))
        finally:
            loop.close()

        # Verify the function still works correctly
        self.assertEqual(result, "async result: test_arg, test_kwarg")

        # Verify span attributes were set
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_async_function")

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_no_current_span(self, mock_get_current_span):
        """Test decorator when there's no current span."""
        mock_get_current_span.return_value = None

        @record_code_attributes
        def test_function():
            return "test result"

        # Call the decorated function
        result = test_function()

        # Verify the function still works correctly
        self.assertEqual(result, "test result")

        # Verify no span attributes were set since there's no span
        self.mock_span.set_attribute.assert_not_called()

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_exception_handling(self, mock_get_current_span):
        """Test decorator handles exceptions gracefully."""
        mock_get_current_span.side_effect = Exception("Test exception")

        @record_code_attributes
        def test_function():
            return "test result"

        # Call the decorated function - should not raise exception
        result = test_function()

        # Verify the function still works correctly
        self.assertEqual(result, "test result")

    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves original function metadata."""

        @record_code_attributes
        def test_function():
            """Test function docstring."""
            return "test result"

        # Verify function metadata is preserved
        self.assertEqual(test_function.__name__, "test_function")
        self.assertEqual(test_function.__doc__, "Test function docstring.")

    def test_async_function_detection(self):
        """Test that async functions are properly detected."""

        # Create a regular function
        def sync_func():
            pass

        # Create an async function
        async def async_func():
            pass

        # Apply decorator to both
        decorated_sync = record_code_attributes(sync_func)
        decorated_async = record_code_attributes(async_func)

        # Check that sync function returns a regular function
        self.assertFalse(asyncio.iscoroutinefunction(decorated_sync))

        # Check that async function returns a coroutine function
        self.assertTrue(asyncio.iscoroutinefunction(decorated_async))

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_with_function_that_raises_exception(self, mock_get_current_span):
        """Test decorator with function that raises exception."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        def test_function():
            raise ValueError("Test function exception")

        # Verify exception is still raised
        with self.assertRaises(ValueError):
            test_function()

        # Verify span attributes were still set before exception
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_function")

    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_with_async_function_that_raises_exception(self, mock_get_current_span):
        """Test decorator with async function that raises exception."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        async def test_async_function():
            raise ValueError("Test async function exception")

        # Verify exception is still raised
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with self.assertRaises(ValueError):
                loop.run_until_complete(test_async_function())
        finally:
            loop.close()

        # Verify span attributes were still set before exception
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_async_function")

    @patch("amazon.opentelemetry.distro.code_correlation.add_code_attributes_to_span")
    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_internal_exception_handling_sync(self, mock_get_current_span, mock_add_attributes):
        """Test that decorator handles internal exceptions gracefully in sync function."""
        mock_get_current_span.return_value = self.mock_span
        # Make add_code_attributes_to_span raise an exception
        mock_add_attributes.side_effect = Exception("Internal exception")

        @record_code_attributes
        def test_function():
            return "test result"

        # Call the decorated function - should not raise exception
        result = test_function()

        # Verify the function still works correctly despite internal exception
        self.assertEqual(result, "test result")

    @patch("amazon.opentelemetry.distro.code_correlation.add_code_attributes_to_span")
    @patch("amazon.opentelemetry.distro.code_correlation.trace.get_current_span")
    def test_decorator_internal_exception_handling_async(self, mock_get_current_span, mock_add_attributes):
        """Test that decorator handles internal exceptions gracefully in async function."""
        mock_get_current_span.return_value = self.mock_span
        # Make add_code_attributes_to_span raise an exception
        mock_add_attributes.side_effect = Exception("Internal exception")

        @record_code_attributes
        async def test_async_function():
            return "async test result"

        # Call the decorated async function - should not raise exception
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_async_function())
        finally:
            loop.close()

        # Verify the function still works correctly despite internal exception
        self.assertEqual(result, "async test result")
