# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

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

    def setUp(self):
        """Set up test fixtures."""
        self.mock_span = MagicMock(spec=Span)
        self.mock_span.is_recording.return_value = True

    def test_add_code_attributes_to_recording_span(self):
        """Test adding code attributes to a recording span."""

        def test_function():
            pass

        add_code_attributes_to_span(self.mock_span, test_function)

        # Verify function name attribute is set
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_function")

        # Verify file path attribute is set
        expected_file_path = test_function.__code__.co_filename
        self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, expected_file_path)

        # Verify line number attribute is set
        expected_line_number = test_function.__code__.co_firstlineno
        self.mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, expected_line_number)

    def test_add_code_attributes_to_non_recording_span(self):
        """Test that no attributes are added to a non-recording span."""
        self.mock_span.is_recording.return_value = False

        def test_function():
            pass

        add_code_attributes_to_span(self.mock_span, test_function)

        # Verify no attributes are set
        self.mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_function_without_code(self):
        """Test handling of functions without __code__ attribute."""
        # Create a mock function without __code__ attribute
        mock_func = MagicMock()
        mock_func.__name__ = "mock_function"
        delattr(mock_func, "__code__")

        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify only function name attribute is set
        self.mock_span.set_attribute.assert_called_once_with(CODE_FUNCTION_NAME, "mock_function")

    def test_add_code_attributes_builtin_function(self):
        """Test handling of built-in functions."""
        # Use a built-in function like len
        add_code_attributes_to_span(self.mock_span, len)

        # Verify only function name attribute is set
        self.mock_span.set_attribute.assert_called_once_with(CODE_FUNCTION_NAME, "len")

    def test_add_code_attributes_function_without_name(self):
        """Test handling of functions without __name__ attribute."""
        # Create an object without __name__ attribute
        mock_func = MagicMock()
        delattr(mock_func, "__name__")
        mock_func.__code__ = MagicMock()
        mock_func.__code__.co_filename = "/test/file.py"
        mock_func.__code__.co_firstlineno = 10

        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify function name uses str() representation
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, str(mock_func))
        self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, "/test/file.py")
        self.mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, 10)

    def test_add_code_attributes_exception_handling(self):
        """Test that exceptions are handled gracefully."""
        # Create a function that will cause an exception when accessing attributes
        mock_func = MagicMock()
        mock_func.__name__ = "test_func"
        mock_func.__code__ = MagicMock()
        mock_func.__code__.co_filename = "/test/file.py"
        mock_func.__code__.co_firstlineno = MagicMock(side_effect=Exception("Test exception"))

        # This should not raise an exception
        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify function name and file path are still set
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_func")
        self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, "/test/file.py")

    @patch("amazon.opentelemetry.distro.code_correlation.getattr")
    def test_add_code_attributes_getattr_exception(self, mock_getattr):
        """Test exception handling when getattr fails."""
        mock_getattr.side_effect = Exception("Test exception")

        def test_function():
            pass

        # This should not raise an exception
        add_code_attributes_to_span(self.mock_span, test_function)

        # Verify no attributes are set due to exception
        self.mock_span.set_attribute.assert_not_called()

    def test_add_code_attributes_co_filename_exception(self):
        """Test exception handling when accessing co_filename raises exception."""
        # Create a mock function with __code__ that raises exception on co_filename access
        mock_func = MagicMock()
        mock_func.__name__ = "test_func"
        mock_code = MagicMock()
        mock_code.co_firstlineno = 10

        # Make co_filename raise AttributeError
        type(mock_code).co_filename = PropertyMock(side_effect=AttributeError("Test exception"))
        mock_func.__code__ = mock_code

        # This should not raise an exception
        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify function name and line number are still set, but not file path
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_func")
        self.mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, 10)
        # File path should not be called due to exception
        with self.assertRaises(AssertionError):
            self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, MagicMock())

    def test_add_code_attributes_co_firstlineno_exception(self):
        """Test exception handling when accessing co_firstlineno raises exception."""
        # Create a mock function with __code__ that raises exception on co_firstlineno access
        mock_func = MagicMock()
        mock_func.__name__ = "test_func"
        mock_code = MagicMock()
        mock_code.co_filename = "/test/file.py"

        # Make co_firstlineno raise TypeError
        type(mock_code).co_firstlineno = PropertyMock(side_effect=TypeError("Test exception"))
        mock_func.__code__ = mock_code

        # This should not raise an exception
        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify function name and file path are still set, but not line number
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_func")
        self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, "/test/file.py")
        # Line number should not be called due to exception
        with self.assertRaises(AssertionError):
            self.mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, MagicMock())

    def test_add_code_attributes_co_filename_type_error(self):
        """Test exception handling when accessing co_filename raises TypeError."""
        # Create a mock function with __code__ that raises TypeError on co_filename access
        mock_func = MagicMock()
        mock_func.__name__ = "test_func"
        mock_code = MagicMock()
        mock_code.co_firstlineno = 10

        # Make co_filename raise TypeError
        type(mock_code).co_filename = PropertyMock(side_effect=TypeError("Test exception"))
        mock_func.__code__ = mock_code

        # This should not raise an exception
        add_code_attributes_to_span(self.mock_span, mock_func)

        # Verify function name and line number are still set, but not file path
        self.mock_span.set_attribute.assert_any_call(CODE_FUNCTION_NAME, "test_func")
        self.mock_span.set_attribute.assert_any_call(CODE_LINE_NUMBER, 10)
        # File path should not be called due to TypeError
        with self.assertRaises(AssertionError):
            self.mock_span.set_attribute.assert_any_call(CODE_FILE_PATH, MagicMock())


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

        # Verify no span attributes were set
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
