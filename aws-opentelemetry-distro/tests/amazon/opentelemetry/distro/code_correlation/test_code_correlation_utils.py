# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import functools
import inspect
import types
from types import FrameType
from unittest import TestCase
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.code_correlation.utils import (
    _construct_qualified_name,
    add_code_attributes_to_span,
    add_code_attributes_to_span_from_frame,
    get_callable_fullname,
    get_function_fullname_from_frame,
    record_code_attributes,
)
from opentelemetry.semconv.attributes.code_attributes import (  # noqa: F401
    CODE_FILE_PATH,
    CODE_FUNCTION_NAME,
    CODE_LINE_NUMBER,
)


# Test classes and functions for testing
class TestClass:
    """Test class for testing callable name extraction."""

    def instance_method(self):
        """Instance method for testing."""
        pass

    @classmethod
    def class_method(cls):
        """Class method for testing."""
        pass

    @staticmethod
    def static_method():
        """Static method for testing."""
        pass


def test_function():
    """Test function for testing."""
    pass


def test_function_with_locals():
    """Test function that creates a frame with locals."""
    local_var = "test"  # noqa: F841
    return inspect.currentframe()


class TestGetCallableFullname(TestCase):
    """Test the get_callable_fullname function."""

    def test_regular_function(self):
        """Test with regular Python function."""
        result = get_callable_fullname(test_function)
        expected = f"{__name__}.test_function"
        self.assertEqual(result, expected)

    def test_builtin_function(self):
        """Test with built-in function."""
        result = get_callable_fullname(len)
        self.assertEqual(result, "builtins.len")

    def test_lambda_function(self):
        """Test with lambda function."""

        def lambda_func(x):
            return x + 1

        # Properly simulate a lambda function by setting both __name__ and __qualname__
        lambda_func.__name__ = "<lambda>"
        lambda_func.__qualname__ = "<lambda>"
        result = get_callable_fullname(lambda_func)
        # Lambda functions include the full qualname which includes the test method
        self.assertTrue(result.endswith("<lambda>"))
        self.assertTrue(result.startswith(__name__))

    def test_class(self):
        """Test with class."""
        result = get_callable_fullname(TestClass)
        expected = f"{__name__}.TestClass"
        self.assertEqual(result, expected)

    def test_instance_method_unbound(self):
        """Test with unbound instance method."""
        result = get_callable_fullname(TestClass.instance_method)
        expected = f"{__name__}.TestClass.instance_method"
        self.assertEqual(result, expected)

    def test_instance_method_bound(self):
        """Test with bound instance method."""
        instance = TestClass()
        result = get_callable_fullname(instance.instance_method)
        expected = f"{__name__}.TestClass.instance_method"
        self.assertEqual(result, expected)

    def test_class_method(self):
        """Test with class method."""
        result = get_callable_fullname(TestClass.class_method)
        expected = f"{__name__}.TestClass.class_method"
        self.assertEqual(result, expected)

    def test_static_method(self):
        """Test with static method."""
        result = get_callable_fullname(TestClass.static_method)
        expected = f"{__name__}.TestClass.static_method"
        self.assertEqual(result, expected)

    def test_functools_partial(self):
        """Test with functools.partial object."""
        partial_func = functools.partial(test_function)
        result = get_callable_fullname(partial_func)
        expected = f"{__name__}.test_function"
        self.assertEqual(result, expected)

    def test_nested_partial(self):
        """Test with nested functools.partial objects."""
        partial_func = functools.partial(functools.partial(test_function))
        result = get_callable_fullname(partial_func)
        expected = f"{__name__}.test_function"
        self.assertEqual(result, expected)

    def test_callable_object_with_module_and_name(self):
        """Test with callable object that has __module__ and __name__."""
        mock_callable = Mock()
        mock_callable.__module__ = "test.module"
        mock_callable.__name__ = "test_callable"
        mock_callable.__qualname__ = "test_callable"

        result = get_callable_fullname(mock_callable)
        self.assertEqual(result, "test.module.test_callable")

    def test_callable_object_with_qualname(self):
        """Test with callable object that has __qualname__ but no __name__."""
        mock_callable = Mock()
        mock_callable.__module__ = "test.module"
        mock_callable.__qualname__ = "TestClass.method"
        # Remove __name__ attribute
        if hasattr(mock_callable, "__name__"):
            del mock_callable.__name__

        result = get_callable_fullname(mock_callable)
        self.assertEqual(result, "test.module.TestClass.method")

    def test_callable_without_module(self):
        """Test with callable that has no __module__ attribute."""
        mock_callable = Mock()
        mock_callable.__name__ = "test_callable"
        # Mock objects have persistent __module__ attribute, so we need to set it to None
        mock_callable.__module__ = None

        result = get_callable_fullname(mock_callable)
        # When module is None, _construct_qualified_name just returns the function name
        self.assertEqual(result, "test_callable")

    def test_callable_without_name_or_qualname(self):
        """Test with callable that has no __name__ or __qualname__."""
        mock_callable = Mock()
        mock_callable.__module__ = "test.module"
        # Remove __name__ and __qualname__ attributes
        if hasattr(mock_callable, "__name__"):
            del mock_callable.__name__
        if hasattr(mock_callable, "__qualname__"):
            del mock_callable.__qualname__

        result = get_callable_fullname(mock_callable)
        # Should return repr of the object as fallback
        self.assertTrue(result.startswith("<"))

    def test_exception_handling(self):
        """Test exception handling in get_callable_fullname."""
        # Create a mock object that raises exception when accessing attributes
        mock_callable = Mock()
        mock_callable.__module__ = Mock(side_effect=Exception("Test exception"))

        result = get_callable_fullname(mock_callable)
        # Should return repr as fallback when exceptions occur
        self.assertTrue(result.startswith("<Mock"))

    def test_object_without_callable_attributes(self):
        """Test with object that doesn't have typical callable attributes."""
        # Test with a simple object that's not really callable
        obj = object()
        result = get_callable_fullname(obj)
        # Should return repr as fallback
        self.assertTrue(result.startswith("<"))

    def test_method_type_without_self(self):
        """Test with MethodType that has no __self__ attribute."""
        # Create a mock MethodType
        mock_method = Mock(spec=types.MethodType)
        del mock_method.__self__  # Remove __self__ attribute
        mock_method.__func__ = Mock()
        mock_method.__func__.__module__ = "test.module"
        mock_method.__func__.__name__ = "test_method"

        result = get_callable_fullname(mock_method)
        # Mock MethodType will fall back to repr when it doesn't match expected patterns
        self.assertTrue(result.startswith("<Mock"))


class TestGetFunctionFullnameFromFrame(TestCase):
    """Test the get_function_fullname_from_frame function."""

    def test_regular_function_frame(self):
        """Test with frame from regular function."""

        def test_func():
            return inspect.currentframe()

        frame = test_func()
        result = get_function_fullname_from_frame(frame)
        expected = f"{__name__}.test_func"
        self.assertEqual(result, expected)

    def test_method_frame_with_self(self):
        """Test with frame from instance method."""

        class TestClassForFrame:
            def test_method(self):
                return inspect.currentframe()

        instance = TestClassForFrame()
        frame = instance.test_method()
        result = get_function_fullname_from_frame(frame)
        expected = f"{__name__}.TestClassForFrame.test_method"
        self.assertEqual(result, expected)

    def test_classmethod_frame_with_cls(self):
        """Test with frame from class method."""

        class TestClassForFrame:
            @classmethod
            def test_classmethod(cls):
                return inspect.currentframe()

        frame = TestClassForFrame.test_classmethod()
        result = get_function_fullname_from_frame(frame)
        expected = f"{__name__}.TestClassForFrame.test_classmethod"
        self.assertEqual(result, expected)

    def test_function_without_module_name(self):
        """Test with frame that has no __name__ in globals."""
        # Create a mock frame
        mock_frame = Mock(spec=FrameType)
        mock_frame.f_code = Mock()
        mock_frame.f_code.co_name = "test_function"
        mock_frame.f_globals = {}  # No __name__ key
        mock_frame.f_locals = {}

        result = get_function_fullname_from_frame(mock_frame)
        # When __name__ is missing, get() returns None, _construct_qualified_name handles it
        expected = "test_function"
        self.assertEqual(result, expected)

    def test_frame_with_invalid_self(self):
        """Test with frame that has 'self' but invalid class info."""
        mock_frame = Mock(spec=FrameType)
        mock_frame.f_code = Mock()
        mock_frame.f_code.co_name = "test_method"
        mock_frame.f_globals = {"__name__": "test.module"}
        # 'self' without proper __class__ attribute - Mock still has __class__
        mock_self = Mock()
        mock_frame.f_locals = {"self": mock_self}

        result = get_function_fullname_from_frame(mock_frame)
        # Mock objects have __class__ attribute pointing to Mock, so it gets used
        expected = "test.module.Mock.test_method"
        self.assertEqual(result, expected)

    def test_frame_with_invalid_cls(self):
        """Test with frame that has 'cls' but invalid class info."""
        mock_frame = Mock(spec=FrameType)
        mock_frame.f_code = Mock()
        mock_frame.f_code.co_name = "test_classmethod"
        mock_frame.f_globals = {"__name__": "test.module"}
        # 'cls' without proper __name__ attribute
        mock_cls = Mock()
        del mock_cls.__name__  # Remove __name__ attribute
        mock_frame.f_locals = {"cls": mock_cls}

        result = get_function_fullname_from_frame(mock_frame)
        expected = "test.module.test_classmethod"
        self.assertEqual(result, expected)

    def test_exception_handling(self):
        """Test exception handling in get_function_fullname_from_frame."""
        mock_frame = Mock(spec=FrameType)
        mock_frame.f_code = Mock()
        mock_frame.f_code.co_name = "test_function"
        # Make f_globals raise an exception
        mock_frame.f_globals = Mock(side_effect=Exception("Test exception"))

        result = get_function_fullname_from_frame(mock_frame)
        self.assertEqual(result, "test_function")

    def test_staticmethod_frame(self):
        """Test with frame from static method."""

        class TestClassForFrame:
            @staticmethod
            def test_staticmethod():
                return inspect.currentframe()

        frame = TestClassForFrame.test_staticmethod()
        result = get_function_fullname_from_frame(frame)
        expected = f"{__name__}.test_staticmethod"
        self.assertEqual(result, expected)


class TestAddCodeAttributesToSpanFromFrame(TestCase):
    """Test the add_code_attributes_to_span_from_frame function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_span = Mock()
        self.mock_span.is_recording.return_value = True

    def test_add_attributes_from_frame(self):
        """Test adding attributes from a valid frame."""

        def test_func():
            frame = inspect.currentframe()
            add_code_attributes_to_span_from_frame(frame, self.mock_span)
            return frame

        test_func()

        # Check that set_attribute was called with expected arguments
        actual_calls = self.mock_span.set_attribute.call_args_list
        self.assertEqual(len(actual_calls), 3)

        # Extract just the arguments from the calls for comparison
        actual_args = [(call[0][0], call[0][1]) for call in actual_calls]

        # Check function name
        self.assertEqual(actual_args[0][0], CODE_FUNCTION_NAME)
        # Function name includes the full qualname including the test class
        self.assertTrue(actual_args[0][1].endswith("test_func"))
        self.assertTrue(actual_args[0][1].startswith(__name__))

        # Check file path
        self.assertEqual(actual_args[1][0], CODE_FILE_PATH)
        self.assertTrue(actual_args[1][1].endswith("test_code_correlation_utils.py"))

        # Check line number
        self.assertEqual(actual_args[2][0], CODE_LINE_NUMBER)
        self.assertIsInstance(actual_args[2][1], int)

    def test_span_not_recording(self):
        """Test with span that is not recording."""
        self.mock_span.is_recording.return_value = False

        def test_func():
            frame = inspect.currentframe()
            add_code_attributes_to_span_from_frame(frame, self.mock_span)

        test_func()

        # Verify no attributes were set
        self.mock_span.set_attribute.assert_not_called()

    def test_exception_handling(self):
        """Test exception handling when setting attributes."""
        # Make set_attribute raise an exception
        self.mock_span.set_attribute.side_effect = Exception("Test exception")

        def test_func():
            frame = inspect.currentframe()
            # Should not raise exception
            add_code_attributes_to_span_from_frame(frame, self.mock_span)

        test_func()  # Should complete without raising


class TestAddCodeAttributesToSpan(TestCase):
    """Test the add_code_attributes_to_span function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_span = Mock()
        self.mock_span.is_recording.return_value = True

    def test_add_attributes_from_function(self):
        """Test adding attributes from a function."""
        add_code_attributes_to_span(self.mock_span, test_function)

        # Verify span attributes were set
        calls = self.mock_span.set_attribute.call_args_list
        self.assertGreaterEqual(len(calls), 2)  # At least function name and file path

        # Check function name was set
        function_name_call = next((call for call in calls if call[0][0] == CODE_FUNCTION_NAME), None)
        self.assertIsNotNone(function_name_call)
        self.assertEqual(function_name_call[0][1], f"{__name__}.test_function")

    def test_add_attributes_from_class(self):
        """Test adding attributes from a class."""
        add_code_attributes_to_span(self.mock_span, TestClass)

        # Verify function name attribute was set
        function_name_call = None
        for call in self.mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_FUNCTION_NAME:
                function_name_call = call
                break

        self.assertIsNotNone(function_name_call)
        self.assertEqual(function_name_call[0][1], f"{__name__}.TestClass")

    def test_add_attributes_from_builtin(self):
        """Test adding attributes from a built-in function."""
        add_code_attributes_to_span(self.mock_span, len)

        # Verify function name attribute was set
        function_name_call = None
        for call in self.mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_FUNCTION_NAME:
                function_name_call = call
                break

        self.assertIsNotNone(function_name_call)
        self.assertEqual(function_name_call[0][1], "builtins.len")

    def test_span_not_recording(self):
        """Test with span that is not recording."""
        self.mock_span.is_recording.return_value = False

        add_code_attributes_to_span(self.mock_span, test_function)

        # Verify no attributes were set
        self.mock_span.set_attribute.assert_not_called()

    def test_function_with_line_number(self):
        """Test function that has __code__ attribute with line number."""
        add_code_attributes_to_span(self.mock_span, test_function)

        # Check if line number was set
        line_number_call = None
        for call in self.mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_LINE_NUMBER:
                line_number_call = call
                break

        self.assertIsNotNone(line_number_call)
        self.assertIsInstance(line_number_call[0][1], int)

    def test_inspect_getfile_failure(self):
        """Test when inspect.getfile fails."""
        # Create a mock callable that will cause inspect.getfile to fail
        mock_callable = Mock()
        mock_callable.__module__ = "test_module"
        mock_callable.__name__ = "test_callable"

        with patch("amazon.opentelemetry.distro.code_correlation.utils.inspect.getfile", side_effect=OSError):
            add_code_attributes_to_span(self.mock_span, mock_callable)

        # Should still set function name, but not file path
        function_name_call = None
        file_path_call = None
        for call in self.mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_FUNCTION_NAME:
                function_name_call = call
            elif call[0][0] == CODE_FILE_PATH:
                file_path_call = call

        self.assertIsNotNone(function_name_call)
        self.assertIsNone(file_path_call)

    def test_exception_handling(self):
        """Test exception handling when setting attributes."""
        self.mock_span.set_attribute.side_effect = Exception("Test exception")

        # Should not raise exception
        add_code_attributes_to_span(self.mock_span, test_function)


class TestRecordCodeAttributes(TestCase):
    """Test the record_code_attributes decorator."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_span = Mock()
        self.mock_span.is_recording.return_value = True

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_sync_function(self, mock_get_current_span):
        """Test decorator on synchronous function."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        def test_sync_func(x, y):
            return x + y

        result = test_sync_func(1, 2)

        # Verify function worked correctly
        self.assertEqual(result, 3)

        # Verify span attributes were set
        self.mock_span.set_attribute.assert_called()

        # Check that function name attribute was set
        function_name_call = None
        for call in self.mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_FUNCTION_NAME:
                function_name_call = call
                break

        self.assertIsNotNone(function_name_call)
        self.assertTrue(function_name_call[0][1].endswith("test_sync_func"))

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_async_function(self, mock_get_current_span):
        """Test decorator on asynchronous function."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        async def test_async_func(x, y):
            return x + y

        # Run the async function
        result = asyncio.run(test_async_func(1, 2))

        # Verify function worked correctly
        self.assertEqual(result, 3)

        # Verify span attributes were set
        self.mock_span.set_attribute.assert_called()

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_no_current_span(self, mock_get_current_span):
        """Test decorator when there's no current span."""
        mock_get_current_span.return_value = None

        @record_code_attributes
        def test_func():
            return "success"

        result = test_func()

        # Function should still work
        self.assertEqual(result, "success")

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_get_current_span_exception(self, mock_get_current_span):
        """Test decorator when getting current span raises exception."""
        mock_get_current_span.side_effect = Exception("Test exception")

        @record_code_attributes
        def test_func():
            return "success"

        result = test_func()

        # Function should still work despite exception
        self.assertEqual(result, "success")

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_preserves_function_metadata(self, mock_get_current_span):
        """Test that decorator preserves original function metadata."""
        mock_get_current_span.return_value = self.mock_span

        def original_func():
            """Original function docstring."""
            return "original"

        decorated_func = record_code_attributes(original_func)

        # Check that metadata is preserved
        self.assertEqual(decorated_func.__name__, "original_func")
        self.assertEqual(decorated_func.__doc__, "Original function docstring.")

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_with_args_and_kwargs(self, mock_get_current_span):
        """Test decorator with functions that have various argument types."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        def test_func(pos_arg, *args, kw_arg=None, **kwargs):
            return {"pos": pos_arg, "args": args, "kw": kw_arg, "kwargs": kwargs}

        result = test_func("test", "extra1", "extra2", kw_arg="kw_test", extra_kw="extra")

        expected = {"pos": "test", "args": ("extra1", "extra2"), "kw": "kw_test", "kwargs": {"extra_kw": "extra"}}
        self.assertEqual(result, expected)

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_async_with_exception(self, mock_get_current_span):
        """Test decorator on async function that raises exception."""
        mock_get_current_span.return_value = self.mock_span

        @record_code_attributes
        async def test_async_func():
            raise ValueError("Test exception")

        # Exception should propagate
        with self.assertRaises(ValueError):
            asyncio.run(test_async_func())

        # But span attributes should still be set
        self.mock_span.set_attribute.assert_called()


class TestConstructQualifiedName(TestCase):
    """Test the _construct_qualified_name helper function."""

    def test_module_class_function(self):
        """Test with module, class, and function name."""
        result = _construct_qualified_name("mymodule", "MyClass", "my_function")
        self.assertEqual(result, "mymodule.MyClass.my_function")

    def test_module_function_no_class(self):
        """Test with module and function name, no class."""
        result = _construct_qualified_name("mymodule", None, "my_function")
        self.assertEqual(result, "mymodule.my_function")

    def test_unknown_module_with_class(self):
        """Test with unknown module but with class name."""
        result = _construct_qualified_name("<unknown_module>", "MyClass", "my_function")
        self.assertEqual(result, "<unknown_module>.MyClass.my_function")

    def test_unknown_module_no_class(self):
        """Test with unknown module and no class name."""
        result = _construct_qualified_name("<unknown_module>", None, "my_function")
        self.assertEqual(result, "my_function")

    def test_none_module_no_class(self):
        """Test with None module and no class name."""
        result = _construct_qualified_name(None, None, "my_function")
        self.assertEqual(result, "my_function")

    def test_empty_module_no_class(self):
        """Test with empty module name and no class name."""
        result = _construct_qualified_name("", None, "my_function")
        self.assertEqual(result, "my_function")

    def test_empty_class_name(self):
        """Test with empty class name."""
        result = _construct_qualified_name("mymodule", "", "my_function")
        # Empty class name is treated as falsy, so it's skipped
        self.assertEqual(result, "mymodule.my_function")

    def test_empty_function_name(self):
        """Test with empty function name."""
        result = _construct_qualified_name("mymodule", "MyClass", "")
        self.assertEqual(result, "mymodule.MyClass.")

    def test_all_empty_strings(self):
        """Test with all empty strings."""
        result = _construct_qualified_name("", "", "")
        self.assertEqual(result, "")

    def test_whitespace_in_names(self):
        """Test with whitespace in names."""
        result = _construct_qualified_name("my module", "My Class", "my function")
        self.assertEqual(result, "my module.My Class.my function")


class TestUtilsIntegration(TestCase):
    """Integration tests for utils functions."""

    def test_frame_and_callable_consistency(self):
        """Test that frame-based and callable-based functions give consistent results."""

        def test_integration_func():
            frame = inspect.currentframe()
            frame_name = get_function_fullname_from_frame(frame)
            callable_name = get_callable_fullname(test_integration_func)
            return frame_name, callable_name

        frame_name, callable_name = test_integration_func()

        # Both should end with the same function name, though they may have different levels of detail
        self.assertTrue(frame_name.endswith("test_integration_func"))
        self.assertTrue(callable_name.endswith("test_integration_func"))
        # Both should start with the same module name
        self.assertTrue(frame_name.startswith(__name__))
        self.assertTrue(callable_name.startswith(__name__))

    def test_span_attributes_consistency(self):
        """Test that both span attribute functions set consistent function names."""
        mock_span = Mock()
        mock_span.is_recording.return_value = True

        def test_consistency_func():
            frame = inspect.currentframe()
            # Test frame-based approach
            add_code_attributes_to_span_from_frame(frame, mock_span)

            # Reset mock to check callable-based approach
            mock_span.reset_mock()
            mock_span.is_recording.return_value = True

            # Test callable-based approach
            add_code_attributes_to_span(mock_span, test_consistency_func)

            return frame

        test_consistency_func()

        # Both approaches should have set the function name attribute
        self.assertTrue(mock_span.set_attribute.called)

    @patch("amazon.opentelemetry.distro.code_correlation.utils.trace.get_current_span")
    def test_decorator_integration(self, mock_get_current_span):
        """Test decorator integration with span attribute functions."""
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_get_current_span.return_value = mock_span

        @record_code_attributes
        def decorated_test_func():
            return "decorated"

        result = decorated_test_func()

        self.assertEqual(result, "decorated")
        self.assertTrue(mock_span.set_attribute.called)

        # Check that function name was set
        function_name_set = False
        for call in mock_span.set_attribute.call_args_list:
            if call[0][0] == CODE_FUNCTION_NAME:
                function_name_set = True
                self.assertTrue(call[0][1].endswith("decorated_test_func"))
                break

        self.assertTrue(function_name_set, "Function name attribute was not set")
