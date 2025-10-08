# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Utility functions for code correlation in AWS OpenTelemetry Python Instrumentation.

This module contains the core functionality for extracting and correlating
code metadata with telemetry data.
"""

import functools
import inspect
from functools import wraps
from types import FunctionType, MethodType
from typing import Any, Callable

from opentelemetry import trace


def get_callable_fullname(obj) -> str:
    """
    Return the fully qualified name of any callable (module + qualname),
    safely handling functions, methods, classes, partials, built-ins, etc.

    Examples:
        >>> get_callable_fullname(len)
        'builtins.len'
        >>> get_callable_fullname(math.sqrt)
        'math.sqrt'
        >>> get_callable_fullname(MyClass.method)
        '__main__.MyClass.method'
        >>> get_callable_fullname(functools.partial(func))
        '__main__.func'
    """
    try:
        # functools.partial objects
        if isinstance(obj, functools.partial):
            target = get_callable_fullname(obj.func)
            return target

        # Classes
        if inspect.isclass(obj):
            module = getattr(obj, "__module__", "<unknown_module>")
            name = getattr(obj, "__qualname__", getattr(obj, "__name__", "<unknown_class>"))
            return f"{module}.{name}"

        # Bound or unbound methods
        if isinstance(obj, MethodType):
            func = obj.__func__
            cls = getattr(obj, "__self__", None)
            if cls:
                cls_name = cls.__class__.__name__ if not inspect.isclass(cls) else cls.__name__
                module = getattr(func, "__module__", "<unknown_module>")
                name = getattr(func, "__name__", "<unknown_func>")
                return f"{module}.{cls_name}.{name}"

        # Regular Python functions, lambdas, static/class methods
        if isinstance(obj, (FunctionType, staticmethod, classmethod)):
            func = inspect.unwrap(obj)
            module = getattr(func, "__module__", "<unknown_module>")
            qualname = getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))
            return f"{module}.{qualname}"

        # Built-in or C extension functions (e.g., len, numpy.add)
        module = getattr(obj, "__module__", None)
        name = getattr(obj, "__qualname__", None) or getattr(obj, "__name__", None)
        if module and name:
            return f"{module}.{name}"
        elif name:
            return name

        # Fallback for unknown callables
        return repr(obj)

    except Exception:
        return "<unknown_callable>"


def add_code_attributes_to_span(span, func_or_class: Callable[..., Any]) -> None:
    """
    Add code-related attributes to a span based on a Python function or class.

    This utility method extracts metadata and adds the following span attributes:
    - CODE_FUNCTION_NAME: The fully qualified name of the function/class
    - CODE_FILE_PATH: The file path where the function/class is defined
    - CODE_LINE_NUMBER: The line number where the function is defined (if available)

    Args:
        span: The OpenTelemetry span to add attributes to
        func_or_class: The Python function or class to extract metadata from
    """
    # Import constants here to avoid circular imports
    from . import CODE_FUNCTION_NAME, CODE_FILE_PATH, CODE_LINE_NUMBER
    
    if not span.is_recording():
        return

    try:
        # Always set the function name using our robust helper
        span.set_attribute(CODE_FUNCTION_NAME, get_callable_fullname(func_or_class))
        
        # Try to get file path using inspect.getfile (works for both classes and functions)
        try:
            file_path = inspect.getfile(func_or_class)
            span.set_attribute(CODE_FILE_PATH, file_path)
        except (OSError, TypeError):
            # Built-ins and some other callables don't have source files
            pass
        
        # Try to get line number from __code__ attribute (only available for functions)
        code = getattr(func_or_class, "__code__", None)
        if code:
            span.set_attribute(CODE_LINE_NUMBER, code.co_firstlineno)
            
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def record_code_attributes(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to automatically add code attributes to the current OpenTelemetry span.

    This decorator extracts metadata from the decorated function and adds it as
    attributes to the current active span. The attributes added are:
    - code.function.name: The name of the function
    - code.file.path: The file path where the function is defined
    - code.line.number: The line number where the function is defined

    This decorator supports both synchronous and asynchronous functions.

    Usage:
        @record_code_attributes
        def my_sync_function():
            # Sync function implementation
            pass

        @record_code_attributes
        async def my_async_function():
            # Async function implementation
            pass

    Args:
        func: The function to be decorated

    Returns:
        The wrapped function with current span code attributes tracing
    """
    # Detect async functions
    is_async = inspect.iscoroutinefunction(func)

    if is_async:
        # Async function wrapper
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Add code attributes to current span
            try:
                current_span = trace.get_current_span()
                if current_span:
                    add_code_attributes_to_span(current_span, func)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            # Call and await the original async function
            return await func(*args, **kwargs)

        return async_wrapper

    # Sync function wrapper
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        # Add code attributes to current span
        try:
            current_span = trace.get_current_span()
            if current_span:
                add_code_attributes_to_span(current_span, func)
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        # Call the original sync function
        return func(*args, **kwargs)

    return sync_wrapper
