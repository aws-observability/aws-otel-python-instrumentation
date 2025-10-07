# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Code correlation module for AWS OpenTelemetry Python Instrumentation.

This module provides functionality for correlating code execution with telemetry data.
"""

import inspect
from functools import wraps
from typing import Any, Callable

from opentelemetry import trace

__version__ = "1.0.0"


# Code correlation attribute constants
CODE_FUNCTION_NAME = "code.function.name"
CODE_FILE_PATH = "code.file.path"
CODE_LINE_NUMBER = "code.line.number"


def add_code_attributes_to_span(span, func_or_class: Callable[..., Any]) -> None:
    """
    Add code-related attributes to a span based on a Python function.

    This utility method extracts function metadata and adds the following
    span attributes:
    - CODE_FUNCTION_NAME: The name of the function
    - CODE_FILE_PATH: The file path where the function is defined
    - CODE_LINE_NUMBER: The line number where the function is defined

    Args:
        span: The OpenTelemetry span to add attributes to
        func: The Python function to extract metadata from
    """
    if not span.is_recording():
        return

    try:
        if inspect.isclass(func_or_class):
            span.set_attribute(CODE_FUNCTION_NAME, func_or_class.__name__)
            span.set_attribute(CODE_FILE_PATH, inspect.getfile(func_or_class))
        else:
            code = getattr(func_or_class, "__code__", None)
            if code:
                span.set_attribute(CODE_FUNCTION_NAME, func_or_class.__name__)
                span.set_attribute(CODE_FILE_PATH, code.co_filename)
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
                # Silently handle any unexpected errors
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
            # Silently handle any unexpected errors
            pass

        # Call the original sync function
        return func(*args, **kwargs)

    return sync_wrapper
