# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Code correlation module for AWS OpenTelemetry Python Instrumentation.

This module provides functionality for correlating code execution with telemetry data.
"""

__version__ = "1.0.0"


"""
Utility functions for adding code information to OpenTelemetry spans.
"""

from typing import Any, Callable
from functools import wraps
from opentelemetry import trace


# Code correlation attribute constants
CODE_FUNCTION_NAME = "code.function.name"
CODE_FILE_PATH = "code.file.path"
CODE_LINE_NUMBER = "code.line.number"


def _add_code_attributes_to_span(span, func: Callable[..., Any]) -> None:
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
        # Get function name
        function_name = getattr(func, '__name__', str(func))
        span.set_attribute(CODE_FUNCTION_NAME, function_name)
        
        # Get function source file from code object
        try:
            if hasattr(func, '__code__'):
                source_file = func.__code__.co_filename
                span.set_attribute(CODE_FILE_PATH, source_file)
        except (AttributeError, TypeError):
            # Handle cases where code object is not available
            # (e.g., built-in functions, C extensions)
            pass
            
        # Get function line number from code object
        try:
            if hasattr(func, '__code__'):
                line_number = func.__code__.co_firstlineno
                span.set_attribute(CODE_LINE_NUMBER, line_number)
        except (AttributeError, TypeError):
            # Handle cases where code object is not available
            pass
            
    except Exception:
        # Silently handle any unexpected errors to avoid breaking
        # the instrumentation flow
        pass


def add_code_attributes_to_span(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to automatically add code attributes to the current OpenTelemetry span.
    
    This decorator extracts metadata from the decorated function and adds it as
    attributes to the current active span. The attributes added are:
    - code.function.name: The name of the function
    - code.file.path: The file path where the function is defined
    - code.line.number: The line number where the function is defined
    
    This decorator supports both synchronous and asynchronous functions.
    
    Usage:
        @add_code_attributes_to_span
        def my_sync_function():
            # Sync function implementation
            pass
            
        @add_code_attributes_to_span
        async def my_async_function():
            # Async function implementation
            pass
    
    Args:
        func: The function to be decorated
        
    Returns:
        The wrapped function with current span code attributes tracing
    """
    # Detect async functions: check function code object flags or special attributes
    # CO_ITERABLE_COROUTINE = 0x80, async functions will have this flag set
    is_async = (hasattr(func, '__code__') and 
                func.__code__.co_flags & 0x80) or hasattr(func, '_is_coroutine')
    
    if is_async:
        # Async function wrapper
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Add code attributes to current span
            try:
                current_span = trace.get_current_span()
                if current_span:
                    _add_code_attributes_to_span(current_span, func)
            except Exception:
                # Silently handle any unexpected errors
                pass
            
            # Call and await the original async function
            return await func(*args, **kwargs)
        
        return async_wrapper
    else:
        # Sync function wrapper
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Add code attributes to current span
            try:
                current_span = trace.get_current_span()
                if current_span:
                    _add_code_attributes_to_span(current_span, func)
            except Exception:
                # Silently handle any unexpected errors
                pass
            
            # Call the original sync function
            return func(*args, **kwargs)
        
        return sync_wrapper
