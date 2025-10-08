# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Code correlation module for AWS OpenTelemetry Python Instrumentation.

This module provides functionality for correlating code execution with telemetry data.
"""

# Version information
__version__ = "1.0.0"

# Code correlation attribute constants
CODE_FUNCTION_NAME = "code.function.name"
CODE_FILE_PATH = "code.file.path"
CODE_LINE_NUMBER = "code.line.number"

# Import main utilities to maintain API compatibility
from .utils import (
    add_code_attributes_to_span,
    get_callable_fullname,
    record_code_attributes,
)

# Import stack trace processor
from .stack_trace_span_processor import StackTraceSpanProcessor

# Define public API
__all__ = [
    # Constants
    "CODE_FUNCTION_NAME",
    "CODE_FILE_PATH", 
    "CODE_LINE_NUMBER",
    # Functions
    "add_code_attributes_to_span",
    "get_callable_fullname",
    "record_code_attributes",
    # Classes
    "StackTraceSpanProcessor",
    # Version
    "__version__",
]
