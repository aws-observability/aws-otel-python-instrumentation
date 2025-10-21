# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Code correlation module for AWS OpenTelemetry Python Instrumentation.

This module provides functionality for correlating code execution with telemetry data.
"""
# Import code attributes span processor
from .code_attributes_span_processor import CodeAttributesSpanProcessor

# Import main utilities to maintain API compatibility
from .utils import (
    add_code_attributes_to_span,
    add_code_attributes_to_span_from_frame,
    get_callable_fullname,
    get_function_fullname_from_frame,
    record_code_attributes,
)

# Version information
__version__ = "1.0.0"

# Define public API
__all__ = [
    # Functions
    "add_code_attributes_to_span",
    "add_code_attributes_to_span_from_frame",
    "get_callable_fullname",
    "get_function_fullname_from_frame",
    "record_code_attributes",
    # Classes
    "CodeAttributesSpanProcessor",
    # Version
    "__version__",
]
