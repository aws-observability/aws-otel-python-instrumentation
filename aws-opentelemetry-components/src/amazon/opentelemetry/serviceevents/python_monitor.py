# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
ServiceEvents function monitoring - Interface module.

This module provides a context manager for tracking function invocations,
durations, exceptions, and caller relationships, backed by a pure-Python
implementation.

All external code should import from this module:
    from .python_monitor import _ServiceEventsMonitorState, PythonServiceEventsMonitor
    from .python_monitor import set_current_operation, get_current_operation
"""

# ============================================================================
# Implementation
# ============================================================================

# The monitor is backed by a single pure-Python implementation. (An experimental
# native C++ backend was retired; this module is now a thin re-export of the
# pure-Python implementation kept for import-path stability.)
from amazon.opentelemetry.serviceevents.python_monitor_impl import (
    PythonServiceEventsMonitor,
    _ServiceEventsMonitorState,
    clear_current_operation,
    get_call_stack,
    get_current_operation,
    get_sampling_mode,
    reset_after_fork,
    set_current_operation,
    set_sampling_mode,
    set_sampling_thresholds,
)

# ============================================================================
# Public API
# ============================================================================

__all__ = [
    # Classes
    "_ServiceEventsMonitorState",
    "PythonServiceEventsMonitor",
    # Operation functions
    "set_current_operation",
    "get_current_operation",
    "clear_current_operation",
    # Sampling mode functions
    "set_sampling_mode",
    "get_sampling_mode",
    "set_sampling_thresholds",
    # State management functions
    "reset_after_fork",
    "get_call_stack",
]
