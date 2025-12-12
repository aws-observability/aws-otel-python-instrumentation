# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Patches for OpenTelemetry Celery instrumentation to add code correlation support.

This module provides patches to enhance the Celery instrumentation with code correlation
capabilities, allowing tracking of user code that is executed within Celery tasks.
"""

import functools
import logging
from typing import Any, Callable, Optional

from amazon.opentelemetry.distro.code_correlation.utils import add_code_attributes_to_span

logger = logging.getLogger(__name__)

# Import at module level to avoid pylint import-outside-toplevel
try:
    from opentelemetry.instrumentation.celery import CeleryInstrumentor
    from opentelemetry.instrumentation.celery import utils as celery_utils
except ImportError:
    celery_utils = None
    CeleryInstrumentor = None


def _extract_task_function(task) -> Optional[Callable[..., Any]]:  # pylint: disable=too-many-return-statements
    """
    Extract the actual user function from a Celery task object.

    Args:
        task: The Celery task object

    Returns:
        The underlying user function if found, None otherwise
    """
    if task is None:
        return None

    try:
        # For regular function-based tasks, the actual function is stored in task.run
        if hasattr(task, "run") and callable(task.run):
            func = task.run
            if hasattr(func, "__func__"):
                return func.__func__
            if func.__name__ != "run":  # Avoid returning generic run methods
                return func

        # For function-based tasks, the original function might be stored differently
        if hasattr(task, "__call__") and callable(task.__call__):
            func = task.__call__
            if hasattr(func, "__func__") and func.__func__.__name__ != "__call__":
                return func.__func__
            if func.__name__ != "__call__":
                return func

        # Try to get the original function from __wrapped__ attribute
        if hasattr(task, "__wrapped__") and callable(task.__wrapped__):
            return task.__wrapped__

    except Exception:  # pylint: disable=broad-exception-caught
        pass

    return None


def _add_code_correlation_to_span(span, task) -> None:
    """
    Add code correlation attributes to a span for a Celery task.

    Args:
        span: The OpenTelemetry span to add attributes to
        task: The Celery task object
    """
    try:
        if span is None or not span.is_recording():
            return

        user_function = _extract_task_function(task)
        if user_function is not None:
            add_code_attributes_to_span(span, user_function)

    except Exception:  # pylint: disable=broad-exception-caught
        pass


def patch_celery_prerun(original_trace_prerun: Callable) -> Callable:
    """
    Patch the Celery _trace_prerun method to add code correlation support.

    Args:
        original_trace_prerun: The original _trace_prerun method to wrap

    Returns:
        The patched _trace_prerun method
    """

    @functools.wraps(original_trace_prerun)
    def patched_trace_prerun(self, *args, **kwargs):
        result = original_trace_prerun(self, *args, **kwargs)

        try:
            task = kwargs.get("task")
            task_id = kwargs.get("task_id")

            if task is not None and task_id is not None and celery_utils is not None:
                ctx = celery_utils.retrieve_context(task, task_id)
                if ctx is not None:
                    span, _, _ = ctx
                    if span is not None:
                        _add_code_correlation_to_span(span, task)

        except Exception:  # pylint: disable=broad-exception-caught
            pass

        return result

    return patched_trace_prerun


def _apply_celery_instrumentation_patches():
    """
    Apply code correlation patches to the Celery instrumentation.
    """
    try:
        if CeleryInstrumentor is None:
            logger.warning("Failed to apply Celery patches: CeleryInstrumentor not available")
            return

        original_trace_prerun = CeleryInstrumentor._trace_prerun
        CeleryInstrumentor._trace_prerun = patch_celery_prerun(original_trace_prerun)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Failed to apply Celery instrumentation patches: %s", exc)
