# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared stack trace utilities for Dynamic Instrumentation."""

import inspect
import logging
from typing import List

from amazon.opentelemetry.distro.debugger._snapshot_models import StackFrame

logger = logging.getLogger(__name__)

_INTERNAL_PATH_PATTERNS = (
    "/amazon/opentelemetry/",
    "/site-packages/opentelemetry/",
)


def is_internal_frame(filename: str) -> bool:
    """Check if a stack frame filename belongs to ADOT or OpenTelemetry internals."""
    if not filename:
        return False
    normalized = filename.replace("\\", "/")
    return any(pattern in normalized for pattern in _INTERNAL_PATH_PATTERNS)


def capture_stack_frames(max_frames: int = 20) -> List[StackFrame]:
    """
    Capture stack frames, filtering out debugger-internal frames.

    Returns frames from top (current) to bottom (entry point).
    """
    if max_frames <= 0:
        return []
    try:
        frames = []
        frame = inspect.currentframe()
        try:
            while frame:
                filename = frame.f_code.co_filename
                if not is_internal_frame(filename):
                    frames.append(
                        StackFrame(
                            file_name=filename,
                            function=frame.f_code.co_name,
                            line_number=frame.f_lineno,
                        )
                    )
                frame = frame.f_back
        finally:
            del frame

        if len(frames) > max_frames:
            frames = frames[:max_frames]
        return frames
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.debug("Failed to capture stack frames: %s", exc)
        return []
