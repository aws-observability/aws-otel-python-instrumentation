# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Code Attributes Span Processor implementation for OpenTelemetry Python.

This processor captures stack traces and attaches them to spans as attributes.
It's based on the OpenTelemetry Java contrib StackTraceSpanProcessor.
"""

import sys
import typing as t
from types import FrameType
from typing import Optional

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv.attributes.code_attributes import CODE_FUNCTION_NAME
from opentelemetry.trace import SpanKind

from .internal.packages_resolver import _build_package_mapping, _load_third_party_packages, is_user_code
from .utils import add_code_attributes_to_span_from_frame


# pylint: disable=no-self-use
class CodeAttributesSpanProcessor(SpanProcessor):
    """
    A SpanProcessor that captures and attaches code attributes to spans.

    This processor adds stack trace information as span attributes, which can be
    useful for debugging and understanding the call flow that led to span creation.
    """

    # Maximum number of stack frames to examine
    MAX_STACK_FRAMES = 50

    @staticmethod
    def _iter_stack_frames(frame: FrameType) -> t.Iterator[FrameType]:
        """Iterate through stack frames starting from the given frame."""
        _frame: t.Optional[FrameType] = frame
        while _frame is not None:
            yield _frame
            _frame = _frame.f_back

    def __init__(self):
        """Initialize the CodeAttributesSpanProcessor."""
        # Pre-initialize expensive operations to avoid runtime performance overhead
        # These @execute_once methods are slow, so we call them during initialization
        # to cache their results ahead of time
        _build_package_mapping()
        _load_third_party_packages()

    def on_start(
        self,
        span: Span,
        parent_context: Optional[Context] = None,
    ) -> None:
        """
        Called when a span is started. Captures and attaches code attributes from stack trace.

        Args:
            span: The span that was started
            parent_context: The parent context (unused)
        """
        # Skip if span should not be processed
        if not self._should_process_span(span):
            return

        # Capture code attributes from stack trace
        self._capture_code_attributes(span)

    def _should_process_span(self, span: Span) -> bool:
        """
        Determine if span should be processed for code attributes.

        Returns False if:
        - Span is library instrumentation SERVER or INTERNAL span
        - Span already has code attributes

        Note: Library instrumentation CLIENT/PRODUCER/CONSUMER spans are still processed
        as they provide valuable context for tracing call chains.
        """

        if span.kind in (SpanKind.SERVER, SpanKind.INTERNAL) and self._is_library_instrumentation_span(span):
            return False

        if span.attributes is not None and CODE_FUNCTION_NAME in span.attributes:
            return False

        return True

    def _is_library_instrumentation_span(self, span: Span) -> bool:
        """
        Check if span is created by library instrumentation.

        Args:
            span: The span to check

        Returns:
            True if span is from library instrumentation, False otherwise
        """
        scope = span.instrumentation_scope

        if scope is None or scope.name is None:
            return False  # No scope info, assume user-created

        return scope.name.startswith("opentelemetry.instrumentation")

    def _capture_code_attributes(self, span: Span) -> None:
        """Capture and attach code attributes from current stack trace."""
        try:
            current_frame = sys._getframe(1)  # pylint: disable=protected-access

            for frame_index, frame in enumerate(self._iter_stack_frames(current_frame)):
                if frame_index >= self.MAX_STACK_FRAMES:
                    break

                code = frame.f_code

                if is_user_code(code.co_filename):
                    add_code_attributes_to_span_from_frame(frame, span)
                    break  # Only capture the first user code frame

        except (OSError, ValueError):
            # sys._getframe may not be available on all platforms
            pass

    def on_end(self, span: ReadableSpan) -> None:
        """
        Called when a span is ended. Captures and attaches stack trace if conditions are met.
        """

    def shutdown(self) -> None:
        """Called when the processor is shutdown. No cleanup needed."""
        # No cleanup needed for code attributes processor

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=unused-argument
        """Force flush any pending spans. Always returns True as no pending work."""
        return True
