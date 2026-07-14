# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Base test utilities for LLO Handler tests."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.llo_handler import LLOHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import ReadableSpan, SpanContext
from opentelemetry.trace import SpanKind, TraceFlags, TraceState


class LLOHandlerTestBase(TestCase):
    """Base class with common setup and utilities for LLO Handler tests."""

    def setUp(self):
        self.logger_provider_mock = MagicMock(spec=LoggerProvider)
        self.event_logger_mock = MagicMock()
        self.event_logger_provider_mock = MagicMock()
        self.event_logger_provider_mock.get_event_logger.return_value = self.event_logger_mock

        with patch(
            "amazon.opentelemetry.distro.llo_handler.EventLoggerProvider", return_value=self.event_logger_provider_mock
        ):
            self.llo_handler = LLOHandler(self.logger_provider_mock)

    @staticmethod
    def _create_mock_span(attributes=None, kind=SpanKind.INTERNAL, preserve_none=False):
        """
        Create a mock ReadableSpan for testing.

        Args:
            attributes: Span attributes dictionary. Defaults to empty dict unless preserve_none=True
            kind: The span kind (default: INTERNAL)
            preserve_none: If True, keeps None attributes instead of converting to empty dict

        Returns:
            MagicMock: A mock span with context, attributes, and basic properties set
        """
        if attributes is None and not preserve_none:
            attributes = {}

        span_context = SpanContext(
            trace_id=0x123456789ABCDEF0123456789ABCDEF0,
            span_id=0x123456789ABCDEF0,
            is_remote=False,
            trace_flags=TraceFlags.SAMPLED,
            trace_state=TraceState.get_default(),
        )

        mock_span = MagicMock(spec=ReadableSpan)
        mock_span.context = span_context
        mock_span.attributes = attributes
        mock_span.kind = kind
        mock_span.start_time = 1234567890

        return mock_span
