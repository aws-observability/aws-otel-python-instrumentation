# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for LLO Handler span and attribute processing functionality."""

from unittest.mock import MagicMock, patch

from test_llo_handler_base import LLOHandlerTestBase

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import Event as SpanEvent


class TestLLOHandlerProcessing(LLOHandlerTestBase):
    """Test span processing and attribute filtering functionality."""

    def test_filter_attributes(self):
        """
        Verify _filter_attributes removes LLO content attributes while preserving role attributes
        and other non-LLO attributes.
        """
        attributes = {
            "gen_ai.prompt.0.content": "test content",
            "gen_ai.prompt.0.role": "user",
            "normal.attribute": "value",
            "another.normal.attribute": 123,
        }

        filtered = self.llo_handler._filter_attributes(attributes)

        self.assertNotIn("gen_ai.prompt.0.content", filtered)
        self.assertIn("gen_ai.prompt.0.role", filtered)
        self.assertIn("normal.attribute", filtered)
        self.assertIn("another.normal.attribute", filtered)

    def test_filter_attributes_empty_dict(self):
        """
        Verify _filter_attributes returns empty dict when given empty dict.
        """
        result = self.llo_handler._filter_attributes({})

        self.assertEqual(result, {})

    def test_filter_attributes_none_handling(self):
        """
        Verify _filter_attributes returns original attributes when no LLO attributes are present.
        """
        attributes = {"normal.attr": "value"}
        result = self.llo_handler._filter_attributes(attributes)

        self.assertEqual(result, attributes)

    def test_filter_attributes_no_llo_attrs(self):
        """
        Test _filter_attributes when there are no LLO attributes - should return original
        """
        attributes = {
            "normal.attr1": "value1",
            "normal.attr2": "value2",
            "other.attribute": "value",  # This is not an LLO attribute
        }

        result = self.llo_handler._filter_attributes(attributes)

        # Should return the same attributes object when no LLO attrs present
        self.assertIs(result, attributes)
        self.assertEqual(result, attributes)

    def test_process_spans(self):
        """
        Verify process_spans extracts LLO attributes, emits events, filters attributes,
        and processes span events correctly.
        """
        attributes = {"gen_ai.prompt.0.content": "prompt content", "normal.attribute": "normal value"}

        span = self._create_mock_span(attributes)
        span.events = []

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit, patch.object(
            self.llo_handler, "_filter_attributes"
        ) as mock_filter:

            filtered_attributes = {"normal.attribute": "normal value"}
            mock_filter.return_value = filtered_attributes

            result = self.llo_handler.process_spans([span])

            # Now it's called with only the LLO attributes
            expected_llo_attrs = {"gen_ai.prompt.0.content": "prompt content"}
            mock_emit.assert_called_once_with(span, expected_llo_attrs)
            mock_filter.assert_called_once_with(attributes)

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], span)
            self.assertEqual(result[0]._attributes, filtered_attributes)

    def test_process_spans_with_bounded_attributes(self):
        """
        Verify process_spans correctly handles spans with BoundedAttributes,
        preserving attribute limits and settings.
        """
        bounded_attrs = BoundedAttributes(
            maxlen=10,
            attributes={"gen_ai.prompt.0.content": "prompt content", "normal.attribute": "normal value"},
            immutable=False,
            max_value_len=1000,
        )

        span = self._create_mock_span(bounded_attrs)
        span.events = []  # Add empty events list

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit, patch.object(
            self.llo_handler, "_filter_attributes"
        ) as mock_filter:

            filtered_attributes = {"normal.attribute": "normal value"}
            mock_filter.return_value = filtered_attributes

            result = self.llo_handler.process_spans([span])

            # Now it's called with only the LLO attributes
            expected_llo_attrs = {"gen_ai.prompt.0.content": "prompt content"}
            mock_emit.assert_called_once_with(span, expected_llo_attrs)
            mock_filter.assert_called_once_with(bounded_attrs)

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], span)
            self.assertIsInstance(result[0]._attributes, BoundedAttributes)
            self.assertEqual(dict(result[0]._attributes), filtered_attributes)

    def test_process_spans_none_attributes(self):
        """
        Verify process_spans correctly handles spans with None attributes.
        """
        span = self._create_mock_span(None, preserve_none=True)
        span.events = []

        result = self.llo_handler.process_spans([span])

        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]._attributes)

    def test_filter_span_events(self):
        """
        Verify _filter_span_events filters LLO attributes from span events correctly.
        """
        event_attributes = {
            "gen_ai.prompt": "event prompt",
            "normal.attribute": "keep this",
        }

        event = SpanEvent(
            name="test_event",
            attributes=event_attributes,
            timestamp=1234567890,
        )

        span = self._create_mock_span({})
        span.events = [event]
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._filter_span_events(span)

        span_events = getattr(span, "_events", [])
        updated_event = span_events[0]
        self.assertIn("normal.attribute", updated_event.attributes)
        self.assertNotIn("gen_ai.prompt", updated_event.attributes)

    def test_filter_span_events_no_events(self):
        """
        Verify _filter_span_events handles spans with no events gracefully.
        """
        span = self._create_mock_span({})
        span.events = None
        span._events = None

        self.llo_handler._filter_span_events(span)

        self.assertIsNone(span._events)

    def test_filter_span_events_no_attributes(self):
        """
        Test _filter_span_events when event has no attributes
        """
        event = SpanEvent(
            name="test_event",
            attributes=None,
            timestamp=1234567890,
        )

        span = self._create_mock_span({})
        span.events = [event]

        self.llo_handler._filter_span_events(span)

        # Should handle gracefully and keep the original event
        span_events = getattr(span, "_events", [])
        self.assertEqual(len(span_events), 1)
        self.assertEqual(span_events[0], event)

    def test_filter_span_events_bounded_attributes(self):
        """
        Test _filter_span_events with BoundedAttributes in events
        """
        bounded_event_attrs = BoundedAttributes(
            maxlen=5,
            attributes={
                "gen_ai.prompt": "event prompt",
                "normal.attribute": "keep this",
            },
            immutable=False,
            max_value_len=100,
        )

        event = SpanEvent(
            name="test_event",
            attributes=bounded_event_attrs,
            timestamp=1234567890,
            limit=5,
        )

        span = self._create_mock_span({})
        span.events = [event]
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._filter_span_events(span)

        # Verify event was updated with filtered attributes
        span_events = getattr(span, "_events", [])
        updated_event = span_events[0]
        self.assertIsInstance(updated_event, SpanEvent)
        self.assertEqual(updated_event.name, "test_event")
        self.assertIn("normal.attribute", updated_event.attributes)
        self.assertNotIn("gen_ai.prompt", updated_event.attributes)

    def test_process_spans_consolidated_event_emission(self):
        """
        Verify process_spans collects LLO attributes from both span attributes and events,
        then emits a single consolidated event.
        """
        # Span attributes with prompt
        span_attributes = {
            "gen_ai.prompt": "What is quantum computing?",
            "normal.attribute": "keep this",
        }

        # Event attributes with completion
        event_attributes = {
            "gen_ai.completion": "Quantum computing is...",
            "other.attribute": "also keep this",
        }

        event = SpanEvent(
            name="gen_ai.content.completion",
            attributes=event_attributes,
            timestamp=1234567890,
        )

        span = self._create_mock_span(span_attributes)
        span.events = [event]
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "openlit.otel.tracing"

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit:
            result = self.llo_handler.process_spans([span])

            # Should emit once with combined attributes
            mock_emit.assert_called_once()
            call_args = mock_emit.call_args[0]
            emitted_span = call_args[0]
            emitted_attributes = call_args[1]

            # Verify the emitted attributes contain both prompt and completion
            self.assertEqual(emitted_span, span)
            self.assertIn("gen_ai.prompt", emitted_attributes)
            self.assertIn("gen_ai.completion", emitted_attributes)
            self.assertEqual(emitted_attributes["gen_ai.prompt"], "What is quantum computing?")
            self.assertEqual(emitted_attributes["gen_ai.completion"], "Quantum computing is...")

            # Verify span attributes are filtered
            self.assertNotIn("gen_ai.prompt", result[0]._attributes)
            self.assertIn("normal.attribute", result[0]._attributes)

            # Verify event attributes are filtered
            updated_event = result[0]._events[0]
            self.assertNotIn("gen_ai.completion", updated_event.attributes)
            self.assertIn("other.attribute", updated_event.attributes)

    def test_process_spans_multiple_events_consolidated(self):
        """
        Verify process_spans handles multiple events correctly, collecting all LLO attributes
        into a single consolidated event.
        """
        span_attributes = {"normal.attribute": "keep this"}

        # First event with prompt
        event1_attrs = {"gen_ai.prompt": "First question"}
        event1 = SpanEvent(
            name="gen_ai.content.prompt",
            attributes=event1_attrs,
            timestamp=1234567890,
        )

        # Second event with completion
        event2_attrs = {"gen_ai.completion": "First answer"}
        event2 = SpanEvent(
            name="gen_ai.content.completion",
            attributes=event2_attrs,
            timestamp=1234567891,
        )

        span = self._create_mock_span(span_attributes)
        span.events = [event1, event2]
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "openlit.otel.tracing"

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit:
            self.llo_handler.process_spans([span])

            # Should emit once with attributes from both events
            mock_emit.assert_called_once()
            emitted_attributes = mock_emit.call_args[0][1]

            self.assertIn("gen_ai.prompt", emitted_attributes)
            self.assertIn("gen_ai.completion", emitted_attributes)
            self.assertEqual(emitted_attributes["gen_ai.prompt"], "First question")
            self.assertEqual(emitted_attributes["gen_ai.completion"], "First answer")
