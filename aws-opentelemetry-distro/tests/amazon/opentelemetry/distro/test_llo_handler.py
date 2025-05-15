from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from amazon.opentelemetry.distro.llo_handler import LLOHandler
from opentelemetry._events import Event
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import ReadableSpan, SpanContext
from opentelemetry.trace import SpanKind, TraceFlags, TraceState


class TestLLOHandler(TestCase):
    def setUp(self):
        self.logger_provider_mock = MagicMock(spec=LoggerProvider)
        self.event_logger_mock = MagicMock()
        self.event_logger_provider_mock = MagicMock()
        self.event_logger_provider_mock.get_event_logger.return_value = self.event_logger_mock

        with patch(
            "amazon.opentelemetry.distro.llo_handler.EventLoggerProvider", return_value=self.event_logger_provider_mock
        ):
            self.llo_handler = LLOHandler(self.logger_provider_mock)

    def _create_mock_span(self, attributes=None, kind=SpanKind.INTERNAL):
        """
        Helper method to create a mock span with given attributes
        """
        if attributes is None:
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

    def test_init(self):
        """
        Test initialization of LLOHandler
        """
        self.assertEqual(self.llo_handler._logger_provider, self.logger_provider_mock)
        self.assertEqual(self.llo_handler._event_logger_provider, self.event_logger_provider_mock)
        self.event_logger_provider_mock.get_event_logger.assert_called_once_with("gen_ai.events")

    def test_is_llo_attribute_match(self):
        """
        Test _is_llo_attribute method with matching patterns
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt.0.content"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt.123.content"))

    def test_is_llo_attribute_no_match(self):
        """
        Test _is_llo_attribute method with non-matching patterns
        """
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.prompt.content"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.prompt.abc.content"))
        self.assertFalse(self.llo_handler._is_llo_attribute("some.other.attribute"))

    def test_filter_attributes(self):
        """
        Test _filter_attributes method
        """
        attributes = {
            "gen_ai.prompt.0.content": "test content",
            "gen_ai.prompt.0.role": "user",
            "normal.attribute": "value",
            "another.normal.attribute": 123
        }

        filtered = self.llo_handler._filter_attributes(attributes)

        self.assertNotIn("gen_ai.prompt.0.content", filtered)
        self.assertIn("gen_ai.prompt.0.role", filtered)
        self.assertIn("normal.attribute", filtered)
        self.assertIn("another.normal.attribute", filtered)

    def test_extract_gen_ai_prompt_events_system_role(self):
        """
        Test _extract_gen_ai_prompt_events with system role
        """
        attributes = {
            "gen_ai.prompt.0.content": "system instruction",
            "gen_ai.prompt.0.role": "system",
            "gen_ai.system": "openai"
        }

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_gen_ai_prompt_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.system.message")
        self.assertEqual(event.body["content"], "system instruction")
        self.assertEqual(event.body["role"], "system")
        self.assertEqual(event.attributes["gen_ai.system"], "openai")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.prompt.0.content")

    def test_extract_gen_ai_prompt_events_user_role(self):
        """
        Test _extract_gen_ai_prompt_events with user role
        """
        attributes = {
            "gen_ai.prompt.0.content": "user question",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.system": "anthropic"
        }

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_gen_ai_prompt_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.user.message")
        self.assertEqual(event.body["content"], "user question")
        self.assertEqual(event.body["role"], "user")
        self.assertEqual(event.attributes["gen_ai.system"], "anthropic")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.prompt.0.content")

    def test_extract_gen_ai_prompt_events_assistant_role(self):
        """
        Test _extract_gen_ai_prompt_events with assistant role
        """
        attributes = {
            "gen_ai.prompt.1.content": "assistant response",
            "gen_ai.prompt.1.role": "assistant",
            "gen_ai.system": "anthropic"
        }

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_gen_ai_prompt_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.assistant.message")
        self.assertEqual(event.body["content"], "assistant response")
        self.assertEqual(event.body["role"], "assistant")
        self.assertEqual(event.attributes["gen_ai.system"], "anthropic")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.prompt.1.content")

    def test_extract_gen_ai_prompt_events_function_role(self):
        """
        Test _extract_gen_ai_prompt_events with function role
        """
        pass

    def test_emit_llo_attributes(self):
        pass

    def test_process_spans(self):
        pass
