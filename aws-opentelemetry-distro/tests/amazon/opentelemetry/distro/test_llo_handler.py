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
            "another.normal.attribute": 123,
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
            "gen_ai.system": "openai",
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
            "gen_ai.system": "anthropic",
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
            "gen_ai.system": "anthropic",
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
        attributes = {
            "gen_ai.prompt.2.content": "function data",
            "gen_ai.prompt.2.role": "function",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        events = self.llo_handler._extract_gen_ai_prompt_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.openai.message")
        self.assertEqual(event.body["content"], "function data")
        self.assertEqual(event.body["role"], "function")
        self.assertEqual(event.attributes["gen_ai.system"], "openai")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.prompt.2.content")

    def test_extract_gen_ai_prompt_events_unknown_role(self):
        """
        Test _extract_gen_ai_prompt_events with unknown role
        """
        attributes = {
            "gen_ai.prompt.3.content": "unknown type content",
            "gen_ai.prompt.3.role": "unknown",
            "gen_ai.system": "bedrock",
        }

        span = self._create_mock_span(attributes)
        events = self.llo_handler._extract_gen_ai_prompt_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.bedrock.message")
        self.assertEqual(event.body["content"], "unknown type content")
        self.assertEqual(event.body["role"], "unknown")
        self.assertEqual(event.attributes["gen_ai.system"], "bedrock")

    def test_extract_gen_ai_completion_events_assistant_role(self):
        """
        Test _extract_gen_ai_completion_events with assistant role
        """
        attributes = {
            "gen_ai.completion.0.content": "assistant completion",
            "gen_ai.completion.0.role": "assistant",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899  # end time for completion events

        events = self.llo_handler._extract_gen_ai_completion_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.assistant.message")
        self.assertEqual(event.body["content"], "assistant completion")
        self.assertEqual(event.body["role"], "assistant")
        self.assertEqual(event.attributes["gen_ai.system"], "openai")
        self.assertEqual(event.timestamp, 1234567899)

    def test_extract_gen_ai_completion_events_other_role(self):
        """
        Test _extract_gen_ai_completion_events with non-assistant role
        """
        attributes = {
            "gen_ai.completion.1.content": "other completion",
            "gen_ai.completion.1.role": "other",
            "gen_ai.system": "anthropic",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_gen_ai_completion_events(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.anthropic.message")
        self.assertEqual(event.body["content"], "other completion")
        self.assertEqual(event.attributes["gen_ai.system"], "anthropic")

    def test_extract_traceloop_events(self):
        """
        Test _extract_traceloop_events
        """
        attributes = {
            "traceloop.entity.input": "input data",
            "traceloop.entity.output": "output data",
            "traceloop.entity.name": "my_entity",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_traceloop_events(span, attributes)

        self.assertEqual(len(events), 2)

        input_event = events[0]
        self.assertEqual(input_event.name, "gen_ai.my_entity.message")
        self.assertEqual(input_event.body["content"], "input data")
        self.assertEqual(input_event.attributes["gen_ai.system"], "my_entity")
        self.assertEqual(input_event.attributes["original_attribute"], "traceloop.entity.input")
        self.assertEqual(input_event.timestamp, 1234567890)  # start_time

        output_event = events[1]
        self.assertEqual(output_event.name, "gen_ai.my_entity.message")
        self.assertEqual(output_event.body["content"], "output data")
        self.assertEqual(output_event.attributes["gen_ai.system"], "my_entity")
        self.assertEqual(output_event.attributes["original_attribute"], "traceloop.entity.output")
        self.assertEqual(output_event.timestamp, 1234567899)  # end_time

    def test_emit_llo_attributes(self):
        """
        Test _emit_llo_attributes
        """
        attributes = {
            "gen_ai.prompt.0.content": "prompt content",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "completion content",
            "gen_ai.completion.0.role": "assistant",
            "traceloop.entity.input": "traceloop input",
            "traceloop.entity.name": "entity_name",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        with patch.object(self.llo_handler, "_extract_gen_ai_prompt_events") as mock_extract_prompt, patch.object(
            self.llo_handler, "_extract_gen_ai_completion_events"
        ) as mock_extract_completion, patch.object(
            self.llo_handler, "_extract_traceloop_events"
        ) as mock_extract_traceloop, patch.object(
            self.llo_handler, "_extract_openlit_span_event_attributes"
        ) as mock_extract_openlit:

            # Create mocks with name attribute properly set
            prompt_event = MagicMock(spec=Event)
            prompt_event.name = "gen_ai.user.message"  # Set the name attribute

            completion_event = MagicMock(spec=Event)
            completion_event.name = "gen_ai.assistant.message"  # Set the name attribute

            traceloop_event = MagicMock(spec=Event)
            traceloop_event.name = "gen_ai.entity.message"  # Set the name attribute

            openlit_event = MagicMock(spec=Event)
            openlit_event.name = "gen_ai.langchain.message"

            mock_extract_prompt.return_value = [prompt_event]
            mock_extract_completion.return_value = [completion_event]
            mock_extract_traceloop.return_value = [traceloop_event]
            mock_extract_openlit.return_value = [openlit_event]

            self.llo_handler._emit_llo_attributes(span, attributes)

            mock_extract_prompt.assert_called_once_with(span, attributes, None)
            mock_extract_completion.assert_called_once_with(span, attributes, None)
            mock_extract_traceloop.assert_called_once_with(span, attributes, None)
            mock_extract_openlit.assert_called_once_with(span, attributes, None)

            self.event_logger_mock.emit.assert_has_calls(
                [call(prompt_event), call(completion_event), call(traceloop_event), call(openlit_event)]
            )

    def test_process_spans(self):
        """
        Test process_spans
        """
        attributes = {"gen_ai.prompt.0.content": "prompt content", "normal.attribute": "normal value"}

        span = self._create_mock_span(attributes)

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit, patch.object(
            self.llo_handler, "_filter_attributes"
        ) as mock_filter:

            filtered_attributes = {"normal.attribute": "normal value"}
            mock_filter.return_value = filtered_attributes

            result = self.llo_handler.process_spans([span])

            mock_emit.assert_called_once_with(span, attributes)
            mock_filter.assert_called_once_with(attributes)

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], span)
            # Access the _attributes property that was set by the process_spans method
            self.assertEqual(result[0]._attributes, filtered_attributes)

    def test_process_spans_with_bounded_attributes(self):
        """
        Test process_spans with BoundedAttributes
        """
        from opentelemetry.attributes import BoundedAttributes

        bounded_attrs = BoundedAttributes(
            maxlen=10,
            attributes={"gen_ai.prompt.0.content": "prompt content", "normal.attribute": "normal value"},
            immutable=False,
            max_value_len=1000,
        )

        span = self._create_mock_span(bounded_attrs)

        with patch.object(self.llo_handler, "_emit_llo_attributes") as mock_emit, patch.object(
            self.llo_handler, "_filter_attributes"
        ) as mock_filter:

            filtered_attributes = {"normal.attribute": "normal value"}
            mock_filter.return_value = filtered_attributes

            result = self.llo_handler.process_spans([span])

            mock_emit.assert_called_once_with(span, bounded_attrs)
            mock_filter.assert_called_once_with(bounded_attrs)

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], span)
            # Check that we got a BoundedAttributes instance
            self.assertIsInstance(result[0]._attributes, BoundedAttributes)
            # Check the underlying dictionary content
            self.assertEqual(dict(result[0]._attributes), filtered_attributes)
