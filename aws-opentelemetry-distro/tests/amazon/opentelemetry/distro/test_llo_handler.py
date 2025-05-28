from unittest import TestCase
from unittest.mock import MagicMock, call, patch

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

    def test_is_llo_attribute_traceloop_match(self):
        """
        Test _is_llo_attribute method with Traceloop patterns
        """
        # Test exact matches for Traceloop attributes
        self.assertTrue(self.llo_handler._is_llo_attribute("traceloop.entity.input"))
        self.assertTrue(self.llo_handler._is_llo_attribute("traceloop.entity.output"))

    def test_is_llo_attribute_openlit_match(self):
        """
        Test _is_llo_attribute method with OpenLit patterns
        """
        # Test exact matches for direct OpenLit attributes
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.completion"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.content.revised_prompt"))

    def test_is_llo_attribute_openinference_match(self):
        """
        Test _is_llo_attribute method with OpenInference patterns
        """
        # Test exact matches
        self.assertTrue(self.llo_handler._is_llo_attribute("input.value"))
        self.assertTrue(self.llo_handler._is_llo_attribute("output.value"))

        # Test regex matches
        self.assertTrue(self.llo_handler._is_llo_attribute("llm.input_messages.0.message.content"))
        self.assertTrue(self.llo_handler._is_llo_attribute("llm.output_messages.123.message.content"))

    def test_is_llo_attribute_crewai_match(self):
        """
        Test _is_llo_attribute method with CrewAI patterns
        """
        # Test exact match for CrewAI attributes (handled by Traceloop and OpenLit)
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.agent.actual_output"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.agent.human_input"))
        self.assertTrue(self.llo_handler._is_llo_attribute("crewai.crew.tasks_output"))
        self.assertTrue(self.llo_handler._is_llo_attribute("crewai.crew.result"))

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

    def test_emit_llo_attributes_with_indexed_messages(self):
        """
        Test _emit_llo_attributes with multiple indexed messages
        """
        attributes = {
            "gen_ai.prompt.0.content": "first user message",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.prompt.1.content": "second user message",
            "gen_ai.prompt.1.role": "user",
            "gen_ai.prompt.2.content": "system instruction",
            "gen_ai.prompt.2.role": "system",
            "gen_ai.completion.0.content": "first assistant response",
            "gen_ai.completion.0.role": "assistant",
            "gen_ai.completion.1.content": "second assistant response",
            "gen_ai.completion.1.role": "assistant",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Should emit exactly one consolidated event
        self.event_logger_mock.emit.assert_called_once()

        # Get the emitted event
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify event properties
        self.assertEqual(emitted_event.name, "gen_ai.content.consolidated")

        # Verify the consolidated body structure maintains order
        body = emitted_event.body

        # User messages should be indexed 0, 1
        self.assertEqual(body["user.message.0"]["content"], "first user message")
        self.assertEqual(body["user.message.1"]["content"], "second user message")

        # System message should be indexed 0
        self.assertEqual(body["system.message.0"]["content"], "system instruction")

        # Assistant messages should be indexed 0, 1
        self.assertEqual(body["assistant.message.0"]["content"], "first assistant response")
        self.assertEqual(body["assistant.message.1"]["content"], "second assistant response")

    def test_collect_gen_ai_prompt_messages(self):
        """
        Test _collect_gen_ai_prompt_messages
        """
        attributes = {
            "gen_ai.prompt.0.content": "system instruction",
            "gen_ai.prompt.0.role": "system",
            "gen_ai.prompt.1.content": "user question",
            "gen_ai.prompt.1.role": "user",
            "gen_ai.prompt.2.content": "assistant context",
            "gen_ai.prompt.2.role": "assistant",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_gen_ai_prompt_messages(span, attributes)

        self.assertEqual(len(messages), 3)
        # Should be sorted by index
        self.assertEqual(messages[0], {"content": "system instruction", "role": "system"})
        self.assertEqual(messages[1], {"content": "user question", "role": "user"})
        self.assertEqual(messages[2], {"content": "assistant context", "role": "assistant"})

    def test_collect_gen_ai_completion_messages(self):
        """
        Test _collect_gen_ai_completion_messages
        """
        attributes = {
            "gen_ai.completion.0.content": "first response",
            "gen_ai.completion.0.role": "assistant",
            "gen_ai.completion.1.content": "second response",
            "gen_ai.completion.1.role": "assistant",
            "gen_ai.system": "anthropic",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_gen_ai_completion_messages(span, attributes)

        self.assertEqual(len(messages), 2)
        # Should be sorted by index
        self.assertEqual(messages[0], {"content": "first response", "role": "assistant"})
        self.assertEqual(messages[1], {"content": "second response", "role": "assistant"})

    def test_collect_traceloop_messages(self):
        """
        Test _collect_traceloop_messages
        """
        attributes = {
            "traceloop.entity.input": "input data",
            "traceloop.entity.output": "output data",
            "crewai.crew.tasks_output": "tasks output",
            "crewai.crew.result": "crew result",
            "traceloop.entity.name": "my_entity",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_traceloop_messages(span, attributes)

        self.assertEqual(len(messages), 4)
        self.assertEqual(messages[0], {"content": "input data", "role": "user"})
        self.assertEqual(messages[1], {"content": "output data", "role": "assistant"})
        self.assertEqual(messages[2], {"content": "tasks output", "role": "assistant"})
        self.assertEqual(messages[3], {"content": "crew result", "role": "assistant"})

    def test_collect_openlit_messages(self):
        """
        Test _collect_openlit_messages
        """
        attributes = {
            "gen_ai.prompt": "user prompt",
            "gen_ai.completion": "assistant response",
            "gen_ai.content.revised_prompt": "revised prompt",
            "gen_ai.agent.actual_output": "agent output",
            "gen_ai.agent.human_input": "human input",
            "gen_ai.system": "langchain",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_openlit_messages(span, attributes)

        self.assertEqual(len(messages), 5)
        # Check all messages are present with correct roles
        self.assertIn({"content": "user prompt", "role": "user"}, messages)
        self.assertIn({"content": "assistant response", "role": "assistant"}, messages)
        self.assertIn({"content": "revised prompt", "role": "system"}, messages)
        self.assertIn({"content": "agent output", "role": "assistant"}, messages)
        self.assertIn({"content": "human input", "role": "user"}, messages)

    def test_collect_openinference_messages(self):
        """
        Test _collect_openinference_messages
        """
        attributes = {
            "input.value": "direct input",
            "output.value": "direct output",
            "llm.input_messages.0.message.content": "system prompt",
            "llm.input_messages.0.message.role": "system",
            "llm.input_messages.1.message.content": "user message",
            "llm.input_messages.1.message.role": "user",
            "llm.output_messages.0.message.content": "assistant response",
            "llm.output_messages.0.message.role": "assistant",
            "llm.model_name": "gpt-4",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_openinference_messages(span, attributes)

        self.assertEqual(len(messages), 5)
        # Direct values come first
        self.assertEqual(messages[0], {"content": "direct input", "role": "user"})
        self.assertEqual(messages[1], {"content": "direct output", "role": "assistant"})
        # Then structured messages in order
        self.assertEqual(messages[2], {"content": "system prompt", "role": "system"})
        self.assertEqual(messages[3], {"content": "user message", "role": "user"})
        self.assertEqual(messages[4], {"content": "assistant response", "role": "assistant"})

    def test_emit_llo_attributes_no_llo_attributes(self):
        """
        Test _emit_llo_attributes with no LLO attributes
        """
        attributes = {
            "normal.attribute": "value",
            "another.attribute": 123,
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Should not emit any events
        self.event_logger_mock.emit.assert_not_called()

    def test_emit_llo_attributes(self):
        """
        Test _emit_llo_attributes with consolidated event
        """
        attributes = {
            "gen_ai.prompt.0.content": "prompt content",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "completion content",
            "gen_ai.completion.0.role": "assistant",
            "traceloop.entity.input": "traceloop input",
            "traceloop.entity.name": "entity_name",
            "gen_ai.system": "openai",
            "gen_ai.agent.actual_output": "agent output",
            "crewai.crew.tasks_output": "tasks output",
            "crewai.crew.result": "crew result",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Should emit exactly one consolidated event
        self.event_logger_mock.emit.assert_called_once()

        # Get the emitted event
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify event properties
        self.assertEqual(emitted_event.name, "gen_ai.content.consolidated")
        self.assertEqual(emitted_event.timestamp, 1234567899)  # span.end_time
        self.assertEqual(emitted_event.attributes["gen_ai.system"], "openai")

        # Verify the consolidated body structure
        body = emitted_event.body

        # Collect all user messages
        user_messages = []
        for key, value in body.items():
            if key.startswith("user.message.") and value["role"] == "user":
                user_messages.append(value["content"])

        # Collect all assistant messages
        assistant_messages = []
        for key, value in body.items():
            if key.startswith("assistant.message.") and value["role"] == "assistant":
                assistant_messages.append(value["content"])

        # Check that we have the expected number of messages
        self.assertEqual(len(user_messages), 2)
        self.assertEqual(len(assistant_messages), 4)

        # Check that all expected content is present (order may vary)
        self.assertIn("prompt content", user_messages)
        self.assertIn("traceloop input", user_messages)

        self.assertIn("completion content", assistant_messages)
        self.assertIn("agent output", assistant_messages)
        self.assertIn("tasks output", assistant_messages)
        self.assertIn("crew result", assistant_messages)

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

    def test_emit_llo_attributes_mixed_frameworks(self):
        """
        Test _emit_llo_attributes with attributes from multiple frameworks
        """
        attributes = {
            # Standard Gen AI
            "gen_ai.prompt.0.content": "What's the weather?",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "It's sunny today.",
            "gen_ai.completion.0.role": "assistant",
            # Traceloop
            "traceloop.entity.input": "Calculate 2+2",
            "traceloop.entity.output": "The answer is 4",
            # OpenLit
            "gen_ai.prompt": "Direct prompt",
            "gen_ai.completion": "Direct completion",
            # OpenInference
            "input.value": "Simple input",
            "output.value": "Simple output",
            "gen_ai.system": "mixed-framework",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Should emit exactly one consolidated event
        self.event_logger_mock.emit.assert_called_once()

        # Get the emitted event
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify we have messages from all frameworks
        body = emitted_event.body

        # Count total messages by role
        user_messages = [k for k in body.keys() if k.startswith("user.message.")]
        assistant_messages = [k for k in body.keys() if k.startswith("assistant.message.")]

        # We should have 4 user messages: gen_ai.prompt.0, traceloop.entity.input, gen_ai.prompt, input.value
        self.assertEqual(len(user_messages), 4)

        # We should have 4 assistant messages: gen_ai.completion.0, traceloop.entity.output, gen_ai.completion, output.value
        self.assertEqual(len(assistant_messages), 4)

    def test_emit_llo_attributes_with_event_timestamp(self):
        """
        Test _emit_llo_attributes with custom event timestamp
        """
        attributes = {
            "gen_ai.prompt.0.content": "test prompt",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.system": "openai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        custom_timestamp = 9999999999

        self.llo_handler._emit_llo_attributes(span, attributes, event_timestamp=custom_timestamp)

        # Should emit exactly one consolidated event
        self.event_logger_mock.emit.assert_called_once()

        # Get the emitted event
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Should use the custom timestamp
        self.assertEqual(emitted_event.timestamp, custom_timestamp)

    def test_emit_llo_attributes_with_tool_role(self):
        """
        Test _emit_llo_attributes with tool role (custom role)
        """
        attributes = {
            "gen_ai.prompt.0.content": "Get flight info for booking C46E9F",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": '[{"ticket_no": "7240005432906569", "flight_no": "LX0112"}]',
            "gen_ai.completion.0.role": "tool",
            "gen_ai.completion.1.content": "Your flight LX0112 departs at 15:11",
            "gen_ai.completion.1.role": "assistant",
            "gen_ai.system": "travel-assistant",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Should emit exactly one consolidated event
        self.event_logger_mock.emit.assert_called_once()

        # Get the emitted event
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify the consolidated body includes the tool role
        body = emitted_event.body

        # Should have user, tool, and assistant messages
        self.assertIn("user.message.0", body)
        self.assertEqual(body["user.message.0"]["role"], "user")

        self.assertIn("tool.message.0", body)
        self.assertEqual(body["tool.message.0"]["role"], "tool")
        self.assertEqual(
            body["tool.message.0"]["content"], '[{"ticket_no": "7240005432906569", "flight_no": "LX0112"}]'
        )

        self.assertIn("assistant.message.0", body)
        self.assertEqual(body["assistant.message.0"]["role"], "assistant")

    def test_emit_llo_attributes_system_identification(self):
        """
        Test system identification fallback logic
        """
        # Test 1: Uses gen_ai.system when available
        attributes = {
            "gen_ai.prompt.0.content": "test",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.system": "primary-system",
            "llm.model_name": "fallback-model",
            "traceloop.entity.name": "fallback-entity",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        emitted_event = self.event_logger_mock.emit.call_args[0][0]
        self.assertEqual(emitted_event.attributes["gen_ai.system"], "primary-system")

        # Test 2: Falls back to unknown when gen_ai.system not available
        self.event_logger_mock.reset_mock()
        attributes_without_system = {
            "gen_ai.prompt.0.content": "test",
            "gen_ai.prompt.0.role": "user",
        }

        span_without_system = self._create_mock_span(attributes_without_system)
        span_without_system.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span_without_system, attributes_without_system)

        emitted_event = self.event_logger_mock.emit.call_args[0][0]
        self.assertEqual(emitted_event.attributes["gen_ai.system"], "unknown")

    def test_emit_llo_attributes_trace_context(self):
        """
        Test that trace context is properly preserved in the event
        """
        attributes = {
            "gen_ai.prompt.0.content": "test prompt",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.system": "test-system",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify trace context
        self.assertEqual(emitted_event.trace_id, span.context.trace_id)
        self.assertEqual(emitted_event.span_id, span.context.span_id)
        self.assertEqual(emitted_event.trace_flags, span.context.trace_flags)

    def test_transformation_example_from_specification(self):
        """
        Test the exact transformation example from the specification
        """
        attributes = {
            "gen_ai.prompt.0.content": "Hello",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.prompt.1.content": "How are you?",
            "gen_ai.prompt.1.role": "user",
            "traceloop.entity.input": "Another greeting",
            "gen_ai.completion.0.content": "I'm doing well",
            "gen_ai.completion.0.role": "assistant",
            "gen_ai.system": "test-system",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        self.llo_handler._emit_llo_attributes(span, attributes)

        emitted_event = self.event_logger_mock.emit.call_args[0][0]
        body = emitted_event.body

        # Verify the exact transformation from the spec
        self.assertEqual(body["user.message.0"], {"role": "user", "content": "Hello"})
        self.assertEqual(body["user.message.1"], {"role": "user", "content": "How are you?"})
        self.assertEqual(body["user.message.2"], {"role": "user", "content": "Another greeting"})
        self.assertEqual(body["assistant.message.0"], {"role": "assistant", "content": "I'm doing well"})
