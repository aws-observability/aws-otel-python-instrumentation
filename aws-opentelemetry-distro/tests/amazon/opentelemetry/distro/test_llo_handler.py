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
        Test _extract_traceloop_events with standard Traceloop attributes
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

    def test_extract_traceloop_all_attributes(self):
        """
        Test _extract_traceloop_events with all Traceloop attributes including CrewAI outputs
        """
        attributes = {
            "traceloop.entity.input": "input data",
            "traceloop.entity.output": "output data",
            "crewai.crew.tasks_output": "[TaskOutput(description='Task 1', output='Result 1')]",
            "crewai.crew.result": "Final crew result",
            "traceloop.entity.name": "crewai_agent",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_traceloop_events(span, attributes)

        self.assertEqual(len(events), 4)

        # Get a map of original attributes to events
        events_by_attr = {event.attributes["original_attribute"]: event for event in events}

        # Check all expected attributes are present
        self.assertIn("traceloop.entity.input", events_by_attr)
        self.assertIn("traceloop.entity.output", events_by_attr)
        self.assertIn("crewai.crew.tasks_output", events_by_attr)
        self.assertIn("crewai.crew.result", events_by_attr)

        # Check standard Traceloop events
        input_event = events_by_attr["traceloop.entity.input"]
        self.assertEqual(input_event.name, "gen_ai.crewai_agent.message")
        self.assertEqual(input_event.body["role"], "user")

        output_event = events_by_attr["traceloop.entity.output"]
        self.assertEqual(output_event.name, "gen_ai.crewai_agent.message")
        self.assertEqual(output_event.body["role"], "assistant")

        # Check CrewAI events
        tasks_event = events_by_attr["crewai.crew.tasks_output"]
        self.assertEqual(tasks_event.name, "gen_ai.assistant.message")
        self.assertEqual(tasks_event.body["role"], "assistant")

        result_event = events_by_attr["crewai.crew.result"]
        self.assertEqual(result_event.name, "gen_ai.assistant.message")
        self.assertEqual(result_event.body["role"], "assistant")

    def test_extract_openlit_direct_prompt(self):
        """
        Test _extract_openlit_span_event_attributes with direct prompt attribute
        """
        attributes = {"gen_ai.prompt": "user direct prompt", "gen_ai.system": "openlit"}

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.user.message")
        self.assertEqual(event.body["content"], "user direct prompt")
        self.assertEqual(event.body["role"], "user")
        self.assertEqual(event.attributes["gen_ai.system"], "openlit")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.prompt")
        self.assertEqual(event.timestamp, 1234567890)  # start_time

    def test_extract_openlit_direct_completion(self):
        """
        Test _extract_openlit_span_event_attributes with direct completion attribute
        """
        attributes = {"gen_ai.completion": "assistant direct completion", "gen_ai.system": "openlit"}

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.assistant.message")
        self.assertEqual(event.body["content"], "assistant direct completion")
        self.assertEqual(event.body["role"], "assistant")
        self.assertEqual(event.attributes["gen_ai.system"], "openlit")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.completion")
        self.assertEqual(event.timestamp, 1234567899)  # end_time

    def test_extract_openlit_all_attributes(self):
        """
        Test _extract_openlit_span_event_attributes with all OpenLit attributes
        """
        attributes = {
            "gen_ai.prompt": "user prompt",
            "gen_ai.completion": "assistant response",
            "gen_ai.content.revised_prompt": "revised prompt",
            "gen_ai.agent.actual_output": "agent output",
            "gen_ai.agent.human_input": "human input to agent",
            "gen_ai.system": "langchain",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 5)

        # Check that all events have the correct system
        for event in events:
            self.assertEqual(event.attributes["gen_ai.system"], "langchain")

        # Check we have the expected event types
        event_types = {event.name for event in events}
        self.assertIn("gen_ai.user.message", event_types)
        self.assertIn("gen_ai.assistant.message", event_types)
        self.assertIn("gen_ai.system.message", event_types)

        # Verify counts of user messages (should be 2 - prompt and human input)
        user_events = [e for e in events if e.name == "gen_ai.user.message"]
        self.assertEqual(len(user_events), 2)

        # Check original attributes
        original_attrs = {event.attributes["original_attribute"] for event in events}
        self.assertIn("gen_ai.prompt", original_attrs)
        self.assertIn("gen_ai.completion", original_attrs)
        self.assertIn("gen_ai.content.revised_prompt", original_attrs)
        self.assertIn("gen_ai.agent.actual_output", original_attrs)
        self.assertIn("gen_ai.agent.human_input", original_attrs)

    def test_extract_openlit_revised_prompt(self):
        """
        Test _extract_openlit_span_event_attributes with revised prompt attribute
        """
        attributes = {"gen_ai.content.revised_prompt": "revised system prompt", "gen_ai.system": "openlit"}

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.system.message")
        self.assertEqual(event.body["content"], "revised system prompt")
        self.assertEqual(event.body["role"], "system")
        self.assertEqual(event.attributes["gen_ai.system"], "openlit")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.content.revised_prompt")
        self.assertEqual(event.timestamp, 1234567890)  # start_time

    def test_extract_openinference_direct_attributes(self):
        """
        Test _extract_openinference_attributes with direct input/output values
        """
        attributes = {
            "input.value": "user prompt",
            "output.value": "assistant response",
            "llm.model_name": "gpt-4",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openinference_attributes(span, attributes)

        self.assertEqual(len(events), 2)

        input_event = events[0]
        self.assertEqual(input_event.name, "gen_ai.user.message")
        self.assertEqual(input_event.body["content"], "user prompt")
        self.assertEqual(input_event.body["role"], "user")
        self.assertEqual(input_event.attributes["gen_ai.system"], "gpt-4")
        self.assertEqual(input_event.attributes["original_attribute"], "input.value")
        self.assertEqual(input_event.timestamp, 1234567890)  # start_time

        output_event = events[1]
        self.assertEqual(output_event.name, "gen_ai.assistant.message")
        self.assertEqual(output_event.body["content"], "assistant response")
        self.assertEqual(output_event.body["role"], "assistant")
        self.assertEqual(output_event.attributes["gen_ai.system"], "gpt-4")
        self.assertEqual(output_event.attributes["original_attribute"], "output.value")
        self.assertEqual(output_event.timestamp, 1234567899)  # end_time

    def test_extract_openinference_structured_input_messages(self):
        """
        Test _extract_openinference_attributes with structured input messages
        """
        attributes = {
            "llm.input_messages.0.message.content": "system prompt",
            "llm.input_messages.0.message.role": "system",
            "llm.input_messages.1.message.content": "user message",
            "llm.input_messages.1.message.role": "user",
            "llm.model_name": "claude-3",
        }

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_openinference_attributes(span, attributes)

        self.assertEqual(len(events), 2)

        system_event = events[0]
        self.assertEqual(system_event.name, "gen_ai.system.message")
        self.assertEqual(system_event.body["content"], "system prompt")
        self.assertEqual(system_event.body["role"], "system")
        self.assertEqual(system_event.attributes["gen_ai.system"], "claude-3")
        self.assertEqual(system_event.attributes["original_attribute"], "llm.input_messages.0.message.content")

        user_event = events[1]
        self.assertEqual(user_event.name, "gen_ai.user.message")
        self.assertEqual(user_event.body["content"], "user message")
        self.assertEqual(user_event.body["role"], "user")
        self.assertEqual(user_event.attributes["gen_ai.system"], "claude-3")
        self.assertEqual(user_event.attributes["original_attribute"], "llm.input_messages.1.message.content")

    def test_extract_openinference_structured_output_messages(self):
        """
        Test _extract_openinference_attributes with structured output messages
        """
        attributes = {
            "llm.output_messages.0.message.content": "assistant response",
            "llm.output_messages.0.message.role": "assistant",
            "llm.model_name": "llama-3",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openinference_attributes(span, attributes)

        self.assertEqual(len(events), 1)

        output_event = events[0]
        self.assertEqual(output_event.name, "gen_ai.assistant.message")
        self.assertEqual(output_event.body["content"], "assistant response")
        self.assertEqual(output_event.body["role"], "assistant")
        self.assertEqual(output_event.attributes["gen_ai.system"], "llama-3")
        self.assertEqual(output_event.attributes["original_attribute"], "llm.output_messages.0.message.content")
        self.assertEqual(output_event.timestamp, 1234567899)  # end_time

    def test_extract_openinference_mixed_attributes(self):
        """
        Test _extract_openinference_attributes with a mix of all attribute types
        """
        attributes = {
            "input.value": "direct input",
            "output.value": "direct output",
            "llm.input_messages.0.message.content": "message input",
            "llm.input_messages.0.message.role": "user",
            "llm.output_messages.0.message.content": "message output",
            "llm.output_messages.0.message.role": "assistant",
            "llm.model_name": "bedrock.claude-3",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openinference_attributes(span, attributes)

        self.assertEqual(len(events), 4)

        # Verify all events have the correct model name
        for event in events:
            self.assertEqual(event.attributes["gen_ai.system"], "bedrock.claude-3")

        # We don't need to check every detail since other tests do that,
        # but we can verify we got all the expected event types
        event_types = {event.name for event in events}
        self.assertIn("gen_ai.user.message", event_types)
        self.assertIn("gen_ai.assistant.message", event_types)

        # Verify original attributes were correctly captured
        original_attrs = {event.attributes["original_attribute"] for event in events}
        self.assertIn("input.value", original_attrs)
        self.assertIn("output.value", original_attrs)
        self.assertIn("llm.input_messages.0.message.content", original_attrs)
        self.assertIn("llm.output_messages.0.message.content", original_attrs)

    def test_extract_openlit_agent_actual_output(self):
        """
        Test _extract_openlit_span_event_attributes with agent actual output attribute
        """
        attributes = {"gen_ai.agent.actual_output": "Agent task output result", "gen_ai.system": "crewai"}

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event.name, "gen_ai.assistant.message")
        self.assertEqual(event.body["content"], "Agent task output result")
        self.assertEqual(event.body["role"], "assistant")
        self.assertEqual(event.attributes["gen_ai.system"], "crewai")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.agent.actual_output")
        self.assertEqual(event.timestamp, 1234567899)  # end_time

    def test_extract_openlit_agent_human_input(self):
        """
        Test _extract_openlit_span_event_attributes with agent human input attribute
        """
        attributes = {"gen_ai.agent.human_input": "Human input to the agent", "gen_ai.system": "crewai"}

        span = self._create_mock_span(attributes)

        events = self.llo_handler._extract_openlit_span_event_attributes(span, attributes)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.name, "gen_ai.user.message")
        self.assertEqual(event.body["content"], "Human input to the agent")
        self.assertEqual(event.body["role"], "user")
        self.assertEqual(event.attributes["gen_ai.system"], "crewai")
        self.assertEqual(event.attributes["original_attribute"], "gen_ai.agent.human_input")
        self.assertEqual(event.timestamp, 1234567890)  # start_time

    def test_extract_traceloop_crew_outputs(self):
        """
        Test _extract_traceloop_events with CrewAI specific attributes
        """
        attributes = {
            "crewai.crew.tasks_output": "[TaskOutput(description='Task description', output='Task result')]",
            "crewai.crew.result": "Final crew execution result",
            "traceloop.entity.name": "crewai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_traceloop_events(span, attributes)

        self.assertEqual(len(events), 2)

        # Get a map of original attributes to their content
        events_by_attr = {event.attributes["original_attribute"]: event for event in events}

        # Check the tasks output event
        self.assertIn("crewai.crew.tasks_output", events_by_attr)
        tasks_event = events_by_attr["crewai.crew.tasks_output"]
        self.assertEqual(tasks_event.name, "gen_ai.assistant.message")
        self.assertEqual(
            tasks_event.body["content"], "[TaskOutput(description='Task description', output='Task result')]"
        )
        self.assertEqual(tasks_event.body["role"], "assistant")
        self.assertEqual(tasks_event.attributes["gen_ai.system"], "crewai")
        self.assertEqual(tasks_event.timestamp, 1234567899)  # end_time

        # Check the result event
        self.assertIn("crewai.crew.result", events_by_attr)
        result_event = events_by_attr["crewai.crew.result"]
        self.assertEqual(result_event.name, "gen_ai.assistant.message")
        self.assertEqual(result_event.body["content"], "Final crew execution result")
        self.assertEqual(result_event.body["role"], "assistant")
        self.assertEqual(result_event.attributes["gen_ai.system"], "crewai")
        self.assertEqual(result_event.timestamp, 1234567899)  # end_time

    def test_extract_traceloop_crew_outputs_with_gen_ai_system(self):
        """
        Test _extract_traceloop_events with CrewAI specific attributes when gen_ai.system is available
        """
        attributes = {
            "crewai.crew.tasks_output": "[TaskOutput(description='Task description', output='Task result')]",
            "crewai.crew.result": "Final crew execution result",
            "traceloop.entity.name": "oldvalue",
            "gen_ai.system": "crewai-agent",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_traceloop_events(span, attributes)

        self.assertEqual(len(events), 2)

        # Get a map of original attributes to their content
        events_by_attr = {event.attributes["original_attribute"]: event for event in events}

        # Check the tasks output event
        self.assertIn("crewai.crew.tasks_output", events_by_attr)
        tasks_event = events_by_attr["crewai.crew.tasks_output"]
        self.assertEqual(tasks_event.name, "gen_ai.assistant.message")
        # Should use gen_ai.system attribute instead of traceloop.entity.name
        self.assertEqual(tasks_event.attributes["gen_ai.system"], "crewai-agent")

        # Check the result event
        self.assertIn("crewai.crew.result", events_by_attr)
        result_event = events_by_attr["crewai.crew.result"]
        self.assertEqual(result_event.name, "gen_ai.assistant.message")
        # Should use gen_ai.system attribute instead of traceloop.entity.name
        self.assertEqual(result_event.attributes["gen_ai.system"], "crewai-agent")

    def test_extract_traceloop_entity_with_gen_ai_system(self):
        """
        Test that traceloop.entity.input and traceloop.entity.output still use traceloop.entity.name
        even when gen_ai.system is available
        """
        attributes = {
            "traceloop.entity.input": "input data",
            "traceloop.entity.output": "output data",
            "traceloop.entity.name": "my_entity",
            "gen_ai.system": "should-not-be-used",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        events = self.llo_handler._extract_traceloop_events(span, attributes)

        self.assertEqual(len(events), 2)

        # Get a map of original attributes to their content
        events_by_attr = {event.attributes["original_attribute"]: event for event in events}

        # Regular traceloop entity attributes should still use traceloop.entity.name
        input_event = events_by_attr["traceloop.entity.input"]
        self.assertEqual(input_event.name, "gen_ai.my_entity.message")
        self.assertEqual(input_event.attributes["gen_ai.system"], "my_entity")

        output_event = events_by_attr["traceloop.entity.output"]
        self.assertEqual(output_event.name, "gen_ai.my_entity.message")
        self.assertEqual(output_event.attributes["gen_ai.system"], "my_entity")

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
            "gen_ai.agent.actual_output": "agent output",
            "crewai.crew.tasks_output": "tasks output",
            "crewai.crew.result": "crew result",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        with patch.object(self.llo_handler, "_extract_gen_ai_prompt_events") as mock_extract_prompt, patch.object(
            self.llo_handler, "_extract_gen_ai_completion_events"
        ) as mock_extract_completion, patch.object(
            self.llo_handler, "_extract_traceloop_events"
        ) as mock_extract_traceloop, patch.object(
            self.llo_handler, "_extract_openlit_span_event_attributes"
        ) as mock_extract_openlit, patch.object(
            self.llo_handler, "_extract_openinference_attributes"
        ) as mock_extract_openinference:

            # Create mocks with name attribute properly set
            prompt_event = MagicMock(spec=Event)
            prompt_event.name = "gen_ai.user.message"

            completion_event = MagicMock(spec=Event)
            completion_event.name = "gen_ai.assistant.message"

            traceloop_event = MagicMock(spec=Event)
            traceloop_event.name = "gen_ai.entity.message"

            openlit_event = MagicMock(spec=Event)
            openlit_event.name = "gen_ai.langchain.message"

            openinference_event = MagicMock(spec=Event)
            openinference_event.name = "gen_ai.anthropic.message"

            mock_extract_prompt.return_value = [prompt_event]
            mock_extract_completion.return_value = [completion_event]
            mock_extract_traceloop.return_value = [traceloop_event]
            mock_extract_openlit.return_value = [openlit_event]
            mock_extract_openinference.return_value = [openinference_event]

            self.llo_handler._emit_llo_attributes(span, attributes)

            mock_extract_prompt.assert_called_once_with(span, attributes, None)
            mock_extract_completion.assert_called_once_with(span, attributes, None)
            mock_extract_traceloop.assert_called_once_with(span, attributes, None)
            mock_extract_openlit.assert_called_once_with(span, attributes, None)
            mock_extract_openinference.assert_called_once_with(span, attributes, None)

            self.event_logger_mock.emit.assert_has_calls(
                [
                    call(prompt_event),
                    call(completion_event),
                    call(traceloop_event),
                    call(openlit_event),
                    call(openinference_event),
                ]
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
