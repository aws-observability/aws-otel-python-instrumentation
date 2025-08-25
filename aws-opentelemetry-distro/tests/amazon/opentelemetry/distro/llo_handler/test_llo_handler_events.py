# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for LLO Handler event emission functionality."""

from unittest.mock import MagicMock, patch

from test_llo_handler_base import LLOHandlerTestBase


class TestLLOHandlerEvents(LLOHandlerTestBase):  # pylint: disable=too-many-public-methods
    """Test event emission and formatting functionality."""

    def test_emit_llo_attributes(self):
        """
        Verify _emit_llo_attributes creates a single consolidated event with input/output message groups
        containing all LLO content from various frameworks.
        """
        attributes = {
            "gen_ai.prompt.0.content": "prompt content",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "completion content",
            "gen_ai.completion.0.role": "assistant",
            "traceloop.entity.input": "traceloop input",
            "traceloop.entity.name": "entity_name",
            "gen_ai.agent.actual_output": "agent output",
            "crewai.crew.tasks_output": "tasks output",
            "crewai.crew.result": "crew result",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        self.assertEqual(emitted_event.name, "test.scope")
        self.assertEqual(emitted_event.timestamp, span.end_time)
        self.assertEqual(emitted_event.trace_id, span.context.trace_id)
        self.assertEqual(emitted_event.span_id, span.context.span_id)
        self.assertEqual(emitted_event.trace_flags, span.context.trace_flags)

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)
        self.assertIn("messages", event_body["input"])
        self.assertIn("messages", event_body["output"])

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 2)

        user_prompt = next((msg for msg in input_messages if msg["content"] == "prompt content"), None)
        self.assertIsNotNone(user_prompt)
        self.assertEqual(user_prompt["role"], "user")

        traceloop_input = next((msg for msg in input_messages if msg["content"] == "traceloop input"), None)
        self.assertIsNotNone(traceloop_input)
        self.assertEqual(traceloop_input["role"], "user")

        output_messages = event_body["output"]["messages"]
        self.assertTrue(len(output_messages) >= 3)

        completion = next((msg for msg in output_messages if msg["content"] == "completion content"), None)
        self.assertIsNotNone(completion)
        self.assertEqual(completion["role"], "assistant")

        agent_output = next((msg for msg in output_messages if msg["content"] == "agent output"), None)
        self.assertIsNotNone(agent_output)
        self.assertEqual(agent_output["role"], "assistant")

    def test_emit_llo_attributes_multiple_frameworks(self):
        """
        Verify a single span containing LLO attributes from multiple frameworks
        (Traceloop, OpenLit, OpenInference, CrewAI) generates one consolidated event.
        """
        attributes = {
            "gen_ai.prompt.0.content": "Tell me about AI",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "AI is a field of computer science...",
            "gen_ai.completion.0.role": "assistant",
            "traceloop.entity.input": "What is machine learning?",
            "traceloop.entity.output": "Machine learning is a subset of AI...",
            "gen_ai.prompt": "Explain neural networks",
            "gen_ai.completion": "Neural networks are computing systems...",
            "input.value": "How do transformers work?",
            "output.value": "Transformers are a type of neural network architecture...",
            "crewai.crew.result": "Task completed successfully",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.multi.framework"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        self.assertEqual(emitted_event.name, "test.multi.framework")
        self.assertEqual(emitted_event.timestamp, span.end_time)

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        input_contents = [msg["content"] for msg in input_messages]
        self.assertIn("Tell me about AI", input_contents)
        self.assertIn("What is machine learning?", input_contents)
        self.assertIn("Explain neural networks", input_contents)
        self.assertIn("How do transformers work?", input_contents)

        output_messages = event_body["output"]["messages"]
        output_contents = [msg["content"] for msg in output_messages]
        self.assertIn("AI is a field of computer science...", output_contents)
        self.assertIn("Machine learning is a subset of AI...", output_contents)
        self.assertIn("Neural networks are computing systems...", output_contents)
        self.assertIn("Transformers are a type of neural network architecture...", output_contents)
        self.assertIn("Task completed successfully", output_contents)

        for msg in input_messages:
            self.assertIn(msg["role"], ["user", "system"])
        for msg in output_messages:
            self.assertEqual(msg["role"], "assistant")

    def test_emit_llo_attributes_no_llo_attributes(self):
        """
        Verify _emit_llo_attributes does not emit events when span contains only non-LLO attributes.
        """
        attributes = {
            "normal.attribute": "value",
            "another.attribute": 123,
        }

        span = self._create_mock_span(attributes)
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_not_called()

    def test_emit_llo_attributes_mixed_input_output(self):
        """
        Verify event generation correctly separates mixed input (system/user) and output (assistant) messages.
        """
        attributes = {
            "gen_ai.prompt.0.content": "system message",
            "gen_ai.prompt.0.role": "system",
            "gen_ai.prompt.1.content": "user message",
            "gen_ai.prompt.1.role": "user",
            "gen_ai.completion.0.content": "assistant response",
            "gen_ai.completion.0.role": "assistant",
            "input.value": "direct input",
            "output.value": "direct output",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 3)

        input_roles = [msg["role"] for msg in input_messages]
        self.assertIn("system", input_roles)
        self.assertIn("user", input_roles)

        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 2)

        for msg in output_messages:
            self.assertEqual(msg["role"], "assistant")

    def test_emit_llo_attributes_with_event_timestamp(self):
        """
        Verify _emit_llo_attributes uses provided event timestamp instead of span end time.
        """
        attributes = {
            "gen_ai.prompt": "test prompt",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        event_timestamp = 9999999999

        self.llo_handler._emit_llo_attributes(span, attributes, event_timestamp=event_timestamp)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]
        self.assertEqual(emitted_event.timestamp, event_timestamp)

    def test_emit_llo_attributes_none_attributes(self):
        """
        Test _emit_llo_attributes with None attributes - should return early
        """
        span = self._create_mock_span({})
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, None)

        self.event_logger_mock.emit.assert_not_called()

    def test_emit_llo_attributes_role_based_routing(self):
        """
        Test role-based routing for non-standard roles
        """
        attributes = {
            # Standard roles - should go to their expected places
            "gen_ai.prompt.0.content": "system prompt",
            "gen_ai.prompt.0.role": "system",
            "gen_ai.prompt.1.content": "user prompt",
            "gen_ai.prompt.1.role": "user",
            "gen_ai.completion.0.content": "assistant response",
            "gen_ai.completion.0.role": "assistant",
            # Non-standard roles - should be routed based on source
            "gen_ai.prompt.2.content": "function prompt",
            "gen_ai.prompt.2.role": "function",
            "gen_ai.completion.1.content": "tool completion",
            "gen_ai.completion.1.role": "tool",
            "gen_ai.prompt.3.content": "unknown prompt",
            "gen_ai.prompt.3.role": "custom_role",
            "gen_ai.completion.2.content": "unknown completion",
            "gen_ai.completion.2.role": "another_custom",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        # Verify event was emitted
        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body

        # Check input messages
        input_messages = event_body["input"]["messages"]
        input_contents = [msg["content"] for msg in input_messages]

        # Standard roles (system, user) should be in input
        self.assertIn("system prompt", input_contents)
        self.assertIn("user prompt", input_contents)

        # Non-standard roles from prompt source should be in input
        self.assertIn("function prompt", input_contents)
        self.assertIn("unknown prompt", input_contents)

        # Check output messages
        output_messages = event_body["output"]["messages"]
        output_contents = [msg["content"] for msg in output_messages]

        # Standard role (assistant) should be in output
        self.assertIn("assistant response", output_contents)

        # Non-standard roles from completion source should be in output
        self.assertIn("tool completion", output_contents)
        self.assertIn("unknown completion", output_contents)

    def test_emit_llo_attributes_empty_messages(self):
        """
        Test _emit_llo_attributes when messages list is empty after collection
        """
        # Create a span with attributes that would normally match patterns but with empty content
        attributes = {
            "gen_ai.prompt.0.content": "",
            "gen_ai.prompt.0.role": "user",
        }

        span = self._create_mock_span(attributes)
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        # Mock _collect_all_llo_messages to return empty list
        with patch.object(self.llo_handler, "_collect_all_llo_messages", return_value=[]):
            self.llo_handler._emit_llo_attributes(span, attributes)

            # Should not emit event when no messages collected
            self.event_logger_mock.emit.assert_not_called()

    def test_emit_llo_attributes_only_input_messages(self):
        """
        Test event generation when only input messages are present
        """
        attributes = {
            "gen_ai.prompt.0.content": "system instruction",
            "gen_ai.prompt.0.role": "system",
            "gen_ai.prompt.1.content": "user question",
            "gen_ai.prompt.1.role": "user",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body

        self.assertIn("input", event_body)
        self.assertNotIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 2)

    def test_emit_llo_attributes_only_output_messages(self):
        """
        Test event generation when only output messages are present
        """
        attributes = {
            "gen_ai.completion.0.content": "assistant response",
            "gen_ai.completion.0.role": "assistant",
            "output.value": "another output",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body

        self.assertNotIn("input", event_body)
        self.assertIn("output", event_body)

        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 2)

    def test_emit_llo_attributes_empty_event_body(self):
        """
        Test that no event is emitted when event body would be empty
        """
        # Create attributes that would result in messages with empty content
        attributes = {
            "gen_ai.prompt.0.content": "",
            "gen_ai.prompt.0.role": "user",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        # Mock _collect_all_llo_messages to return messages with empty content
        with patch.object(
            self.llo_handler,
            "_collect_all_llo_messages",
            return_value=[{"content": "", "role": "user", "source": "prompt"}],
        ):
            self.llo_handler._emit_llo_attributes(span, attributes)

            # Event should still be emitted as we have a message (even with empty content)
            self.event_logger_mock.emit.assert_called_once()

    def test_group_messages_by_type_standard_roles(self):
        """
        Test _group_messages_by_type correctly groups messages with standard roles.
        """
        messages = [
            {"role": "system", "content": "System message", "source": "prompt"},
            {"role": "user", "content": "User message", "source": "prompt"},
            {"role": "assistant", "content": "Assistant message", "source": "completion"},
        ]

        result = self.llo_handler._group_messages_by_type(messages)

        self.assertIn("input", result)
        self.assertIn("output", result)

        # Check input messages (system and user go to input)
        self.assertEqual(len(result["input"]), 2)
        self.assertEqual(result["input"][0], {"role": "system", "content": "System message"})
        self.assertEqual(result["input"][1], {"role": "user", "content": "User message"})

        # Check output messages
        self.assertEqual(len(result["output"]), 1)
        self.assertEqual(result["output"][0], {"role": "assistant", "content": "Assistant message"})

    def test_group_messages_by_type_non_standard_roles(self):
        """
        Test _group_messages_by_type correctly routes non-standard roles based on source.
        """
        messages = [
            {"role": "function", "content": "Function call", "source": "prompt"},
            {"role": "tool", "content": "Tool result", "source": "completion"},
            {"role": "custom", "content": "Custom output", "source": "output"},
            {"role": "other", "content": "Other result", "source": "result"},
        ]

        result = self.llo_handler._group_messages_by_type(messages)

        # Non-standard roles from prompt source go to input
        self.assertEqual(len(result["input"]), 1)
        self.assertEqual(result["input"][0], {"role": "function", "content": "Function call"})

        # Note: "tool" role from completion source is routed to output based on source
        # This differs from "tool" role from prompt source which goes to input as standard role
        # Non-standard roles from completion/output/result sources go to output
        self.assertEqual(len(result["output"]), 3)
        output_contents = [msg["content"] for msg in result["output"]]
        self.assertIn("Tool result", output_contents)
        self.assertIn("Custom output", output_contents)
        self.assertIn("Other result", output_contents)

    def test_group_messages_by_type_tool_role_routing(self):
        """
        Test _group_messages_by_type correctly routes tool role based on source.
        Tool messages from prompt source go to input (tool calls/instructions).
        Tool messages from completion/output source go to output (tool results).
        """
        messages = [
            {"role": "tool", "content": "Tool instruction", "source": "prompt"},
            {"role": "tool", "content": "Tool result", "source": "completion"},
            {"role": "tool", "content": "Another tool result", "source": "output"},
        ]

        result = self.llo_handler._group_messages_by_type(messages)

        # Tool from prompt source goes to input
        self.assertEqual(len(result["input"]), 1)
        self.assertEqual(result["input"][0], {"role": "tool", "content": "Tool instruction"})

        # Tool from completion/output sources go to output
        self.assertEqual(len(result["output"]), 2)
        output_contents = [msg["content"] for msg in result["output"]]
        self.assertIn("Tool result", output_contents)
        self.assertIn("Another tool result", output_contents)

    def test_group_messages_by_type_empty_list(self):
        """
        Test _group_messages_by_type handles empty message list.
        """
        result = self.llo_handler._group_messages_by_type([])

        self.assertEqual(result, {"input": [], "output": []})
        self.assertEqual(len(result["input"]), 0)
        self.assertEqual(len(result["output"]), 0)

    def test_group_messages_by_type_missing_fields(self):
        """
        Test _group_messages_by_type handles messages with missing role or content.
        """
        messages = [
            {"content": "No role", "source": "prompt"},  # Missing role
            {"role": "user", "source": "prompt"},  # Missing content
            {"role": "assistant", "content": "Complete message", "source": "completion"},
        ]

        result = self.llo_handler._group_messages_by_type(messages)

        # Message without role gets "unknown" role and goes to input (no completion/output/result in source)
        self.assertEqual(len(result["input"]), 2)
        self.assertEqual(result["input"][0], {"role": "unknown", "content": "No role"})
        self.assertEqual(result["input"][1], {"role": "user", "content": ""})

        # Complete message goes to output
        self.assertEqual(len(result["output"]), 1)
        self.assertEqual(result["output"][0], {"role": "assistant", "content": "Complete message"})

    def test_emit_llo_attributes_with_llm_prompts(self):
        """
        Test that llm.prompts attribute is properly emitted in the input section.
        """
        llm_prompts_content = "[{'role': 'system', 'content': [{'text': 'You are helpful.', 'type': 'text'}]}]"
        attributes = {
            "llm.prompts": llm_prompts_content,
            "gen_ai.completion.0.content": "I understand.",
            "gen_ai.completion.0.role": "assistant",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body

        # Check that llm.prompts is in input section
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 1)
        self.assertEqual(input_messages[0]["content"], llm_prompts_content)
        self.assertEqual(input_messages[0]["role"], "user")

        # Check output section has the completion
        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 1)
        self.assertEqual(output_messages[0]["content"], "I understand.")
        self.assertEqual(output_messages[0]["role"], "assistant")

    def test_emit_llo_attributes_openlit_style_events(self):
        """
        Test that LLO attributes from OpenLit-style span events are collected and emitted
        in a single consolidated event, not as separate events.
        """
        # This test simulates the OpenLit pattern where prompt and completion are in span events
        # The span processor should collect from both and emit a single event

        span_attributes = {"normal.attribute": "value"}

        # Create events like OpenLit does
        prompt_event_attrs = {"gen_ai.prompt": "Explain quantum computing"}
        prompt_event = MagicMock(attributes=prompt_event_attrs, timestamp=1234567890)

        completion_event_attrs = {"gen_ai.completion": "Quantum computing is..."}
        completion_event = MagicMock(attributes=completion_event_attrs, timestamp=1234567891)

        span = self._create_mock_span(span_attributes)
        span.events = [prompt_event, completion_event]
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "openlit.otel.tracing"

        # Process the span (this would normally be called by process_spans)
        all_llo_attrs = {}

        # Collect from span attributes
        for key, value in span_attributes.items():
            if self.llo_handler._is_llo_attribute(key):
                all_llo_attrs[key] = value

        # Collect from events
        for event in span.events:
            if event.attributes:
                for key, value in event.attributes.items():
                    if self.llo_handler._is_llo_attribute(key):
                        all_llo_attrs[key] = value

        # Emit consolidated event
        self.llo_handler._emit_llo_attributes(span, all_llo_attrs)

        # Verify single event was emitted with both input and output
        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body

        # Both input and output should be in the same event
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        # Check input section
        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 1)
        self.assertEqual(input_messages[0]["content"], "Explain quantum computing")
        self.assertEqual(input_messages[0]["role"], "user")

        # Check output section
        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 1)
        self.assertEqual(output_messages[0]["content"], "Quantum computing is...")
        self.assertEqual(output_messages[0]["role"], "assistant")

    def test_emit_llo_attributes_with_session_id(self):
        """
        Verify session.id attribute from span is copied to event attributes when present.
        """
        attributes = {
            "session.id": "test-session-123",
            "gen_ai.prompt": "Hello, AI",
            "gen_ai.completion": "Hello! How can I help you?",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify session.id was copied to event attributes
        self.assertIsNotNone(emitted_event.attributes)
        self.assertEqual(emitted_event.attributes.get("session.id"), "test-session-123")
        # Event class always adds event.name
        self.assertIn("event.name", emitted_event.attributes)

        # Verify event body still contains LLO data
        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

    def test_emit_llo_attributes_without_session_id(self):
        """
        Verify event attributes do not contain session.id when not present in span attributes.
        """
        attributes = {
            "gen_ai.prompt": "Hello, AI",
            "gen_ai.completion": "Hello! How can I help you?",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify session.id is not in event attributes
        self.assertIsNotNone(emitted_event.attributes)
        self.assertNotIn("session.id", emitted_event.attributes)
        # Event class always adds event.name
        self.assertIn("event.name", emitted_event.attributes)

    def test_emit_llo_attributes_with_session_id_and_other_attributes(self):
        """
        Verify only session.id is copied from span attributes when mixed with other attributes.
        """
        attributes = {
            "session.id": "session-456",
            "user.id": "user-789",
            "gen_ai.prompt": "What's the weather?",
            "gen_ai.completion": "I can't check the weather.",
            "other.attribute": "some-value",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        # Verify only session.id was copied to event attributes (plus event.name from Event class)
        self.assertIsNotNone(emitted_event.attributes)
        self.assertEqual(emitted_event.attributes.get("session.id"), "session-456")
        self.assertIn("event.name", emitted_event.attributes)
        # Verify other span attributes were not copied
        self.assertNotIn("user.id", emitted_event.attributes)
        self.assertNotIn("other.attribute", emitted_event.attributes)
        self.assertNotIn("gen_ai.prompt", emitted_event.attributes)
        self.assertNotIn("gen_ai.completion", emitted_event.attributes)

    def test_emit_llo_attributes_otel_genai_patterns(self):
        """
        Test the new GenAI patterns from Strands SDK that follow OTel GenAI Semantic Convention.
        """
        attributes = {
            "gen_ai.user.message": "What is machine learning?",
            "gen_ai.assistant.message": (
                "Machine learning is a subset of AI that enables computers to learn patterns from data."
            ),
            "gen_ai.system.message": "You are a helpful AI assistant specializing in technology topics.",
            "gen_ai.tool.message": "Searching knowledge base for machine learning definitions...",
            "gen_ai.choice": "Best answer: Machine learning uses algorithms to find patterns in data.",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "strands.genai.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        self.assertEqual(emitted_event.name, "strands.genai.scope")
        self.assertEqual(emitted_event.timestamp, span.end_time)

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        # Check input messages (system, user, and tool messages)
        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 3)

        input_contents = [msg["content"] for msg in input_messages]
        self.assertIn("What is machine learning?", input_contents)
        self.assertIn("You are a helpful AI assistant specializing in technology topics.", input_contents)
        self.assertIn("Searching knowledge base for machine learning definitions...", input_contents)

        # Verify roles for input messages
        user_msg = next((msg for msg in input_messages if msg["content"] == "What is machine learning?"), None)
        self.assertIsNotNone(user_msg)
        self.assertEqual(user_msg["role"], "user")

        system_msg = next((msg for msg in input_messages if "helpful AI assistant" in msg["content"]), None)
        self.assertIsNotNone(system_msg)
        self.assertEqual(system_msg["role"], "system")

        tool_msg = next((msg for msg in input_messages if "Searching knowledge base" in msg["content"]), None)
        self.assertIsNotNone(tool_msg)
        self.assertEqual(tool_msg["role"], "tool")

        # Check output messages (assistant message and choice)
        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 2)

        output_contents = [msg["content"] for msg in output_messages]
        self.assertIn(
            "Machine learning is a subset of AI that enables computers to learn patterns from data.", output_contents
        )
        self.assertIn("Best answer: Machine learning uses algorithms to find patterns in data.", output_contents)

        # Verify all output messages have assistant role
        for msg in output_messages:
            self.assertEqual(msg["role"], "assistant")

    def test_emit_llo_attributes_genai_user_message_only(self):
        """
        Test event generation with only gen_ai.user.message attribute.
        """
        attributes = {
            "gen_ai.user.message": "Hello, how are you today?",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertNotIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 1)
        self.assertEqual(input_messages[0]["content"], "Hello, how are you today?")
        self.assertEqual(input_messages[0]["role"], "user")

    def test_emit_llo_attributes_genai_assistant_message_only(self):
        """
        Test event generation with only gen_ai.assistant.message attribute.
        """
        attributes = {
            "gen_ai.assistant.message": "I'm doing well, thank you for asking!",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertNotIn("input", event_body)
        self.assertIn("output", event_body)

        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 1)
        self.assertEqual(output_messages[0]["content"], "I'm doing well, thank you for asking!")
        self.assertEqual(output_messages[0]["role"], "assistant")

    def test_emit_llo_attributes_genai_system_message_only(self):
        """
        Test event generation with only gen_ai.system.message attribute.
        """
        attributes = {
            "gen_ai.system.message": "You are a creative writing assistant. Help users write stories.",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertNotIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 1)
        self.assertEqual(
            input_messages[0]["content"], "You are a creative writing assistant. Help users write stories."
        )
        self.assertEqual(input_messages[0]["role"], "system")

    def test_emit_llo_attributes_genai_tool_message_only(self):
        """
        Test event generation with only gen_ai.tool.message attribute.
        """
        attributes = {
            "gen_ai.tool.message": "Executing search function with query: 'latest news'",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertNotIn("output", event_body)

        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 1)
        self.assertEqual(input_messages[0]["content"], "Executing search function with query: 'latest news'")
        self.assertEqual(input_messages[0]["role"], "tool")

    def test_emit_llo_attributes_genai_choice_only(self):
        """
        Test event generation with only gen_ai.choice attribute.
        """
        attributes = {
            "gen_ai.choice": "Selected option: Continue with the current approach.",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "test.scope"

        self.llo_handler._emit_llo_attributes(span, attributes)

        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertNotIn("input", event_body)
        self.assertIn("output", event_body)

        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 1)
        self.assertEqual(output_messages[0]["content"], "Selected option: Continue with the current approach.")
        self.assertEqual(output_messages[0]["role"], "assistant")

    def test_emit_llo_attributes_from_span_events(self):
        """
        Test that LLO attributes are collected from span events when event names match LLO patterns.
        """
        # Create span with normal attributes
        span_attributes = {"normal.attribute": "value"}

        # Create span events where event names are LLO patterns
        user_event = MagicMock()
        user_event.name = "gen_ai.user.message"
        user_event.attributes = {"content": "What is the weather today?", "other": "metadata"}
        user_event.timestamp = 1234567890

        assistant_event = MagicMock()
        assistant_event.name = "gen_ai.assistant.message"
        assistant_event.attributes = {"content": "It's sunny and 75°F", "confidence": 0.95}
        assistant_event.timestamp = 1234567891

        system_event = MagicMock()
        system_event.name = "gen_ai.system.message"
        system_event.attributes = {"content": "You are a weather assistant"}
        system_event.timestamp = 1234567892

        span = self._create_mock_span(span_attributes)
        span.events = [user_event, assistant_event, system_event]
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "event.patterns.scope"

        # Collect LLO attributes and emit event
        all_llo_attrs = self.llo_handler._collect_llo_attributes_from_span(span)
        self.llo_handler._emit_llo_attributes(span, all_llo_attrs)

        # Should emit event because LLO attributes were collected from span events
        self.event_logger_mock.emit.assert_called_once()
        emitted_event = self.event_logger_mock.emit.call_args[0][0]

        event_body = emitted_event.body
        self.assertIn("input", event_body)
        self.assertIn("output", event_body)

        # Check input messages (user and system)
        input_messages = event_body["input"]["messages"]
        self.assertEqual(len(input_messages), 2)

        # User message should contain all event attributes as content
        user_msg = next((msg for msg in input_messages if msg["role"] == "user"), None)
        self.assertIsNotNone(user_msg)
        self.assertEqual(user_msg["content"], {"content": "What is the weather today?", "other": "metadata"})

        # System message should contain all event attributes as content
        system_msg = next((msg for msg in input_messages if msg["role"] == "system"), None)
        self.assertIsNotNone(system_msg)
        self.assertEqual(system_msg["content"], {"content": "You are a weather assistant"})

        # Check output messages (assistant)
        output_messages = event_body["output"]["messages"]
        self.assertEqual(len(output_messages), 1)
        self.assertEqual(output_messages[0]["role"], "assistant")
        self.assertEqual(output_messages[0]["content"], {"content": "It's sunny and 75°F", "confidence": 0.95})

    def test_filter_span_events_removes_llo_pattern_events(self):
        """
        Test that span events are completely removed when their names match LLO patterns.
        """
        # Create span events
        normal_event = MagicMock()
        normal_event.name = "normal.event"
        normal_event.attributes = {"data": "keep this"}

        llo_event1 = MagicMock()
        llo_event1.name = "gen_ai.user.message"
        llo_event1.attributes = {"content": "remove this event"}

        llo_event2 = MagicMock()
        llo_event2.name = "gen_ai.assistant.message"
        llo_event2.attributes = {"content": "remove this too"}

        another_normal_event = MagicMock()
        another_normal_event.name = "another.normal.event"
        another_normal_event.attributes = {"info": "keep this too"}

        span = self._create_mock_span({})
        span.events = [normal_event, llo_event1, llo_event2, another_normal_event]

        self.llo_handler._filter_span_events(span)

        # Should only have normal events left
        self.assertEqual(len(span._events), 2)
        remaining_names = [event.name for event in span._events]
        self.assertIn("normal.event", remaining_names)
        self.assertIn("another.normal.event", remaining_names)
        self.assertNotIn("gen_ai.user.message", remaining_names)
        self.assertNotIn("gen_ai.assistant.message", remaining_names)
