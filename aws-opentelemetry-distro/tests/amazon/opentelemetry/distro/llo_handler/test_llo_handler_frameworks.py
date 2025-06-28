# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for LLO Handler framework-specific functionality."""

from unittest.mock import MagicMock

from test_llo_handler_base import LLOHandlerTestBase


class TestLLOHandlerFrameworks(LLOHandlerTestBase):
    """Test framework-specific LLO attribute handling."""

    def test_collect_traceloop_messages(self):
        """
        Verify Traceloop entity input/output attributes are collected with correct roles
        (input->user, output->assistant).
        """
        attributes = {
            "traceloop.entity.input": "input data",
            "traceloop.entity.output": "output data",
            "traceloop.entity.name": "my_entity",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        traceloop_messages = [m for m in messages if m["source"] in ["input", "output"]]

        self.assertEqual(len(traceloop_messages), 2)

        input_message = traceloop_messages[0]
        self.assertEqual(input_message["content"], "input data")
        self.assertEqual(input_message["role"], "user")
        self.assertEqual(input_message["source"], "input")

        output_message = traceloop_messages[1]
        self.assertEqual(output_message["content"], "output data")
        self.assertEqual(output_message["role"], "assistant")
        self.assertEqual(output_message["source"], "output")

    def test_collect_traceloop_messages_all_attributes(self):
        """
        Verify collection of mixed Traceloop and CrewAI attributes, ensuring all are collected
        with appropriate roles and sources.
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

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 4)

        self.assertEqual(messages[0]["content"], "input data")
        self.assertEqual(messages[0]["role"], "user")
        self.assertEqual(messages[0]["source"], "input")

        self.assertEqual(messages[1]["content"], "output data")
        self.assertEqual(messages[1]["role"], "assistant")
        self.assertEqual(messages[1]["source"], "output")

        self.assertEqual(messages[2]["content"], "[TaskOutput(description='Task 1', output='Result 1')]")
        self.assertEqual(messages[2]["role"], "assistant")
        self.assertEqual(messages[2]["source"], "output")

        self.assertEqual(messages[3]["content"], "Final crew result")
        self.assertEqual(messages[3]["role"], "assistant")
        self.assertEqual(messages[3]["source"], "result")

    def test_collect_openlit_messages_direct_prompt(self):
        """
        Verify OpenLit's direct gen_ai.prompt attribute is collected with user role and prompt source.
        """
        attributes = {"gen_ai.prompt": "user direct prompt"}

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "user direct prompt")
        self.assertEqual(message["role"], "user")
        self.assertEqual(message["source"], "prompt")

    def test_collect_openlit_messages_direct_completion(self):
        """
        Verify OpenLit's direct gen_ai.completion attribute is collected with assistant role and completion source.
        """
        attributes = {"gen_ai.completion": "assistant direct completion"}

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "assistant direct completion")
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["source"], "completion")

    def test_collect_openlit_messages_all_attributes(self):
        """
        Verify all OpenLit framework attributes (prompt, completion, revised_prompt, agent.*)
        are collected with correct roles and sources.
        """
        attributes = {
            "gen_ai.prompt": "user prompt",
            "gen_ai.completion": "assistant response",
            "gen_ai.content.revised_prompt": "revised prompt",
            "gen_ai.agent.actual_output": "agent output",
            "gen_ai.agent.human_input": "human input to agent",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 5)

        self.assertEqual(messages[0]["content"], "user prompt")
        self.assertEqual(messages[0]["role"], "user")
        self.assertEqual(messages[0]["source"], "prompt")

        self.assertEqual(messages[1]["content"], "assistant response")
        self.assertEqual(messages[1]["role"], "assistant")
        self.assertEqual(messages[1]["source"], "completion")

        self.assertEqual(messages[2]["content"], "revised prompt")
        self.assertEqual(messages[2]["role"], "system")
        self.assertEqual(messages[2]["source"], "prompt")

        self.assertEqual(messages[3]["content"], "agent output")
        self.assertEqual(messages[3]["role"], "assistant")
        self.assertEqual(messages[3]["source"], "output")

        self.assertEqual(messages[4]["content"], "human input to agent")
        self.assertEqual(messages[4]["role"], "user")
        self.assertEqual(messages[4]["source"], "input")

    def test_collect_openlit_messages_revised_prompt(self):
        """
        Verify OpenLit's gen_ai.content.revised_prompt is collected with system role and prompt source.
        """
        attributes = {"gen_ai.content.revised_prompt": "revised system prompt"}

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "revised system prompt")
        self.assertEqual(message["role"], "system")
        self.assertEqual(message["source"], "prompt")

    def test_collect_openinference_messages_direct_attributes(self):
        """
        Verify OpenInference's direct input.value and output.value attributes are collected
        with appropriate roles (user/assistant) and sources.
        """
        attributes = {
            "input.value": "user prompt",
            "output.value": "assistant response",
            "llm.model_name": "gpt-4",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        input_message = messages[0]
        self.assertEqual(input_message["content"], "user prompt")
        self.assertEqual(input_message["role"], "user")
        self.assertEqual(input_message["source"], "input")

        output_message = messages[1]
        self.assertEqual(output_message["content"], "assistant response")
        self.assertEqual(output_message["role"], "assistant")
        self.assertEqual(output_message["source"], "output")

    def test_collect_openinference_messages_structured_input(self):
        """
        Verify OpenInference's indexed llm.input_messages.{n}.message.content attributes
        are collected with roles from corresponding role attributes.
        """
        attributes = {
            "llm.input_messages.0.message.content": "system prompt",
            "llm.input_messages.0.message.role": "system",
            "llm.input_messages.1.message.content": "user message",
            "llm.input_messages.1.message.role": "user",
            "llm.model_name": "claude-3",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        system_message = messages[0]
        self.assertEqual(system_message["content"], "system prompt")
        self.assertEqual(system_message["role"], "system")
        self.assertEqual(system_message["source"], "input")

        user_message = messages[1]
        self.assertEqual(user_message["content"], "user message")
        self.assertEqual(user_message["role"], "user")
        self.assertEqual(user_message["source"], "input")

    def test_collect_openinference_messages_structured_output(self):
        """
        Verify OpenInference's indexed llm.output_messages.{n}.message.content attributes
        are collected with source='output' and roles from corresponding attributes.
        """
        attributes = {
            "llm.output_messages.0.message.content": "assistant response",
            "llm.output_messages.0.message.role": "assistant",
            "llm.model_name": "llama-3",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)

        output_message = messages[0]
        self.assertEqual(output_message["content"], "assistant response")
        self.assertEqual(output_message["role"], "assistant")
        self.assertEqual(output_message["source"], "output")

    def test_collect_openinference_messages_mixed_attributes(self):
        """
        Verify mixed OpenInference attributes (direct and indexed) are all collected
        and maintain correct roles and counts.
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

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 4)

        contents = [msg["content"] for msg in messages]
        self.assertIn("direct input", contents)
        self.assertIn("direct output", contents)
        self.assertIn("message input", contents)
        self.assertIn("message output", contents)

        roles = [msg["role"] for msg in messages]
        self.assertEqual(roles.count("user"), 2)
        self.assertEqual(roles.count("assistant"), 2)

    def test_collect_openlit_messages_agent_actual_output(self):
        """
        Verify OpenLit's gen_ai.agent.actual_output is collected with assistant role and output source.
        """
        attributes = {"gen_ai.agent.actual_output": "Agent task output result"}

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)

        message = messages[0]
        self.assertEqual(message["content"], "Agent task output result")
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["source"], "output")

    def test_collect_openlit_messages_agent_human_input(self):
        """
        Verify OpenLit's gen_ai.agent.human_input is collected with user role and input source.
        """
        attributes = {"gen_ai.agent.human_input": "Human input to the agent"}

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "Human input to the agent")
        self.assertEqual(message["role"], "user")
        self.assertEqual(message["source"], "input")

    def test_collect_traceloop_messages_crew_outputs(self):
        """
        Verify CrewAI-specific attributes (tasks_output, result) are collected with assistant role
        and appropriate sources.
        """
        attributes = {
            "crewai.crew.tasks_output": "[TaskOutput(description='Task description', output='Task result')]",
            "crewai.crew.result": "Final crew execution result",
            "traceloop.entity.name": "crewai",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        tasks_message = messages[0]
        self.assertEqual(tasks_message["content"], "[TaskOutput(description='Task description', output='Task result')]")
        self.assertEqual(tasks_message["role"], "assistant")
        self.assertEqual(tasks_message["source"], "output")

        result_message = messages[1]
        self.assertEqual(result_message["content"], "Final crew execution result")
        self.assertEqual(result_message["role"], "assistant")
        self.assertEqual(result_message["source"], "result")

    def test_openinference_messages_with_default_roles(self):
        """
        Verify OpenInference indexed messages use default roles (user for input, assistant for output)
        when role attributes are missing.
        """
        attributes = {
            "llm.input_messages.0.message.content": "input without role",
            "llm.output_messages.0.message.content": "output without role",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        input_msg = next((m for m in messages if m["content"] == "input without role"), None)
        self.assertIsNotNone(input_msg)
        self.assertEqual(input_msg["role"], "user")
        self.assertEqual(input_msg["source"], "input")

        output_msg = next((m for m in messages if m["content"] == "output without role"), None)
        self.assertIsNotNone(output_msg)
        self.assertEqual(output_msg["role"], "assistant")
        self.assertEqual(output_msg["source"], "output")

    def test_collect_strands_sdk_messages(self):
        """
        Verify Strands SDK patterns (system_prompt, tool.result) are collected
        with correct roles and sources.
        """
        attributes = {
            "system_prompt": "You are a helpful assistant",
            "tool.result": "Tool execution completed successfully",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899
        span.instrumentation_scope = MagicMock()
        span.instrumentation_scope.name = "strands.sdk"

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        system_msg = next((m for m in messages if m["content"] == "You are a helpful assistant"), None)
        self.assertIsNotNone(system_msg)
        self.assertEqual(system_msg["role"], "system")
        self.assertEqual(system_msg["source"], "prompt")

        tool_msg = next((m for m in messages if m["content"] == "Tool execution completed successfully"), None)
        self.assertIsNotNone(tool_msg)
        self.assertEqual(tool_msg["role"], "assistant")
        self.assertEqual(tool_msg["source"], "output")

    def test_collect_llm_prompts_messages(self):
        """
        Verify llm.prompts attribute is collected as a user message with prompt source.
        """
        attributes = {
            "llm.prompts": (
                "[{'role': 'system', 'content': [{'text': 'You are a helpful AI assistant.', 'type': 'text'}]}, "
                "{'role': 'user', 'content': [{'text': 'What are the benefits of using FastAPI?', 'type': 'text'}]}]"
            ),
            "other.attribute": "not collected",
        }

        span = self._create_mock_span(attributes)
        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], attributes["llm.prompts"])
        self.assertEqual(message["role"], "user")
        self.assertEqual(message["source"], "prompt")

    def test_collect_llm_prompts_with_other_messages(self):
        """
        Verify llm.prompts works correctly alongside other LLO attributes.
        """
        attributes = {
            "llm.prompts": "[{'role': 'system', 'content': 'System prompt'}]",
            "gen_ai.prompt": "Direct prompt",
            "gen_ai.completion": "Assistant response",
        }

        span = self._create_mock_span(attributes)
        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 3)

        # Check llm.prompts message
        llm_prompts_msg = next((m for m in messages if m["content"] == attributes["llm.prompts"]), None)
        self.assertIsNotNone(llm_prompts_msg)
        self.assertEqual(llm_prompts_msg["role"], "user")
        self.assertEqual(llm_prompts_msg["source"], "prompt")

        # Check other messages are still collected
        direct_prompt_msg = next((m for m in messages if m["content"] == "Direct prompt"), None)
        self.assertIsNotNone(direct_prompt_msg)

        completion_msg = next((m for m in messages if m["content"] == "Assistant response"), None)
        self.assertIsNotNone(completion_msg)
