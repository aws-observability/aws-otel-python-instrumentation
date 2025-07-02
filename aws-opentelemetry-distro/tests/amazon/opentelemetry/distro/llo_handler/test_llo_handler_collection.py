# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for LLO Handler message collection functionality."""

from test_llo_handler_base import LLOHandlerTestBase


class TestLLOHandlerCollection(LLOHandlerTestBase):
    """Test message collection from various frameworks."""

    def test_collect_gen_ai_prompt_messages_system_role(self):
        """
        Verify indexed prompt messages with system role are collected with correct content, role, and source.
        """
        attributes = {
            "gen_ai.prompt.0.content": "system instruction",
            "gen_ai.prompt.0.role": "system",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "system instruction")
        self.assertEqual(message["role"], "system")
        self.assertEqual(message["source"], "prompt")

    def test_collect_gen_ai_prompt_messages_user_role(self):
        """
        Verify indexed prompt messages with user role are collected with correct content, role, and source.
        """
        attributes = {
            "gen_ai.prompt.0.content": "user question",
            "gen_ai.prompt.0.role": "user",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "user question")
        self.assertEqual(message["role"], "user")
        self.assertEqual(message["source"], "prompt")

    def test_collect_gen_ai_prompt_messages_assistant_role(self):
        """
        Verify indexed prompt messages with assistant role are collected with correct content, role, and source.
        """
        attributes = {
            "gen_ai.prompt.1.content": "assistant response",
            "gen_ai.prompt.1.role": "assistant",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "assistant response")
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["source"], "prompt")

    def test_collect_gen_ai_prompt_messages_function_role(self):
        """
        Verify indexed prompt messages with non-standard 'function' role are collected correctly.
        """
        attributes = {
            "gen_ai.prompt.2.content": "function data",
            "gen_ai.prompt.2.role": "function",
        }

        span = self._create_mock_span(attributes)
        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "function data")
        self.assertEqual(message["role"], "function")
        self.assertEqual(message["source"], "prompt")

    def test_collect_gen_ai_prompt_messages_unknown_role(self):
        """
        Verify indexed prompt messages with unknown role are collected with the role preserved.
        """
        attributes = {
            "gen_ai.prompt.3.content": "unknown type content",
            "gen_ai.prompt.3.role": "unknown",
        }

        span = self._create_mock_span(attributes)
        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "unknown type content")
        self.assertEqual(message["role"], "unknown")
        self.assertEqual(message["source"], "prompt")

    def test_collect_gen_ai_completion_messages_assistant_role(self):
        """
        Verify indexed completion messages with assistant role are collected with source='completion'.
        """
        attributes = {
            "gen_ai.completion.0.content": "assistant completion",
            "gen_ai.completion.0.role": "assistant",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "assistant completion")
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["source"], "completion")

    def test_collect_gen_ai_completion_messages_other_role(self):
        """
        Verify indexed completion messages with custom roles are collected with source='completion'.
        """
        attributes = {
            "gen_ai.completion.1.content": "other completion",
            "gen_ai.completion.1.role": "other",
        }

        span = self._create_mock_span(attributes)
        span.end_time = 1234567899

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 1)
        message = messages[0]
        self.assertEqual(message["content"], "other completion")
        self.assertEqual(message["role"], "other")
        self.assertEqual(message["source"], "completion")

    def test_collect_all_llo_messages_none_attributes(self):
        """
        Verify _collect_all_llo_messages returns empty list when attributes are None.
        """
        span = self._create_mock_span(None, preserve_none=True)

        messages = self.llo_handler._collect_all_llo_messages(span, None)

        self.assertEqual(messages, [])
        self.assertEqual(len(messages), 0)

    def test_collect_indexed_messages_none_attributes(self):
        """
        Verify _collect_indexed_messages returns empty list when attributes are None.
        """
        messages = self.llo_handler._collect_indexed_messages(None)

        self.assertEqual(messages, [])
        self.assertEqual(len(messages), 0)

    def test_collect_indexed_messages_missing_role(self):
        """
        Verify indexed messages use default roles when role attributes are missing.
        """
        attributes = {
            "gen_ai.prompt.0.content": "prompt without role",
            "gen_ai.completion.0.content": "completion without role",
        }

        span = self._create_mock_span(attributes)

        messages = self.llo_handler._collect_all_llo_messages(span, attributes)

        self.assertEqual(len(messages), 2)

        prompt_msg = next((m for m in messages if m["content"] == "prompt without role"), None)
        self.assertIsNotNone(prompt_msg)
        self.assertEqual(prompt_msg["role"], "unknown")
        self.assertEqual(prompt_msg["source"], "prompt")

        completion_msg = next((m for m in messages if m["content"] == "completion without role"), None)
        self.assertIsNotNone(completion_msg)
        self.assertEqual(completion_msg["role"], "unknown")
        self.assertEqual(completion_msg["source"], "completion")

    def test_indexed_messages_with_out_of_order_indices(self):
        """
        Test that indexed messages are sorted correctly even with out-of-order indices
        """
        attributes = {
            "gen_ai.prompt.5.content": "fifth prompt",
            "gen_ai.prompt.5.role": "user",
            "gen_ai.prompt.1.content": "first prompt",
            "gen_ai.prompt.1.role": "system",
            "gen_ai.prompt.3.content": "third prompt",
            "gen_ai.prompt.3.role": "user",
            "llm.input_messages.10.message.content": "tenth message",
            "llm.input_messages.10.message.role": "assistant",
            "llm.input_messages.2.message.content": "second message",
            "llm.input_messages.2.message.role": "user",
        }

        messages = self.llo_handler._collect_indexed_messages(attributes)

        # Messages should be sorted by pattern key first, then by index
        self.assertEqual(len(messages), 5)

        # Check gen_ai.prompt messages are in order
        gen_ai_messages = [m for m in messages if "prompt" in m["source"]]
        self.assertEqual(gen_ai_messages[0]["content"], "first prompt")
        self.assertEqual(gen_ai_messages[1]["content"], "third prompt")
        self.assertEqual(gen_ai_messages[2]["content"], "fifth prompt")

        # Check llm.input_messages are in order
        llm_messages = [m for m in messages if m["content"] in ["second message", "tenth message"]]
        self.assertEqual(llm_messages[0]["content"], "second message")
        self.assertEqual(llm_messages[1]["content"], "tenth message")

    def test_collect_methods_message_format(self):
        """
        Verify all message collection methods return consistent message format with content, role, and source fields.
        """
        attributes = {
            "gen_ai.prompt.0.content": "prompt",
            "gen_ai.prompt.0.role": "user",
            "gen_ai.completion.0.content": "response",
            "gen_ai.completion.0.role": "assistant",
            "traceloop.entity.input": "input",
            "gen_ai.prompt": "direct prompt",
            "input.value": "inference input",
        }

        span = self._create_mock_span(attributes)

        prompt_messages = self.llo_handler._collect_all_llo_messages(span, attributes)
        for msg in prompt_messages:
            self.assertIn("content", msg)
            self.assertIn("role", msg)
            self.assertIn("source", msg)
            self.assertIsInstance(msg["content"], str)
            self.assertIsInstance(msg["role"], str)
            self.assertIsInstance(msg["source"], str)

        completion_messages = self.llo_handler._collect_all_llo_messages(span, attributes)
        for msg in completion_messages:
            self.assertIn("content", msg)
            self.assertIn("role", msg)
            self.assertIn("source", msg)

        traceloop_messages = self.llo_handler._collect_all_llo_messages(span, attributes)
        for msg in traceloop_messages:
            self.assertIn("content", msg)
            self.assertIn("role", msg)
            self.assertIn("source", msg)

        openlit_messages = self.llo_handler._collect_all_llo_messages(span, attributes)
        for msg in openlit_messages:
            self.assertIn("content", msg)
            self.assertIn("role", msg)
            self.assertIn("source", msg)

        openinference_messages = self.llo_handler._collect_all_llo_messages(span, attributes)
        for msg in openinference_messages:
            self.assertIn("content", msg)
            self.assertIn("role", msg)
            self.assertIn("source", msg)
