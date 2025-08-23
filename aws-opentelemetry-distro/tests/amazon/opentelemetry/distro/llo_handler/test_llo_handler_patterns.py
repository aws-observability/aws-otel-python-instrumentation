# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for LLO Handler pattern matching functionality."""

from test_llo_handler_base import LLOHandlerTestBase

from amazon.opentelemetry.distro.llo_handler import LLO_PATTERNS, LLOHandler, PatternType


class TestLLOHandlerPatterns(LLOHandlerTestBase):
    """Test pattern matching and recognition functionality."""

    def test_init(self):
        """
        Verify LLOHandler initializes correctly with logger provider and creates event logger provider.
        """
        self.assertEqual(self.llo_handler._logger_provider, self.logger_provider_mock)
        self.assertEqual(self.llo_handler._event_logger_provider, self.event_logger_provider_mock)

    def test_is_llo_attribute_match(self):
        """
        Verify _is_llo_attribute correctly identifies indexed Gen AI prompt patterns (gen_ai.prompt.{n}.content).
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt.0.content"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt.123.content"))

    def test_is_llo_attribute_no_match(self):
        """
        Verify _is_llo_attribute correctly rejects malformed patterns and non-LLO attributes.
        """
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.prompt.content"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.prompt.abc.content"))
        self.assertFalse(self.llo_handler._is_llo_attribute("some.other.attribute"))

    def test_is_llo_attribute_traceloop_match(self):
        """
        Verify _is_llo_attribute recognizes Traceloop framework patterns (traceloop.entity.input/output).
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("traceloop.entity.input"))
        self.assertTrue(self.llo_handler._is_llo_attribute("traceloop.entity.output"))

    def test_is_llo_attribute_openlit_match(self):
        """
        Verify _is_llo_attribute recognizes OpenLit framework patterns (gen_ai.prompt, gen_ai.completion, etc.).
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.prompt"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.completion"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.content.revised_prompt"))

    def test_is_llo_attribute_openinference_match(self):
        """
        Verify _is_llo_attribute recognizes OpenInference patterns including both direct (input/output.value)
        and indexed (llm.input_messages.{n}.message.content) patterns.
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("input.value"))
        self.assertTrue(self.llo_handler._is_llo_attribute("output.value"))
        self.assertTrue(self.llo_handler._is_llo_attribute("llm.input_messages.0.message.content"))
        self.assertTrue(self.llo_handler._is_llo_attribute("llm.output_messages.123.message.content"))

    def test_is_llo_attribute_crewai_match(self):
        """
        Verify _is_llo_attribute recognizes CrewAI framework patterns (gen_ai.agent.*, crewai.crew.*).
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.agent.actual_output"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.agent.human_input"))
        self.assertTrue(self.llo_handler._is_llo_attribute("crewai.crew.tasks_output"))
        self.assertTrue(self.llo_handler._is_llo_attribute("crewai.crew.result"))

    def test_is_llo_attribute_strands_sdk_match(self):
        """
        Verify _is_llo_attribute recognizes Strands SDK patterns (system_prompt, tool.result).
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("system_prompt"))
        self.assertTrue(self.llo_handler._is_llo_attribute("tool.result"))

    def test_is_llo_attribute_llm_prompts_match(self):
        """
        Verify _is_llo_attribute recognizes llm.prompts pattern.
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("llm.prompts"))

    def test_build_pattern_matchers_with_missing_regex(self):
        """
        Test _build_pattern_matchers handles patterns with missing regex gracefully
        """
        # Temporarily modify LLO_PATTERNS to have a pattern without regex
        original_patterns = LLO_PATTERNS.copy()

        # Add a malformed indexed pattern without regex
        LLO_PATTERNS["test.bad.pattern"] = {
            "type": PatternType.INDEXED,
            # Missing "regex" key
            "role_key": "test.bad.pattern.role",
            "default_role": "unknown",
            "source": "test",
        }

        try:
            # Create a new handler to trigger pattern building
            handler = LLOHandler(self.logger_provider_mock)

            # Should handle gracefully - the bad pattern should be skipped
            self.assertNotIn("test.bad.pattern", handler._pattern_configs)

            # Other patterns should still work
            self.assertTrue(handler._is_llo_attribute("gen_ai.prompt"))
            self.assertFalse(handler._is_llo_attribute("test.bad.pattern"))

        finally:
            # Restore original patterns
            LLO_PATTERNS.clear()
            LLO_PATTERNS.update(original_patterns)

    def test_is_llo_attribute_otel_genai_patterns_match(self):
        """
        Verify _is_llo_attribute recognizes new GenAI patterns from Strands SDK that follow OTel GenAI Semantic
        Convention.
        """
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.user.message"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.assistant.message"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.system.message"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.tool.message"))
        self.assertTrue(self.llo_handler._is_llo_attribute("gen_ai.choice"))

    def test_is_llo_attribute_otel_genai_patterns_no_match(self):
        """
        Verify _is_llo_attribute correctly rejects similar but invalid GenAI patterns.
        """
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.user"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.assistant"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.system"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.tool"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.user.message.content"))
        self.assertFalse(self.llo_handler._is_llo_attribute("gen_ai.invalid.message"))
