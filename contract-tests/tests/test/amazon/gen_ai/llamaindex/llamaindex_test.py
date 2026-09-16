# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GenAITestBase
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_NAME,
    GEN_AI_EMBEDDINGS_DIMENSION_COUNT,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
)

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

GEN_AI_WORKFLOW_NAME = "gen_ai.workflow.name"
OPERATION_INVOKE_WORKFLOW = "invoke_workflow"
_OPERATION_QUERY: str = "query"
_OPERATION_SYNTHESIZE: str = "synthesize"
_OPERATION_RERANK: str = "rerank"


class LlamaIndexTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-llamaindex-app"

    def test_llamaindex_agent(self):
        """Test ReAct agent with tools."""
        self._do_test_for_each_llm(
            "llamaindex/agent",
            test_type="agent",
            expected_s3_call_count=1,
        )

    def test_llamaindex_workflow(self):
        """Test AgentWorkflow with multiple agents."""
        self._do_test_for_each_llm(
            "llamaindex/workflow",
            test_type="workflow",
            expected_s3_call_count=2,
        )

    def test_llamaindex_chat(self):
        """Test basic chat completion."""
        self._do_test_for_each_llm(
            "llamaindex/chat",
            test_type="chat",
        )

    def test_llamaindex_query(self):
        """Test query engine."""
        self._do_test_for_each_llm(
            "llamaindex/query",
            test_type="query",
        )

    def test_llamaindex_embedding(self):
        """Test embedding generation."""
        self.do_test_requests(
            "llamaindex/embedding",
            "GET",
            200,
            0,
            0,
            test_type="embedding",
        )

    def test_llamaindex_tool(self):
        """Test tool calling."""
        self._do_test_for_each_llm(
            "llamaindex/tool",
            test_type="tool",
        )

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        # Every recorded GenAI content attribute MUST contain data and follow its corresponding OTel JSON schema.
        self._assert_otel_gen_ai_attribute_formats(resource_scope_spans)
        expected_provider = kwargs.get("expected_provider")
        if expected_provider is not None:
            # Every span declaring a GenAI provider MUST identify the LLM selected for this test run.
            self._assert_gen_ai_provider(resource_scope_spans, expected_provider)
        test_type = kwargs.get("test_type", "")

        if test_type == "workflow":
            self._assert_workflow_spans(resource_scope_spans)
        elif test_type == "agent":
            self._assert_agent_spans(resource_scope_spans, expected_provider, kwargs.get("expected_model"))
        elif test_type == "chat":
            self._assert_chat_spans(resource_scope_spans, expected_provider, kwargs.get("expected_model"))
        elif test_type == "query":
            self._assert_query_spans(resource_scope_spans)
        elif test_type == "embedding":
            self._assert_embedding_spans(resource_scope_spans)
        elif test_type == "tool":
            self._assert_tool_spans(resource_scope_spans, expected_provider, kwargs.get("expected_model"))

    def _assert_agent_spans(
        self, resource_scope_spans: List[ResourceScopeSpan], expected_provider: str, expected_model: str
    ) -> None:
        invoke_agent_spans = []
        execute_tool_spans = []
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value

                if op_name == GenAiOperationNameValues.INVOKE_AGENT.value:
                    invoke_agent_spans.append(span)
                elif op_name == GenAiOperationNameValues.EXECUTE_TOOL.value:
                    execute_tool_spans.append(span)
                elif op_name == GenAiOperationNameValues.CHAT.value:
                    chat_spans.append(span)

        self.assertGreater(len(invoke_agent_spans), 0, "Expected at least one invoke_agent span")
        completed_invoke_agent_spans = []
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            self._assert_str_attribute(attrs, GEN_AI_AGENT_NAME, "TestAgent")
            if GEN_AI_INPUT_MESSAGES in attrs and GEN_AI_OUTPUT_MESSAGES in attrs:
                completed_invoke_agent_spans.append((span, attrs))
            if "run_agent_step" not in span.name:
                self._assert_str_attribute(attrs, GEN_AI_AGENT_DESCRIPTION, "A test agent that greets and multiplies.")

        # LlamaIndex emits invoke_agent spans for intermediate tool-calling steps. At least one completed invocation
        # MUST contain the initial user input and final agent output; intermediate steps are schema-validated above.
        self.assertTrue(completed_invoke_agent_spans, "Expected a completed invoke_agent span with input and output")
        for span, attrs in completed_invoke_agent_spans:
            self._assert_invoke_agent_content(attrs, span.name)

        if execute_tool_spans:
            tool_names = set()
            for span in execute_tool_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)

                self.assertIn(GEN_AI_TOOL_NAME, attrs)
                tool_name = attrs[GEN_AI_TOOL_NAME].string_value
                tool_names.add(tool_name)
                self.assertIn(
                    tool_name,
                    ["get_greeting", "multiply", "store_agent_output"],
                    f"Unexpected tool name: {tool_name}",
                )

                self.assertIn(GEN_AI_TOOL_DESCRIPTION, attrs)
                description = attrs[GEN_AI_TOOL_DESCRIPTION].string_value
                self.assertTrue(len(description) > 0, "Expected non-empty tool description")

        chat_spans = self._select_gen_ai_provider_spans(chat_spans)
        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span from agent LLM calls")
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            self.assertEqual(self._get_gen_ai_provider(attrs), expected_provider)
            self._assert_str_attribute(attrs, GEN_AI_REQUEST_MODEL, expected_model)

    def _assert_chat_spans(
        self, resource_scope_spans: List[ResourceScopeSpan], expected_provider: str, expected_model: str
    ) -> None:
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value
                if op_name == GenAiOperationNameValues.CHAT.value:
                    chat_spans.append(span)

        chat_spans = self._select_gen_ai_provider_spans(chat_spans)
        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span")

        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)

            self.assertEqual(self._get_gen_ai_provider(attrs), expected_provider)
            self._assert_str_attribute(attrs, GEN_AI_REQUEST_MODEL, expected_model)

            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            temp = attrs[GEN_AI_REQUEST_TEMPERATURE].double_value
            self.assertEqual(temp, 0.7, "Expected temperature to be 0.7")

            self.assertIn(GEN_AI_REQUEST_MAX_TOKENS, attrs)
            max_tokens = attrs[GEN_AI_REQUEST_MAX_TOKENS].int_value
            self.assertEqual(max_tokens, 100, "Expected max_tokens to be 100")

            self.assertIn(GEN_AI_SYSTEM_INSTRUCTIONS, attrs)

            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            messages = self._get_schema_value(attrs, GEN_AI_INPUT_MESSAGES, span.name)
            self.assertIsInstance(messages, list)
            self.assertGreater(len(messages), 0, "Expected at least one message")

            self.assertIn(GEN_AI_OUTPUT_MESSAGES, attrs)
            output_msgs = self._get_schema_value(attrs, GEN_AI_OUTPUT_MESSAGES, span.name)
            self.assertIsInstance(output_msgs, list)

            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            input_tokens = attrs[GEN_AI_USAGE_INPUT_TOKENS].int_value
            self.assertEqual(input_tokens, 10, "Expected 10 input tokens")

            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)
            output_tokens = attrs[GEN_AI_USAGE_OUTPUT_TOKENS].int_value
            self.assertEqual(output_tokens, 20, "Expected 20 output tokens")

    def _assert_query_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        query_spans = []
        retrieve_spans = []
        synthesize_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value

                if op_name == _OPERATION_QUERY:
                    query_spans.append(span)
                elif op_name == GenAiOperationNameValues.RETRIEVAL.value:
                    retrieve_spans.append(span)
                elif op_name == _OPERATION_SYNTHESIZE:
                    synthesize_spans.append(span)

        if query_spans:
            for span in query_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, _OPERATION_QUERY)
                self.assertNotIn(GEN_AI_PROVIDER_NAME, attrs)

        if retrieve_spans:
            for span in retrieve_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.RETRIEVAL.value)
                self.assertNotIn(GEN_AI_PROVIDER_NAME, attrs)

        if synthesize_spans:
            for span in synthesize_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, _OPERATION_SYNTHESIZE)
                self.assertNotIn(GEN_AI_PROVIDER_NAME, attrs)

    def _assert_embedding_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        embedding_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value
                if op_name == GenAiOperationNameValues.EMBEDDINGS.value:
                    embedding_spans.append(span)

        self.assertGreater(len(embedding_spans), 0, "Expected at least one embedding span")

        for span in embedding_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EMBEDDINGS.value)
            self.assertNotIn(GEN_AI_PROVIDER_NAME, attrs)

            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            model = attrs[GEN_AI_REQUEST_MODEL].string_value
            self.assertTrue(len(model) > 0, "Expected non-empty model name for embedding")

            self.assertIn(GEN_AI_EMBEDDINGS_DIMENSION_COUNT, attrs)
            dim_count = attrs[GEN_AI_EMBEDDINGS_DIMENSION_COUNT].int_value
            self.assertEqual(dim_count, 384, "Expected embedding dimension count to be 384")

    def _assert_tool_spans(
        self, resource_scope_spans: List[ResourceScopeSpan], expected_provider: str, expected_model: str
    ) -> None:
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value

                if op_name == GenAiOperationNameValues.CHAT.value:
                    chat_spans.append(span)

        chat_spans = self._select_gen_ai_provider_spans(chat_spans)
        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span with tool definitions")

        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)

            self.assertEqual(self._get_gen_ai_provider(attrs), expected_provider)
            self._assert_str_attribute(attrs, GEN_AI_REQUEST_MODEL, expected_model)

            self.assertIn(GEN_AI_TOOL_DEFINITIONS, attrs)
            tools = self._get_schema_value(attrs, GEN_AI_TOOL_DEFINITIONS, span.name)
            self.assertIsInstance(tools, list)
            self.assertEqual(len(tools), 2, "Expected exactly two tool definitions (calculate_sum, multiply)")
            if tools and isinstance(tools[0], dict):
                tool_names = {tool["name"] for tool in tools if "name" in tool}
                self.assertEqual(tool_names, {"calculate_sum", "multiply"})
            else:
                self.assertEqual(len(tools), 2)

            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)

            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_OUTPUT_MESSAGES, attrs)

    def _assert_workflow_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        invoke_workflow_spans = []
        invoke_agent_spans = []
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)

            if attrs.get(GEN_AI_OPERATION_NAME):
                op_name = attrs[GEN_AI_OPERATION_NAME].string_value

                if op_name == OPERATION_INVOKE_WORKFLOW:
                    invoke_workflow_spans.append(span)
                elif op_name == GenAiOperationNameValues.INVOKE_AGENT.value:
                    invoke_agent_spans.append(span)
                elif op_name == GenAiOperationNameValues.CHAT.value:
                    chat_spans.append(span)

        self.assertEqual(len(invoke_workflow_spans), 1, "Expected exactly one invoke_workflow span")
        wf_attrs = self._get_attributes_dict(invoke_workflow_spans[0].attributes)
        self._assert_str_attribute(wf_attrs, GEN_AI_OPERATION_NAME, OPERATION_INVOKE_WORKFLOW)
        self._assert_str_attribute(wf_attrs, GEN_AI_WORKFLOW_NAME, "multi_agent_workflow")

        self.assertGreater(len(invoke_agent_spans), 0, "Expected at least one invoke_agent span")
        completed_invoke_agent_spans = []
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            self.assertIn(GEN_AI_AGENT_NAME, attrs)
            if GEN_AI_INPUT_MESSAGES in attrs and GEN_AI_OUTPUT_MESSAGES in attrs:
                completed_invoke_agent_spans.append((span, attrs))

        # Intermediate workflow steps may end in a tool call. A completed invoke_agent span MUST still preserve the
        # workflow's first user input and last agent output.
        self.assertTrue(completed_invoke_agent_spans, "Expected a completed invoke_agent span with input and output")
        for span, attrs in completed_invoke_agent_spans:
            self._assert_invoke_agent_content(attrs, span.name)

        chat_spans = self._select_gen_ai_provider_spans(chat_spans)
        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span")

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass
