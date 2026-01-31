# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

# Gen AI semantic convention attributes
_GEN_AI_OPERATION_NAME: str = "gen_ai.operation.name"
_GEN_AI_PROVIDER_NAME: str = "gen_ai.provider.name"
_GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
_GEN_AI_REQUEST_TEMPERATURE: str = "gen_ai.request.temperature"
_GEN_AI_REQUEST_MAX_TOKENS: str = "gen_ai.request.max_tokens"
_GEN_AI_SYSTEM_INSTRUCTIONS: str = "gen_ai.system_instructions"
_GEN_AI_INPUT_MESSAGES: str = "gen_ai.input.messages"
_GEN_AI_OUTPUT_MESSAGES: str = "gen_ai.output.messages"
_GEN_AI_USAGE_INPUT_TOKENS: str = "gen_ai.usage.input_tokens"
_GEN_AI_USAGE_OUTPUT_TOKENS: str = "gen_ai.usage.output_tokens"
_GEN_AI_TOOL_DEFINITIONS: str = "gen_ai.tool.definitions"
_GEN_AI_TOOL_NAME: str = "gen_ai.tool.name"
_GEN_AI_TOOL_TYPE: str = "gen_ai.tool.type"
_GEN_AI_TOOL_DESCRIPTION: str = "gen_ai.tool.description"
_GEN_AI_TOOL_CALL_ARGUMENTS: str = "gen_ai.tool.call.arguments"
_GEN_AI_TOOL_CALL_RESULT: str = "gen_ai.tool.call.result"
_GEN_AI_TOOL_CALL_ID: str = "gen_ai.tool.call.id"
_GEN_AI_EMBEDDINGS_DIMENSION_COUNT: str = "gen_ai.embeddings.dimension.count"
_GEN_AI_AGENT_ID: str = "gen_ai.agent.id"
_GEN_AI_AGENT_NAME: str = "gen_ai.agent.name"
_GEN_AI_AGENT_DESCRIPTION: str = "gen_ai.agent.description"

# LlamaIndex operation names
_OPERATION_CHAT: str = "chat"
_OPERATION_TEXT_COMPLETION: str = "text_completion"
_OPERATION_EMBEDDINGS: str = "embeddings"
_OPERATION_INVOKE_AGENT: str = "invoke_agent"
_OPERATION_EXECUTE_TOOL: str = "execute_tool"
_OPERATION_QUERY: str = "query"
_OPERATION_RETRIEVE: str = "retrieve"
_OPERATION_SYNTHESIZE: str = "synthesize"
_OPERATION_RERANK: str = "rerank"


class LlamaIndexTest(ContractTestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-llamaindex-app"

    def test_llamaindex_agent(self):
        """Test ReAct agent with tools."""
        self.do_test_requests(
            "llamaindex/agent",
            "GET",
            200,
            0,
            0,
            test_type="agent",
        )

    def test_llamaindex_chat(self):
        """Test basic chat completion."""
        self.do_test_requests(
            "llamaindex/chat",
            "GET",
            200,
            0,
            0,
            test_type="chat",
        )

    def test_llamaindex_query(self):
        """Test query engine."""
        self.do_test_requests(
            "llamaindex/query",
            "GET",
            200,
            0,
            0,
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
        self.do_test_requests(
            "llamaindex/tool",
            "GET",
            200,
            0,
            0,
            test_type="tool",
        )

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        pass

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        test_type = kwargs.get("test_type", "")

        if test_type == "agent":
            self._assert_agent_spans(resource_scope_spans)
        elif test_type == "chat":
            self._assert_chat_spans(resource_scope_spans)
        elif test_type == "query":
            self._assert_query_spans(resource_scope_spans)
        elif test_type == "embedding":
            self._assert_embedding_spans(resource_scope_spans)
        elif test_type == "tool":
            self._assert_tool_spans(resource_scope_spans)

    def _assert_agent_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        invoke_agent_spans = []
        execute_tool_spans = []
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            
            if attrs.get(_GEN_AI_OPERATION_NAME):
                op_name = attrs[_GEN_AI_OPERATION_NAME].string_value
                
                if op_name == _OPERATION_INVOKE_AGENT:
                    invoke_agent_spans.append(span)
                elif op_name == _OPERATION_EXECUTE_TOOL:
                    execute_tool_spans.append(span)
                elif op_name == _OPERATION_CHAT:
                    chat_spans.append(span)
        
        if invoke_agent_spans:
            for span in invoke_agent_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_INVOKE_AGENT)
                self.assertIn(_GEN_AI_PROVIDER_NAME, attrs)

        if execute_tool_spans:
            tool_names = set()
            for span in execute_tool_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_EXECUTE_TOOL)
                
                self.assertIn(_GEN_AI_TOOL_NAME, attrs)
                tool_name = attrs[_GEN_AI_TOOL_NAME].string_value
                tool_names.add(tool_name)
                self.assertIn(tool_name, ["get_greeting", "multiply"], 
                             f"Unexpected tool name: {tool_name}")
                
                self.assertIn(_GEN_AI_TOOL_DESCRIPTION, attrs)

    def _assert_chat_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            
            if attrs.get(_GEN_AI_OPERATION_NAME):
                op_name = attrs[_GEN_AI_OPERATION_NAME].string_value
                if op_name == _OPERATION_CHAT:
                    chat_spans.append(span)

        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span")

        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_CHAT)
            
            self.assertIn(_GEN_AI_PROVIDER_NAME, attrs)
            provider = attrs[_GEN_AI_PROVIDER_NAME].string_value
            self.assertIn(provider, ["openai", "llama_index"])
            
            self._assert_str_attribute(attrs, _GEN_AI_REQUEST_MODEL, "gpt-4")
            
            self.assertIn(_GEN_AI_REQUEST_TEMPERATURE, attrs)
            temp = attrs[_GEN_AI_REQUEST_TEMPERATURE].double_value
            self.assertEqual(temp, 0.7, "Expected temperature to be 0.7")
            
            self.assertIn(_GEN_AI_REQUEST_MAX_TOKENS, attrs)
            max_tokens = attrs[_GEN_AI_REQUEST_MAX_TOKENS].int_value
            self.assertEqual(max_tokens, 100, "Expected max_tokens to be 100")
            
            self.assertIn(_GEN_AI_INPUT_MESSAGES, attrs)
            input_messages = attrs[_GEN_AI_INPUT_MESSAGES].string_value
            self.assertIsNotNone(input_messages)
            import json
            messages = json.loads(input_messages)
            self.assertIsInstance(messages, list)
            self.assertGreater(len(messages), 0, "Expected at least one message")
            
            self.assertIn(_GEN_AI_OUTPUT_MESSAGES, attrs)
            output_messages = attrs[_GEN_AI_OUTPUT_MESSAGES].string_value
            self.assertIsNotNone(output_messages)
            output_msgs = json.loads(output_messages)
            self.assertIsInstance(output_msgs, list)
            
            self.assertIn(_GEN_AI_USAGE_INPUT_TOKENS, attrs)
            input_tokens = attrs[_GEN_AI_USAGE_INPUT_TOKENS].int_value
            self.assertEqual(input_tokens, 25, "Expected 25 input tokens")
            
            self.assertIn(_GEN_AI_USAGE_OUTPUT_TOKENS, attrs)
            output_tokens = attrs[_GEN_AI_USAGE_OUTPUT_TOKENS].int_value
            self.assertEqual(output_tokens, 50, "Expected 50 output tokens")

    def _assert_query_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        query_spans = []
        retrieve_spans = []
        synthesize_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            
            if attrs.get(_GEN_AI_OPERATION_NAME):
                op_name = attrs[_GEN_AI_OPERATION_NAME].string_value
                
                if op_name == _OPERATION_QUERY:
                    query_spans.append(span)
                elif op_name == _OPERATION_RETRIEVE:
                    retrieve_spans.append(span)
                elif op_name == _OPERATION_SYNTHESIZE:
                    synthesize_spans.append(span)

        if query_spans:
            for span in query_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_QUERY)
                self._assert_str_attribute(attrs, _GEN_AI_PROVIDER_NAME, "llama_index")

        if retrieve_spans:
            for span in retrieve_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_RETRIEVE)

        if synthesize_spans:
            for span in synthesize_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_SYNTHESIZE)

    def _assert_embedding_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        embedding_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            
            if attrs.get(_GEN_AI_OPERATION_NAME):
                op_name = attrs[_GEN_AI_OPERATION_NAME].string_value
                if op_name == _OPERATION_EMBEDDINGS:
                    embedding_spans.append(span)

        self.assertGreater(len(embedding_spans), 0, "Expected at least one embedding span")

        for span in embedding_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_EMBEDDINGS)
            self._assert_str_attribute(attrs, _GEN_AI_PROVIDER_NAME, "llama_index")
            
            self.assertIn(_GEN_AI_EMBEDDINGS_DIMENSION_COUNT, attrs)
            dim_count = attrs[_GEN_AI_EMBEDDINGS_DIMENSION_COUNT].int_value
            self.assertEqual(dim_count, 384, "Expected embedding dimension count to be 384")

    def _assert_tool_spans(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        chat_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            
            if attrs.get(_GEN_AI_OPERATION_NAME):
                op_name = attrs[_GEN_AI_OPERATION_NAME].string_value
                
                if op_name == _OPERATION_CHAT:
                    chat_spans.append(span)

        self.assertGreater(len(chat_spans), 0, "Expected at least one chat span with tool definitions")
        
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, _OPERATION_CHAT)
            
            self.assertIn(_GEN_AI_TOOL_DEFINITIONS, attrs)
            tool_defs = attrs[_GEN_AI_TOOL_DEFINITIONS].string_value
            self.assertIsNotNone(tool_defs)
            import json
            tools = json.loads(tool_defs)
            self.assertIsInstance(tools, list)
            self.assertEqual(len(tools), 2, "Expected exactly two tool definitions (calculate_sum, multiply)")
            if tools and isinstance(tools[0], dict):
                tool_names = {tool["name"] for tool in tools if "name" in tool}
                self.assertEqual(tool_names, {"calculate_sum", "multiply"})
            else:
                self.assertEqual(len(tools), 2)

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass
