# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GEN_AI_REQUEST_TEMPERATURE, GEN_AI_TOOL_NAME, GenAITestBase
from opentelemetry.semconv.trace import SpanAttributes


class OpenAIAgentsTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-openai_agents-app"

    def test_openai_agents_single_agent(self):
        self._do_test_for_each_llm("openai_agents/agent", expected_tool_count=4)

    def test_openai_agents_multi_agent(self):
        self._do_test_for_each_llm("openai_agents/multiagent", expected_agent_count=2, expected_tool_count=4)

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertEqual(attrs[GEN_AI_REQUEST_TEMPERATURE].double_value, 0.7)

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: list, path: str, **kwargs) -> None:
        if "/agent/" not in path:
            return

        spans = [resource_scope_span.span for resource_scope_span in resource_scope_spans]
        store_tool_spans = []
        for span in spans:
            attrs = self._get_attributes_dict(span.attributes)
            if attrs.get(GEN_AI_TOOL_NAME) and attrs[GEN_AI_TOOL_NAME].string_value == "store_trip_plan":
                store_tool_spans.append(span)

        s3_spans = [span for span in spans if span.name == "S3.PutObject"]
        self.assertEqual(len(store_tool_spans), 1)
        self.assertEqual(len(s3_spans), 1)
        self.assertEqual(s3_spans[0].parent_span_id, store_tool_spans[0].span_id)

        attrs = self._get_attributes_dict(s3_spans[0].attributes)
        self._assert_str_attribute(attrs, SpanAttributes.AWS_S3_BUCKET, "example-bucket")
