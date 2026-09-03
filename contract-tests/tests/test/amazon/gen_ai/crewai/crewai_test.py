# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_TOOL_DESCRIPTION,
    GenAITestBase,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_TOOL_DEFINITIONS,
)

GEN_AI_WORKFLOW_NAME = "gen_ai.workflow.name"
OPERATION_INVOKE_WORKFLOW = "invoke_workflow"


class CrewAITest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-crewai-app"

    def test_crewai_single_agent(self):
        self._do_test_for_each_llm(
            "crewai/agent",
            expected_tool_count=2,
            expected_s3_call_count=1,
        )

    def test_crewai_multi_agent(self):
        self._do_test_for_each_llm(
            "crewai/multiagent",
            expected_agent_count=2,
            expected_tool_count=4,
            expected_s3_call_count=2,
        )

    @override
    def _assert_invoke_agent_spans(self, invoke_agent_spans: list, expected_count: int = 1):
        super()._assert_invoke_agent_spans(invoke_agent_spans, expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)

    @override
    def _assert_execute_tool_spans(self, execute_tool_spans: list, expected_count: int = 1):
        super()._assert_execute_tool_spans(execute_tool_spans, expected_count)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_TOOL_DESCRIPTION, attrs)

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertIn(GEN_AI_RESPONSE_MODEL, attrs)

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        super()._assert_semantic_conventions_span_attributes(resource_scope_spans, method, path, status_code, **kwargs)

        invoke_workflow_span = None
        for resource_scope_span in resource_scope_spans:
            if "invoke_workflow" in resource_scope_span.span.name:
                invoke_workflow_span = resource_scope_span.span
                break

        self.assertIsNotNone(invoke_workflow_span)
        crew_attrs = self._get_attributes_dict(invoke_workflow_span.attributes)
        self._assert_str_attribute(crew_attrs, GEN_AI_OPERATION_NAME, OPERATION_INVOKE_WORKFLOW)
        self.assertIn(GEN_AI_WORKFLOW_NAME, crew_attrs)
        self.assertIn(GEN_AI_AGENT_ID, crew_attrs)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, crew_attrs)

        invoke_agent_spans, _, _ = self._collect_gen_ai_spans(resource_scope_spans)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_AGENT_ID, attrs)
            self.assertIn(GEN_AI_AGENT_DESCRIPTION, attrs)
