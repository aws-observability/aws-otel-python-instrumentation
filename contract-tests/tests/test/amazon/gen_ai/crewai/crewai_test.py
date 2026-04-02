# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GEN_AI_OPERATION_NAME, GEN_AI_WORKFLOW_NAME, GenAITestBase

GEN_AI_AGENT_ID: str = "gen_ai.agent.id"
GEN_AI_AGENT_DESCRIPTION: str = "gen_ai.agent.description"
GEN_AI_SYSTEM_INSTRUCTIONS: str = "gen_ai.system_instructions"
GEN_AI_TOOL_DEFINITIONS: str = "gen_ai.tool.definitions"


class CrewAITest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-crewai-app"

    def test_crewai_single_agent(self):
        self.do_test_requests("crewai/agent", "GET", 200, 0, 0)

    def test_crewai_multi_agent(self):
        self.do_test_requests("crewai/multiagent", "GET", 200, 0, 0, expected_agent_count=2)

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
        self._assert_str_attribute(crew_attrs, GEN_AI_OPERATION_NAME, "invoke_workflow")
        self.assertIn(GEN_AI_WORKFLOW_NAME, crew_attrs)
        self.assertIn(GEN_AI_AGENT_ID, crew_attrs)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, crew_attrs)

        _, invoke_agent_spans, _, _ = self._collect_gen_ai_spans(resource_scope_spans)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_AGENT_ID, attrs)
            self.assertIn(GEN_AI_AGENT_DESCRIPTION, attrs)
