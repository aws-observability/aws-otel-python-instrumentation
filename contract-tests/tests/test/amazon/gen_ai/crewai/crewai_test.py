# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

_GEN_AI_OPERATION_NAME: str = "gen_ai.operation.name"
_GEN_AI_AGENT_ID: str = "gen_ai.agent.id"
_GEN_AI_AGENT_NAME: str = "gen_ai.agent.name"
_GEN_AI_AGENT_DESCRIPTION: str = "gen_ai.agent.description"
_GEN_AI_PROVIDER_NAME: str = "gen_ai.provider.name"
_GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
_GEN_AI_REQUEST_TEMPERATURE: str = "gen_ai.request.temperature"
_GEN_AI_SYSTEM_INSTRUCTIONS: str = "gen_ai.system_instructions"
_GEN_AI_TOOL_DEFINITIONS: str = "gen_ai.tool.definitions"
_GEN_AI_TOOL_NAME: str = "gen_ai.tool.name"
_GEN_AI_TOOL_TYPE: str = "gen_ai.tool.type"
_GEN_AI_TOOL_DESCRIPTION: str = "gen_ai.tool.description"
_GEN_AI_TOOL_CALL_ARGUMENTS: str = "gen_ai.tool.call.arguments"
_GEN_AI_TOOL_CALL_RESULT: str = "gen_ai.tool.call.result"


class CrewAITest(ContractTestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-crewai-app"

    def test_crewai_single_agent(self):
        self.do_test_requests(
            "crewai/agent",
            "GET",
            200,
            0,
            0,
            expected_agents={"Greeter": "openai"},
        )

    def test_crewai_multi_agent(self):
        self.do_test_requests(
            "crewai/multiagent",
            "GET",
            200,
            0,
            0,
            expected_agents={"Greeter": "openai", "Formatter": "openai"},
        )

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        pass

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        expected_agents = kwargs.get("expected_agents", {})
        crew_kickoff_span = None
        invoke_agent_spans = []
        execute_tool_spans = []

        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            if "crew_kickoff" in span.name:
                crew_kickoff_span = span
            elif "invoke_agent" in span.name:
                invoke_agent_spans.append(span)
            elif "execute_tool" in span.name:
                execute_tool_spans.append(span)

        self.assertIsNotNone(crew_kickoff_span)
        crew_attrs = self._get_attributes_dict(crew_kickoff_span.attributes)
        self._assert_str_attribute(crew_attrs, _GEN_AI_OPERATION_NAME, "invoke_agent")
        self.assertIn(_GEN_AI_AGENT_NAME, crew_attrs)
        self.assertIn(_GEN_AI_AGENT_ID, crew_attrs)
        self.assertIn(_GEN_AI_TOOL_DEFINITIONS, crew_attrs)
        self.assertNotIn(_GEN_AI_PROVIDER_NAME, crew_attrs)
        self.assertNotIn(_GEN_AI_REQUEST_MODEL, crew_attrs)

        self.assertEqual(len(invoke_agent_spans), len(expected_agents))
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, "invoke_agent")
            self.assertIn(_GEN_AI_AGENT_NAME, attrs)
            agent_name = attrs[_GEN_AI_AGENT_NAME].string_value
            self.assertIn(agent_name, expected_agents)
            self._assert_str_attribute(attrs, _GEN_AI_PROVIDER_NAME, expected_agents[agent_name])
            self.assertIn(_GEN_AI_AGENT_ID, attrs)
            self.assertIn(_GEN_AI_AGENT_DESCRIPTION, attrs)
            self.assertIn(_GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(_GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertIn(_GEN_AI_SYSTEM_INSTRUCTIONS, attrs)

        self.assertGreater(len(execute_tool_spans), 0)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, _GEN_AI_OPERATION_NAME, "execute_tool")
            self.assertIn(_GEN_AI_TOOL_NAME, attrs)
            self._assert_str_attribute(attrs, _GEN_AI_TOOL_TYPE, "function")
            self.assertIn(_GEN_AI_TOOL_DESCRIPTION, attrs)
            self.assertIn(_GEN_AI_TOOL_CALL_ARGUMENTS, attrs)
            self.assertIn(_GEN_AI_TOOL_CALL_RESULT, attrs)

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass
