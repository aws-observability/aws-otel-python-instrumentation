# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use,protected-access,too-many-locals

from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.agents import AgentActionMessageLog
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import Tool


@pytest.fixture
def mock_search_tool():
    mock_tool = Tool(
        name="duckduckgo_results_json",
        func=MagicMock(return_value=[{"result": "Amazon founded in 1994"}]),
        description="Search for information",
    )
    return mock_tool


@pytest.fixture
def mock_model():
    model = MagicMock()
    model.bind_tools = MagicMock(return_value=model)

    # Return proper AgentActionMessageLog instead of raw AIMessage
    model.invoke = MagicMock(
        return_value=AIMessage(
            content="",
            additional_kwargs={
                "tool_calls": [
                    {
                        "id": "call_123",
                        "type": "function",
                        "function": {
                            "name": "duckduckgo_results_json",
                            "arguments": '{"query": "Amazon founding date"}',
                        },
                    }
                ]
            },
        )
    )
    return model


@pytest.fixture
def mock_prompt():
    return ChatPromptTemplate.from_messages(
        [
            ("system", "You are a helpful assistant"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ]
    )


def test_agents(
    instrument_langchain, span_exporter, model_fixture, search_tool_fixture, prompt_fixture
):  # Changed parameter names
    # pylint: disable=redefined-outer-name
    tools = [search_tool_fixture]  # Use renamed parameter

    agent = create_tool_calling_agent(model_fixture, tools, prompt_fixture)  # Use renamed parameters
    agent_executor = AgentExecutor(agent=agent, tools=tools)

    # Mock the agent's intermediate steps
    with patch("langchain.agents.AgentExecutor._iter_next_step") as mock_iter:
        mock_iter.return_value = [
            (
                AgentActionMessageLog(
                    tool="duckduckgo_results_json",
                    tool_input={"query": "Amazon founding date"},
                    log="",
                    message_log=[AIMessage(content="")],
                ),
                "Tool result",
            )
        ]

        span_exporter.clear()
        agent_executor.invoke({"input": "When was Amazon founded?"})

    spans = span_exporter.get_finished_spans()
    assert {span.name for span in spans} == {
        "chain AgentExecutor",
    }


def test_agents_with_events_with_content(
    instrument_with_content, span_exporter, model_param, search_tool_param, prompt_param  # Changed parameter names
):
    # pylint: disable=redefined-outer-name
    tools = [search_tool_param]  # Use renamed parameter

    agent = create_tool_calling_agent(model_param, tools, prompt_param)  # Use renamed parameters
    agent_executor = AgentExecutor(agent=agent, tools=tools)

    with patch("langchain.agents.AgentExecutor._iter_next_step") as mock_iter:
        mock_iter.return_value = [
            (
                AgentActionMessageLog(
                    tool="duckduckgo_results_json",
                    tool_input={"query": "AWS definition"},
                    log="",
                    message_log=[AIMessage(content="")],
                ),
                "Tool result",
            )
        ]

        span_exporter.clear()
        agent_executor.invoke({"input": "What is AWS?"})

    spans = span_exporter.get_finished_spans()
    assert {span.name for span in spans} == {
        "chain AgentExecutor",
    }


def test_agents_with_events_with_no_content(
    instrument_langchain, span_exporter, model_input, search_tool_input, prompt_input  # Changed parameter names
):
    # pylint: disable=redefined-outer-name
    tools = [search_tool_input]  # Use renamed parameter

    agent = create_tool_calling_agent(model_input, tools, prompt_input)  # Use renamed parameters
    agent_executor = AgentExecutor(agent=agent, tools=tools)

    with patch("langchain.agents.AgentExecutor._iter_next_step") as mock_iter:
        mock_iter.return_value = [
            (
                AgentActionMessageLog(
                    tool="duckduckgo_results_json",
                    tool_input={"query": "AWS information"},
                    log="",
                    message_log=[AIMessage(content="")],
                ),
                "Tool result",
            )
        ]

        span_exporter.clear()
        agent_executor.invoke({"input": "What is AWS?"})

    spans = span_exporter.get_finished_spans()
    assert {span.name for span in spans} == {
        "chain AgentExecutor",
    }
