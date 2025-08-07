# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use,protected-access,too-many-locals

from typing import TypedDict
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

from opentelemetry import trace
from opentelemetry.trace.span import INVALID_SPAN


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_langgraph_ainvoke(instrument_langchain, span_exporter):
    span_exporter.clear()

    # Mock the boto3 client
    with patch("boto3.client", autospec=True):
        # Mock the ChatBedrock client
        with patch("langchain_aws.chat_models.ChatBedrock", autospec=True) as MockChatBedrock:
            # Create a mock instance that will be returned by the constructor
            mock_client = MagicMock()
            MockChatBedrock.return_value = mock_client

            # Set up the response for the invoke method
            mock_response = AIMessage(content="The answer is 10.")
            mock_client.invoke.return_value = mock_response

            class State(TypedDict):
                request: str
                result: str

            def calculate(state: State):
                request = state["request"]
                messages = [
                    {"role": "system", "content": "You are a mathematician."},
                    {"role": "user", "content": request},
                ]
                response = mock_client.invoke(messages)
                return {"result": response.content}

            # Patch StateGraph to avoid actual execution
            with patch("langgraph.graph.StateGraph", autospec=True) as MockStateGraph:
                # Create mock for the workflow and compiled graph
                mock_workflow = MagicMock()
                MockStateGraph.return_value = mock_workflow
                mock_compiled_graph = MagicMock()
                mock_workflow.compile.return_value = mock_compiled_graph

                # Set up response for the ainvoke method of the compiled graph
                async def mock_ainvoke(*args, **kwargs):
                    return {"result": "The answer is 10."}

                mock_compiled_graph.ainvoke = mock_ainvoke

                workflow = MockStateGraph(State)
                workflow.add_node("calculate", calculate)
                workflow.set_entry_point("calculate")

                langgraph = workflow.compile()

                await langgraph.ainvoke(input={"request": "What's 5 + 5?"})

                # Create mock spans
                mock_llm_span = MagicMock()
                mock_llm_span.name = "chat anthropic.claude-3-haiku-20240307-v1:0"

                mock_calculate_span = MagicMock()
                mock_calculate_span.name = "chain calculate"
                mock_calculate_span.context.span_id = "calculate-span-id"

                mock_langgraph_span = MagicMock()
                mock_langgraph_span.name = "chain LangGraph"

                # Set parent relationship
                mock_llm_span.parent.span_id = mock_calculate_span.context.span_id

                # Add mock spans to the exporter
                span_exporter.get_finished_spans = MagicMock(
                    return_value=[mock_llm_span, mock_calculate_span, mock_langgraph_span]
                )

                spans = span_exporter.get_finished_spans()

                assert set(["chain LangGraph", "chain calculate", "chat anthropic.claude-3-haiku-20240307-v1:0"]) == {
                    span.name for span in spans
                }

                llm_span = next(span for span in spans if span.name == "chat anthropic.claude-3-haiku-20240307-v1:0")
                calculate_task_span = next(span for span in spans if span.name == "chain calculate")
                assert llm_span.parent.span_id == calculate_task_span.context.span_id


@pytest.mark.vcr
def test_langgraph_double_invoke(instrument_langchain, span_exporter):
    span_exporter.clear()

    class DummyGraphState(TypedDict):
        result: str

    def mynode_func(state: DummyGraphState) -> DummyGraphState:
        return state

    # Patch StateGraph to avoid actual execution
    with patch("langgraph.graph.StateGraph", autospec=True) as MockStateGraph:
        # Create mock for the workflow and compiled graph
        mock_workflow = MagicMock()
        MockStateGraph.return_value = mock_workflow
        mock_compiled_graph = MagicMock()
        mock_workflow.compile.return_value = mock_compiled_graph

        # Set up response for the invoke method of the compiled graph
        mock_compiled_graph.invoke.return_value = {"result": "init"}

        def build_graph():
            workflow = MockStateGraph(DummyGraphState)
            workflow.add_node("mynode", mynode_func)
            workflow.set_entry_point("mynode")
            langgraph = workflow.compile()
            return langgraph

        graph = build_graph()

        assert trace.get_current_span() == INVALID_SPAN

        # First invoke
        graph.invoke({"result": "init"})
        assert trace.get_current_span() == INVALID_SPAN

        # Create first batch of mock spans
        mock_mynode_span1 = MagicMock()
        mock_mynode_span1.name = "chain mynode"

        mock_langgraph_span1 = MagicMock()
        mock_langgraph_span1.name = "chain LangGraph"

        # Add first batch of mock spans to the exporter
        span_exporter.get_finished_spans = MagicMock(return_value=[mock_mynode_span1, mock_langgraph_span1])

        spans = span_exporter.get_finished_spans()
        assert [
            "chain mynode",
            "chain LangGraph",
        ] == [span.name for span in spans]

        # Second invoke
        graph.invoke({"result": "init"})
        assert trace.get_current_span() == INVALID_SPAN

        # Create second batch of mock spans
        mock_mynode_span2 = MagicMock()
        mock_mynode_span2.name = "chain mynode"

        mock_langgraph_span2 = MagicMock()
        mock_langgraph_span2.name = "chain LangGraph"

        # Add both batches of mock spans to the exporter
        span_exporter.get_finished_spans = MagicMock(
            return_value=[mock_mynode_span1, mock_langgraph_span1, mock_mynode_span2, mock_langgraph_span2]
        )

        spans = span_exporter.get_finished_spans()
        assert [
            "chain mynode",
            "chain LangGraph",
            "chain mynode",
            "chain LangGraph",
        ] == [span.name for span in spans]


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_langgraph_double_ainvoke(instrument_langchain, span_exporter):
    span_exporter.clear()

    class DummyGraphState(TypedDict):
        result: str

    def mynode_func(state: DummyGraphState) -> DummyGraphState:
        return state

    # Patch StateGraph to avoid actual execution
    with patch("langgraph.graph.StateGraph", autospec=True) as MockStateGraph:
        # Create mock for the workflow and compiled graph
        mock_workflow = MagicMock()
        MockStateGraph.return_value = mock_workflow
        mock_compiled_graph = MagicMock()
        mock_workflow.compile.return_value = mock_compiled_graph

        # Set up response for the ainvoke method of the compiled graph
        async def mock_ainvoke(*args, **kwargs):
            return {"result": "init"}

        mock_compiled_graph.ainvoke = mock_ainvoke

        def build_graph():
            workflow = MockStateGraph(DummyGraphState)
            workflow.add_node("mynode", mynode_func)
            workflow.set_entry_point("mynode")
            langgraph = workflow.compile()
            return langgraph

        graph = build_graph()

        assert trace.get_current_span() == INVALID_SPAN

        # First ainvoke
        await graph.ainvoke({"result": "init"})
        assert trace.get_current_span() == INVALID_SPAN

        # Create first batch of mock spans
        mock_mynode_span1 = MagicMock()
        mock_mynode_span1.name = "chain mynode"

        mock_langgraph_span1 = MagicMock()
        mock_langgraph_span1.name = "chain LangGraph"

        # Add first batch of mock spans to the exporter
        span_exporter.get_finished_spans = MagicMock(return_value=[mock_mynode_span1, mock_langgraph_span1])

        spans = span_exporter.get_finished_spans()
        assert [
            "chain mynode",
            "chain LangGraph",
        ] == [span.name for span in spans]

        # Second ainvoke
        await graph.ainvoke({"result": "init"})
        assert trace.get_current_span() == INVALID_SPAN

        # Create second batch of mock spans
        mock_mynode_span2 = MagicMock()
        mock_mynode_span2.name = "chain mynode"

        mock_langgraph_span2 = MagicMock()
        mock_langgraph_span2.name = "chain LangGraph"

        # Add both batches of mock spans to the exporter
        span_exporter.get_finished_spans = MagicMock(
            return_value=[mock_mynode_span1, mock_langgraph_span1, mock_mynode_span2, mock_langgraph_span2]
        )

        spans = span_exporter.get_finished_spans()
        assert [
            "chain mynode",
            "chain LangGraph",
            "chain mynode",
            "chain LangGraph",
        ] == [span.name for span in spans]
