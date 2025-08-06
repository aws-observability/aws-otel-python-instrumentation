# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import os
from typing import TypedDict

import boto3
import pytest
from langchain_aws import ChatBedrock
from langgraph.graph import StateGraph

from opentelemetry import trace
from opentelemetry.trace import INVALID_SPAN


@pytest.mark.vcr(filter_headers=["Authorization", "X-Amz-Date", "X-Amz-Security-Token"], record_mode="once")
def test_langgraph_invoke(instrument_langchain, span_exporter):
    span_exporter.clear()
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-west-2",
    )

    bedrock_client = session.client(service_name="bedrock-runtime", region_name="us-west-2")

    client = ChatBedrock(
        model_id="anthropic.claude-3-haiku-20240307-v1:0",
        model_kwargs={"max_tokens": 1000, "temperature": 0},
        client=bedrock_client,
    )

    class State(TypedDict):
        request: str
        result: str

    def calculate(state: State):
        request = state["request"]
        messages = [{"role": "system", "content": "You are a mathematician."}, {"role": "user", "content": request}]
        response = client.invoke(messages)
        return {"result": response.content}

    workflow = StateGraph(State)
    workflow.add_node("calculate", calculate)
    workflow.set_entry_point("calculate")

    langgraph = workflow.compile()

    response = langgraph.invoke(input={"request": "What's 5 + 5?"})["result"]

    spans = span_exporter.get_finished_spans()
    for span in spans:
        print(f"Span: {span.name}")
        print(f"  Attributes: {span.attributes}")
        print("---")

    assert {"chain LangGraph", "chain calculate", "chat anthropic.claude-3-haiku-20240307-v1:0"} == {
        span.name for span in spans
    }

    llm_span = next(span for span in spans if span.name == "chat anthropic.claude-3-haiku-20240307-v1:0")
    calculate_task_span = next(span for span in spans if span.name == "chain calculate")

    assert llm_span.parent.span_id == calculate_task_span.context.span_id

    assert llm_span.attributes["gen_ai.operation.name"] == "chat"
    assert llm_span.attributes["gen_ai.request.model"] == "anthropic.claude-3-haiku-20240307-v1:0"
    assert llm_span.attributes["gen_ai.response.model"] == "anthropic.claude-3-haiku-20240307-v1:0"

    assert "gen_ai.usage.input_tokens" in llm_span.attributes
    assert "gen_ai.usage.output_tokens" in llm_span.attributes

    assert llm_span.attributes["gen_ai.request.max_tokens"] == 1000
    assert llm_span.attributes["gen_ai.request.temperature"] == 0

    assert "gen_ai.prompt" in calculate_task_span.attributes
    assert "gen_ai.completion" in calculate_task_span.attributes
    assert "What's 5 + 5?" in calculate_task_span.attributes["gen_ai.prompt"]

    langgraph_span = next(span for span in spans if span.name == "chain LangGraph")
    assert "gen_ai.prompt" in langgraph_span.attributes
    assert "gen_ai.completion" in langgraph_span.attributes
    assert "What's 5 + 5?" in langgraph_span.attributes["gen_ai.prompt"]
    assert response in langgraph_span.attributes["gen_ai.completion"]


@pytest.mark.vcr
@pytest.mark.asyncio
# @pytest.mark.xfail(reason="Context propagation is not yet supported for async LangChain callbacks", strict=True)
async def test_langgraph_ainvoke(instrument_langchain, span_exporter):
    span_exporter.clear()
    bedrock_client = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")

    client = ChatBedrock(
        model_id="anthropic.claude-3-haiku-20240307-v1:0",
        client=bedrock_client,
        model_kwargs={"max_tokens": 1000, "temperature": 0},
    )

    class State(TypedDict):
        request: str
        result: str

    def calculate(state: State):
        request = state["request"]
        messages = [{"role": "system", "content": "You are a mathematician."}, {"role": "user", "content": request}]
        response = client.invoke(messages)
        return {"result": response.content}

    workflow = StateGraph(State)
    workflow.add_node("calculate", calculate)
    workflow.set_entry_point("calculate")

    langgraph = workflow.compile()

    await langgraph.ainvoke(input={"request": "What's 5 + 5?"})
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

    def build_graph():
        workflow = StateGraph(DummyGraphState)
        workflow.add_node("mynode", mynode_func)
        workflow.set_entry_point("mynode")
        langgraph = workflow.compile()
        return langgraph

    graph = build_graph()

    assert trace.get_current_span() == INVALID_SPAN

    graph.invoke({"result": "init"})
    assert trace.get_current_span() == INVALID_SPAN

    spans = span_exporter.get_finished_spans()
    assert [
        "chain mynode",
        "chain LangGraph",
    ] == [span.name for span in spans]

    graph.invoke({"result": "init"})
    assert trace.get_current_span() == INVALID_SPAN

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

    def build_graph():
        workflow = StateGraph(DummyGraphState)
        workflow.add_node("mynode", mynode_func)
        workflow.set_entry_point("mynode")
        langgraph = workflow.compile()
        return langgraph

    graph = build_graph()

    assert trace.get_current_span() == INVALID_SPAN

    await graph.ainvoke({"result": "init"})
    assert trace.get_current_span() == INVALID_SPAN

    spans = span_exporter.get_finished_spans()
    assert [
        "chain mynode",
        "chain LangGraph",
    ] == [span.name for span in spans]

    await graph.ainvoke({"result": "init"})
    assert trace.get_current_span() == INVALID_SPAN

    spans = span_exporter.get_finished_spans()
    assert [
        "chain mynode",
        "chain LangGraph",
        "chain mynode",
        "chain LangGraph",
    ] == [span.name for span in spans]
