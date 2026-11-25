# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""End to end in a ``python -I`` child: the real loader and hooks, the upstream LangChain instrumentor and a
LangGraph ReAct agent whose model calls each make a nested CLIENT ``chat`` span, under baggage ``session.id`` on
AgentCore Runtime. Checks what CloudWatch needs to evaluate the session."""
import pytest
from genai_helpers import child_env, plugin_entry_points, run_child, summary_parts

pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
pytest.importorskip("langchain_core")
pytest.importorskip("langgraph.prebuilt")

from child_evaluable import BOTOCORE_SCOPE, USAGE  # noqa: E402  pylint: disable=wrong-import-position

RUNTIME_URL = (
    "https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/"
    "arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A123456789012%3Aruntime%2Fmy_agent-AbCdEf1234/invocations"
)
IDENTITY = {
    "aws.service.type": "gen_ai_agent",
    "cloud.resource_id": "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my_agent-AbCdEf1234"
    "/runtime-endpoint/DEFAULT:DEFAULT",
    "cloud.platform": "aws_bedrock_agentcore",
}
LANGCHAIN_SCOPE = "opentelemetry.instrumentation.genai.langchain"


@pytest.fixture(scope="module")
def child_run():
    for group in ("opentelemetry_pre_instrument", "opentelemetry_post_instrument"):
        plugin_entry_points(group)
    report, stderr = run_child("child_evaluable.py", env=child_env(AGENTCORE_RUNTIME_URL=RUNTIME_URL))
    return report, stderr


def _request_spans(spans):
    return [span for span in spans if span["name"] != "startup"]


def _chat(spans):
    return [span for span in spans if span["attributes"].get("gen_ai.operation.name") == "chat"]


def test_every_span_of_the_request_has_the_session_id(child_run):
    spans = child_run[0]["spans"]
    request = _request_spans(spans)
    operations = {span["attributes"].get("gen_ai.operation.name") for span in request}
    assert {"invoke_agent", "chat", "execute_tool"} <= operations, operations
    assert [span["attributes"].get("session.id") for span in request] == ["S1"] * len(request)
    (startup,) = [span for span in spans if span["name"] == "startup"]
    assert "session.id" not in startup["attributes"]


def test_one_client_chat_span_per_model_call(child_run):
    chat = _chat(child_run[0]["spans"])
    client = [span for span in chat if span["kind"] == "CLIENT"]
    langchain = [span for span in chat if span["scope"] == LANGCHAIN_SCOPE]
    assert len(client) == len(USAGE), client
    assert all(span["scope"].startswith(BOTOCORE_SCOPE) for span in client), client  # the real botocore spans
    assert len(langchain) == len(USAGE) and {span["kind"] for span in langchain} == {"INTERNAL"}
    # Each CLIENT span is the child of a LangChain chat span: the nesting the demotion is for.
    assert {span["parent_id"] for span in client} == {span["span_id"] for span in langchain}


def test_client_token_sums_equal_the_true_usage(child_run):
    chat = _chat(child_run[0]["spans"])
    for index, key in enumerate(("gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens")):
        true_usage = sum(usage[index] for usage in USAGE)
        assert sum(span["attributes"].get(key, 0) for span in chat if span["kind"] == "CLIENT") == true_usage
        # LangChain's spans carry the same usage: counted too, the tokens would double.
        assert sum(span["attributes"].get(key, 0) for span in chat if span["scope"] == LANGCHAIN_SCOPE) == true_usage


def test_the_resource_has_the_agent_identity(child_run):
    report, _ = child_run
    for span in report["spans"]:
        assert {key: span["resource"].get(key) for key in IDENTITY} == IDENTITY, span["name"]
        assert span["resource"]["service.name"] == "weather-agent"
    assert {key: report["logger_resource"].get(key) for key in IDENTITY} == IDENTITY


def test_the_summary_line_reports_the_steps(child_run):
    _, stderr = child_run
    summary = [line for line in stderr.splitlines() if "CW-PLUGIN genai: " in line and " INFO " in line]
    assert len(summary) == 1, stderr
    parts = summary_parts(summary[0][summary[0].index("CW-PLUGIN genai: ") :])
    assert parts["langchain"] == "activated (langchain-core only)", summary
    assert parts["session.id"] == parts["nested chat demotion"] == "on", summary
    assert parts["agent identity"] == "service.type+runtime", summary
