# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""End to end in a ``python -I`` child: loader + hooks + upstream instrumentor + a LangGraph ReAct agent."""
import importlib.metadata
import json
import subprocess
import sys
from pathlib import Path

import pytest
from genai_helpers import EVALUABLE, OLD, child_env, plugin_entry_points, summary_parts

pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
pytest.importorskip("langchain_core")
pytest.importorskip("langgraph.prebuilt")

from child_e2e import AGENT_NAME  # noqa: E402  pylint: disable=wrong-import-position

CHILD = Path(__file__).with_name("child_e2e.py")
SCOPE = "opentelemetry.instrumentation.genai.langchain"


@pytest.fixture(scope="module")
def child_run():
    for group in ("opentelemetry_pre_instrument", "opentelemetry_post_instrument"):
        plugin_entry_points(group)
    with pytest.raises(importlib.metadata.PackageNotFoundError):
        importlib.metadata.version("langchain")  # precondition: only langchain-core is installed

    # Only what this test sets: no inherited OTEL_* (exporters, disabled lists, ...) or plugin switch.
    env = child_env(**{OLD: "true"})  # `true` is what the AgentCore docs tell users to set
    proc = subprocess.run(
        [sys.executable, "-I", str(CHILD)], capture_output=True, text=True, env=env, timeout=300, check=False
    )
    assert proc.returncode == 0, proc.stderr
    spans = json.loads(proc.stdout.strip().splitlines()[-1])["spans"]
    return spans, proc.stderr


def _by_operation(spans):
    grouped = {}
    for span in spans:
        grouped.setdefault(span["attributes"].get("gen_ai.operation.name"), []).append(span)
    return grouped


def test_agent_chat_and_tool_spans_are_produced(child_run):
    spans, _ = child_run
    grouped = _by_operation(spans)
    assert "invoke_workflow" not in grouped, "upstream labels create_react_agent as a workflow: report, do not patch"
    for operation in ("invoke_agent", "chat", "execute_tool"):
        assert grouped.get(operation), f"no {operation} span in {[s['name'] for s in spans]}"


def test_messages_are_captured_on_chat_and_agent_spans(child_run):
    spans, _ = child_run
    grouped = _by_operation(spans)
    agent = [s for s in grouped["invoke_agent"] if s["attributes"].get("gen_ai.agent.name") == AGENT_NAME]
    assert len(agent) == 1, [s["name"] for s in grouped["invoke_agent"]]
    assert len(grouped["invoke_agent"]) == 1, f"spurious agent spans: {[s['name'] for s in grouped['invoke_agent']]}"
    for span in agent + grouped["chat"]:
        assert span["attributes"].get("gen_ai.input.messages"), span["name"]
        assert span["attributes"].get("gen_ai.output.messages"), span["name"]
    assert "What is the weather in Seattle?" in agent[0]["attributes"]["gen_ai.input.messages"]
    assert "It is raining lightly in Seattle." in agent[0]["attributes"]["gen_ai.output.messages"]


def test_tool_result_is_captured(child_run):
    spans, _ = child_run
    (tool,) = _by_operation(spans)["execute_tool"]
    assert tool["attributes"]["gen_ai.tool.name"] == "get_weather"
    assert tool["attributes"]["gen_ai.tool.call.result"] == "light rain in Seattle, 52F"


def test_spans_come_from_the_upstream_instrumentation_scope(child_run):
    spans, _ = child_run
    genai = [s for s in spans if "gen_ai.operation.name" in s["attributes"]]
    assert genai and {s["scope"] for s in genai} == {SCOPE}


def test_summary_line_reaches_stderr(child_run):
    _, stderr = child_run
    plugin_lines = [line for line in stderr.splitlines() if "CW-PLUGIN genai: " in line]
    assert not any("WARNING" in line for line in plugin_lines), stderr  # `true` just works: no warning
    summary = [line for line in plugin_lines if " INFO " in line]
    assert len(summary) == 1, f"expected exactly one INFO line (the summary): {plugin_lines}"
    parts = summary_parts(summary[0][summary[0].index("CW-PLUGIN genai: ") :])
    assert parts == {
        "content capture": "SPAN_ONLY (translated from true)",
        "langchain": "activated (langchain-core only)",
        **EVALUABLE,
    }, summary
