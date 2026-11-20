# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Child process for test_genai_e2e.py; not a test module.

Usage: ``python -I child_e2e.py``. Sets an SDK TracerProvider with an in-memory exporter globally, runs the real
auto-instrumentation loader with this package's hooks and the real upstream ``langchain`` entry point, then runs
a LangGraph ReAct agent (create_react_agent with a prompt, a checkpointer and a name) over a fake chat model,
and prints the spans as JSON on stdout. It configures no logging, so the plugin's lines reach stderr through its
fallback handler.
"""
import json
import warnings

from opentelemetry import trace
from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

PLUGIN_DIST = "cloudwatch-plugin-otel"
AGENT_NAME = "weather_agent"


def _restrict_entry_points(instrumentors=("langchain",)):
    real_entry_points = _load.entry_points

    def restricted(**params):
        group = params.get("group")
        eps = real_entry_points(**params)
        if group == "opentelemetry_instrumentor":
            return [ep for ep in eps if ep.name in instrumentors]
        if group in ("opentelemetry_pre_instrument", "opentelemetry_post_instrument"):
            return [ep for ep in eps if ep.dist is not None and ep.dist.name == PLUGIN_DIST]
        return []

    _load.entry_points = restricted


def _run_agent():
    # pylint: disable=import-outside-toplevel
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage
    from langchain_core.tools import tool
    from langgraph.checkpoint.memory import MemorySaver

    with warnings.catch_warnings():
        # langgraph >= 1.0 deprecates create_react_agent in favour of langchain.agents.create_agent, which needs
        # the `langchain` package; LangGraph apps still use it widely.
        warnings.simplefilter("ignore")
        from langgraph.prebuilt import create_react_agent

    class ToolCallingFakeChatModel(GenericFakeChatModel):
        # The upstream instrumentor skips chat models without a model name.
        model_name: str = "fake-chat-model"

        def bind_tools(self, tools, **kwargs):
            return self

    @tool
    def get_weather(city: str) -> str:
        """Return the current weather for a city."""
        return f"light rain in {city}, 52F"

    model = ToolCallingFakeChatModel(
        messages=iter(
            [
                AIMessage(
                    content="", tool_calls=[{"name": "get_weather", "args": {"city": "Seattle"}, "id": "call_1"}]
                ),
                AIMessage(content="It is raining lightly in Seattle."),
            ]
        )
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        graph = create_react_agent(
            model, [get_weather], prompt="You are a helpful assistant.", checkpointer=MemorySaver(), name=AGENT_NAME
        )
    graph.invoke(
        {"messages": [("user", "What is the weather in Seattle?")]},
        config={"configurable": {"thread_id": "session-1"}},
    )


def _jsonable(value):
    return list(value) if isinstance(value, tuple) else value


def main():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    _restrict_entry_points()
    _load._load_instrumentors(DefaultDistro())
    _run_agent()

    spans = [
        {
            "name": span.name,
            "scope": span.instrumentation_scope.name,
            "attributes": {key: _jsonable(value) for key, value in span.attributes.items()},
        }
        for span in exporter.get_finished_spans()
    ]
    print(json.dumps({"spans": spans}))


if __name__ == "__main__":
    main()
