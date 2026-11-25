# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Child process for test_genai_evaluable_e2e.py; not a test module.

Usage: ``python -I child_evaluable.py``. Like child_e2e.py (real loader, this package's hooks, the real upstream
``langchain`` entry point, a LangGraph ReAct agent over a fake chat model), plus what the evaluable steps act on:

- the real ``botocore`` instrumentation, and a fake model whose ``_generate`` calls Bedrock ``converse`` through a
  real botocore client (stubbed with ``Stubber``: dummy credentials, no network), so botocore's CLIENT ``chat``
  span is nested in LangChain's, and LangChain gets the same token usage;
- the agent runs inside a SERVER span whose context is extracted from a ``baggage: session.id=S1`` request;
- an SDK LoggerProvider is set, so its resource can be checked.

Prints the spans and the logger provider's resource as JSON on stdout.
"""
import json
import os
import sys
import warnings

from opentelemetry import _logs, context, propagate, trace
from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # `-I` leaves this directory off sys.path

from child_e2e import _restrict_entry_points  # noqa: E402  pylint: disable=wrong-import-position

MODEL = "fake-chat-model"
BEDROCK_MODEL_ID = "amazon.nova-lite-v1:0"
# Per model call: (input tokens, output tokens).
USAGE = [(11, 7), (23, 5)]
BOTOCORE_SCOPE = "opentelemetry.instrumentation.botocore"


def _stubbed_bedrock_client():
    """A real bedrock-runtime client whose ``converse`` calls return ``USAGE`` in order, from a Stubber."""
    # pylint: disable=import-outside-toplevel
    import botocore.session
    from botocore.stub import Stubber

    client = botocore.session.get_session().create_client(
        "bedrock-runtime", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    stubber = Stubber(client)
    for tokens_in, tokens_out in USAGE:
        response = {
            "output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": tokens_in, "outputTokens": tokens_out, "totalTokens": tokens_in + tokens_out},
            "metrics": {"latencyMs": 1},
        }
        stubber.add_response("converse", response)
    stubber.activate()
    return client


def _run_agent():
    # pylint: disable=import-outside-toplevel
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage
    from langchain_core.tools import tool

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # langgraph >= 1.0 deprecates create_react_agent
        from langgraph.prebuilt import create_react_agent

    bedrock = _stubbed_bedrock_client()

    class BedrockLikeChatModel(GenericFakeChatModel):
        model_name: str = MODEL

        def bind_tools(self, tools, **kwargs):
            return self

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            # The model call itself: botocore's CLIENT span, inside LangChain's (the current span).
            bedrock.converse(modelId=BEDROCK_MODEL_ID, messages=[{"role": "user", "content": [{"text": "hi"}]}])
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    def message(index, **fields):
        tokens_in, tokens_out = USAGE[index]
        usage = {"input_tokens": tokens_in, "output_tokens": tokens_out, "total_tokens": tokens_in + tokens_out}
        return AIMessage(usage_metadata=usage, **fields)

    @tool
    def get_weather(city: str) -> str:
        """Return the current weather for a city."""
        return f"light rain in {city}, 52F"

    model = BedrockLikeChatModel(
        messages=iter(
            [
                message(
                    0, content="", tool_calls=[{"name": "get_weather", "args": {"city": "Seattle"}, "id": "call_1"}]
                ),
                message(1, content="It is raining lightly in Seattle."),
            ]
        )
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        graph = create_react_agent(model, [get_weather], name="weather_agent")

    # As a WSGI/ASGI instrumentation does: extract with the global textmap, attach, start the SERVER span.
    token = context.attach(propagate.extract({"baggage": "session.id=S1"}))
    try:
        with trace.get_tracer("server").start_as_current_span("POST /invocations", kind=SpanKind.SERVER):
            graph.invoke({"messages": [("user", "What is the weather in Seattle?")]})
    finally:
        context.detach(token)


def _jsonable(value):
    return list(value) if isinstance(value, tuple) else value


def main():
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": "weather-agent"}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logger_provider = LoggerProvider(resource=provider.resource)
    _logs.set_logger_provider(logger_provider)

    _restrict_entry_points(("langchain", "botocore"))
    _load._load_instrumentors(DefaultDistro())

    trace.get_tracer("app").start_span("startup").end()  # outside any request
    _run_agent()

    spans = [
        {
            "name": span.name,
            "kind": span.kind.name,
            "scope": span.instrumentation_scope.name,
            "span_id": span.context.span_id,
            "parent_id": span.parent.span_id if span.parent else None,
            "attributes": {key: _jsonable(value) for key, value in span.attributes.items()},
            "resource": dict(span.resource.attributes),
        }
        for span in exporter.get_finished_spans()
    ]
    print(json.dumps({"spans": spans, "logger_resource": dict(logger_provider.resource.attributes)}))


if __name__ == "__main__":
    main()
