# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Child process for test_genai_loader.py; not a test module.

Usage: ``python -I child_langchain_loader.py <workdir>``. ``workdir`` holds stub distributions written by the
test (a `langchain` dist-info). Runs the real loader with this package's hooks and the ``langchain`` instrumentor
entry point, makes one chat-model call, and prints a JSON report on stdout.
"""
import json
import logging
import sys

from opentelemetry import trace
from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

PLUGIN_DIST = "cloudwatch-plugin-otel"


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append({"name": record.name, "level": record.levelname, "msg": record.getMessage()})


def main():
    sys.path.insert(0, sys.argv[1])
    collector = _Collect()
    logging.getLogger().addHandler(collector)
    logging.getLogger().setLevel(logging.DEBUG)

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    real_entry_points = _load.entry_points

    def restricted(**params):
        group = params.get("group")
        eps = real_entry_points(**params)
        if group == "opentelemetry_instrumentor":
            return [ep for ep in eps if ep.name == "langchain"]
        if group in ("opentelemetry_pre_instrument", "opentelemetry_post_instrument"):
            return [ep for ep in eps if ep.dist is not None and ep.dist.name == PLUGIN_DIST]
        return []

    _load.entry_points = restricted
    _load._load_instrumentors(DefaultDistro())

    # pylint: disable=import-outside-toplevel
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    from opentelemetry.instrumentation.genai.langchain import LangChainInstrumentor

    class NamedFakeChatModel(GenericFakeChatModel):
        model_name: str = "fake-chat-model"

    NamedFakeChatModel(messages=iter([AIMessage(content="hi there")])).invoke("hello")

    report = {
        "upstream_instrumented": LangChainInstrumentor().is_instrumented_by_opentelemetry,
        "spans": [
            {"name": s.name, "scope": s.instrumentation_scope.name, "content": "gen_ai.input.messages" in s.attributes}
            for s in exporter.get_finished_spans()
        ],
        "records": collector.records,
    }
    print(json.dumps(report))


if __name__ == "__main__":
    main()
