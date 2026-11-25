# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The session.id, demotion and resource steps apply only while a bridged GenAI instrumentation is active, and
never under the kill switch (no upstream packages needed)."""
import pytest
from genai_helpers import FakeLangChainInstrumentor
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import _pipeline
from plugins.opentelemetry.cloudwatch.genai import langchain as lc

from opentelemetry import propagate, trace


def _state():
    provider = trace.get_tracer_provider()
    return provider._active_span_processor._span_processors, provider.resource, propagate.get_global_textmap()


def test_the_predicate_follows_the_langchain_instrumentor(genai_active):
    assert _pipeline.genai_instrumentation_active()
    FakeLangChainInstrumentor().uninstrument()
    assert not _pipeline.genai_instrumentation_active()


def test_no_active_genai_instrumentation_installs_nothing(span_exporter, monkeypatch):
    monkeypatch.setattr(lc, "_langchain_core_present", lambda: False)  # the LangChain step does not apply
    before = _state()

    _auto_instrumentation.post_instrument()

    assert _state() == before
    assert set(_auto_instrumentation._results.values()) == {None}  # no step applied


@pytest.mark.parametrize("env", [{"CW_PLUGIN_GENAI_ENABLED": "false"}, {"OTEL_SDK_DISABLED": "true"}])
def test_the_kill_switch_installs_nothing(genai_active, span_exporter, monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    before = _state()

    _auto_instrumentation.post_instrument()

    assert _state() == before
    assert _auto_instrumentation._results == {}


def test_an_active_instrumentation_installs_everything(genai_active, span_exporter):
    processors, resource, textmap = _state()

    _auto_instrumentation.post_instrument()

    after_processors, after_resource, after_textmap = _state()
    assert len(after_processors) == len(processors) + 2
    assert after_resource.attributes["aws.service.type"] == "gen_ai_agent"
    assert after_textmap is textmap  # no step changes global propagation
