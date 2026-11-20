# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The real auto-instrumentation loader with our hooks and the real upstream LangChain entry point: in process,
and in ``python -I`` children against real installed-distribution metadata for a `langchain` package."""
import importlib.metadata
import logging
import os

import pytest
from genai_helpers import EVALUABLE, OLD, messages, plugin_entry_points, run_child, summary_parts, write_stub_dist
from plugins.opentelemetry.cloudwatch.genai import langchain as lc

from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro

langchain_instrumentation = pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
pytest.importorskip("langchain_core")

PRE = "opentelemetry_pre_instrument"
POST = "opentelemetry_post_instrument"
INSTRUMENTOR = "opentelemetry_instrumentor"


@pytest.fixture
def restricted_entry_points(monkeypatch):
    """Lets ``_load_instrumentors`` see only our real hooks and the real upstream `langchain` entry point.

    ``_load`` imports ``entry_points`` by name from ``opentelemetry.util._importlib_metadata`` (true on 0.63b0,
    0.64b0 and 0.66b2), so that is the name to patch. The returned EntryPoint objects are the installed ones, so
    the loader's dependency check runs against the real distribution metadata.
    """
    real_entry_points = _load.entry_points
    hooks = {PRE: plugin_entry_points(PRE), POST: plugin_entry_points(POST)}
    upstream = [ep for ep in real_entry_points(group=INSTRUMENTOR) if ep.name == "langchain"]
    assert len(upstream) == 1 and upstream[0].dist.name == "opentelemetry-instrumentation-genai-langchain"
    groups = {**hooks, INSTRUMENTOR: upstream}

    def only_ours(**params):
        return groups.get(params.get("group"), [])

    monkeypatch.setattr(_load, "entry_points", only_ours)


def _instrumented():
    return langchain_instrumentation.LangChainInstrumentor().is_instrumented_by_opentelemetry


def _chat_once():
    # pylint: disable=import-outside-toplevel
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    class NamedFakeChatModel(GenericFakeChatModel):
        # The instrumentor skips chat models without a model name.
        model_name: str = "fake-chat-model"

    NamedFakeChatModel(messages=iter([AIMessage(content="hi there")])).invoke("hello")


def test_precondition_langchain_distribution_is_absent():
    with pytest.raises(importlib.metadata.PackageNotFoundError):
        importlib.metadata.version("langchain")


def test_loader_skips_upstream_and_the_post_hook_activates_it(
    restricted_entry_points, span_exporter, monkeypatch, caplog
):
    caplog.set_level(logging.DEBUG, logger="opentelemetry.instrumentation.auto_instrumentation._load")
    monkeypatch.setenv(OLD, "true")  # what the AgentCore docs tell users to set

    before = dict(os.environ)
    _load._load_instrumentors(DefaultDistro())

    # The loader itself skipped the instrumentor: its dependency check found `langchain` missing.
    assert any(
        r.name == _load.__name__ and "langchain" in r.getMessage() and r.levelno == logging.DEBUG
        for r in caplog.records
    )
    assert _instrumented()
    assert dict(os.environ) == before  # `true` is honored without touching the environment
    (line,) = messages(caplog, logging.INFO)
    assert summary_parts(line) == {
        "content capture": "SPAN_ONLY (translated from true)",
        "langchain": lc.ACTIVATED,
        **EVALUABLE,
    }
    assert messages(caplog, logging.WARNING) == []

    _chat_once()
    chat = [s for s in span_exporter.get_finished_spans() if s.attributes.get("gen_ai.operation.name") == "chat"]
    assert len(chat) == 1
    assert "hello" in chat[0].attributes["gen_ai.input.messages"]  # `true` reached util-genai as SPAN_ONLY


def test_unset_content_capture_stays_off(restricted_entry_points, span_exporter, caplog):
    _load._load_instrumentors(DefaultDistro())

    (line,) = messages(caplog, logging.INFO)
    assert summary_parts(line) == {"content capture": "NO_CONTENT (unset)", "langchain": lc.ACTIVATED, **EVALUABLE}
    _chat_once()
    (chat,) = [s for s in span_exporter.get_finished_spans() if s.attributes.get("gen_ai.operation.name") == "chat"]
    assert "gen_ai.input.messages" not in chat.attributes  # the plugin never turns content capture on


@pytest.mark.parametrize("disabled", ["langchain", " flask , langchain "])
def test_loader_and_plugin_respect_a_disabled_langchain(restricted_entry_points, monkeypatch, caplog, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", disabled)

    _load._load_instrumentors(DefaultDistro())

    assert not _instrumented()
    assert any(m.endswith("langchain=disabled") for m in messages(caplog, logging.INFO))


# A `langchain` package installed: the loader's dependency check decides, and the plugin stays silent -----------

UPSTREAM_SCOPE = "opentelemetry.instrumentation.genai.langchain"


def _plugin(report, level):
    return [r["msg"] for r in report["records"] if r["name"].startswith("plugins.") and r["level"] == level]


@pytest.mark.parametrize(
    "version,instrumented,langchain_part",
    # in / out of the declared range; the evaluable steps apply only while the instrumentor is active
    [("0.3.25", True, {"langchain": "instrumented", **EVALUABLE}), ("2.0.0", False, {})],
)
def test_installed_langchain_leaves_the_decision_to_the_loader(tmp_path, version, instrumented, langchain_part):
    plugin_entry_points(PRE)
    write_stub_dist(tmp_path, "langchain", version)

    report, _ = run_child("child_langchain_loader.py", tmp_path)

    assert report["upstream_instrumented"] is instrumented
    (line,) = _plugin(report, "INFO")
    assert summary_parts(line) == {"content capture": "NO_CONTENT (unset)", **langchain_part}
    assert _plugin(report, "WARNING") == []
    assert [s["scope"] for s in report["spans"]] == ([UPSTREAM_SCOPE] if instrumented else [])
