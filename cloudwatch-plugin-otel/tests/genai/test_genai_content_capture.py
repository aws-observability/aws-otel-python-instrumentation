# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Content capture: `true`/`false` honored for util-genai handlers, nothing else changed, never a default.

Resolution tests run against the real util-genai when it is installed, and always against a stand-in module with
the same shape, so the oldest tox envs (without util-genai) cover the logic too.
"""
import logging
import os
import sys

import pytest
from genai_helpers import (
    FAKE_HANDLER_SOURCE,
    FAKE_INVOCATION_SOURCE,
    FAKE_SITES,
    FAKE_UTILS_SOURCE,
    NEW,
    OLD,
    install_fake_util_genai,
    messages,
)
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import content_capture

LANGCHAIN = "opentelemetry.instrumentation.genai.langchain"
OTHER = "opentelemetry.instrumentation.openai_v2"


@pytest.fixture(params=["stand-in", "util-genai"])
def util_genai(request, monkeypatch):
    """``(utils, handler, _invocation)`` modules: stand-ins, or the real util-genai ones when installed."""
    if request.param == "util-genai":
        return tuple(pytest.importorskip(name) for name in FAKE_SITES)
    return install_fake_util_genai(monkeypatch)


@pytest.fixture
def handler_module(util_genai):
    return util_genai[1]


def _mode(handler_module, scope):
    handler = handler_module.TelemetryHandler(instrumentation_scope_name=scope)
    # Read-only look at the resolved mode: the public should_capture_content() cannot tell SPAN_ONLY from EVENT_ONLY.
    return handler._content_capturing_mode.name


def _summary():
    return content_capture.content_capture_step()


def _warnings(caplog):
    return messages(caplog, logging.WARNING)


# Resolution ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env,mode,summary",
    [
        ({}, "NO_CONTENT", "NO_CONTENT (unset)"),
        ({OLD: " "}, "NO_CONTENT", "NO_CONTENT (unset)"),
        ({NEW: "SPAN_ONLY"}, "NO_CONTENT", "NO_CONTENT (unset)"),  # the new name is not read
        ({NEW: "true"}, "NO_CONTENT", "NO_CONTENT (unset)"),
        ({OLD: "true", NEW: "SPAN_ONLY"}, "NO_CONTENT", "NO_CONTENT"),  # not translated: the new name must win
        ({OLD: "true"}, "SPAN_ONLY", "SPAN_ONLY (translated from true)"),
        ({OLD: " TRUE "}, "SPAN_ONLY", "SPAN_ONLY (translated from true)"),
        ({OLD: "False"}, "NO_CONTENT", "NO_CONTENT"),  # upstream's own NO_CONTENT
        ({OLD: "span_and_event"}, "SPAN_AND_EVENT", "SPAN_AND_EVENT"),
        ({OLD: "EVENT_ONLY", NEW: "SPAN_ONLY"}, "EVENT_ONLY", "EVENT_ONLY"),
        ({OLD: "bogus"}, "NO_CONTENT", "NO_CONTENT"),
    ],
)
def test_resolution_is_the_same_for_every_handler(handler_module, monkeypatch, env, mode, summary):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    before = dict(os.environ)

    assert _summary() == summary
    assert _mode(handler_module, LANGCHAIN) == mode
    assert _mode(handler_module, OTHER) == mode
    assert _mode(handler_module, None) == mode
    assert dict(os.environ) == before


def test_unset_is_upstream_no_content_without_a_warning(handler_module, caplog):
    assert _summary() == "NO_CONTENT (unset)"
    assert _warnings(caplog) == []


def test_without_util_genai_the_step_does_not_apply(monkeypatch, caplog):
    monkeypatch.setattr(content_capture.importlib.util, "find_spec", lambda name: None)
    monkeypatch.setenv(OLD, "true")

    assert content_capture.content_capture_step() is None
    assert content_capture._patch == {} and _warnings(caplog) == []


@pytest.mark.parametrize("value", ["true", "True", "false", "SPAN_ONLY", "bogus"])
def test_no_plugin_warning_for_any_value(handler_module, monkeypatch, caplog, value):
    monkeypatch.setenv(OLD, value)
    content_capture.content_capture_step()
    _mode(handler_module, LANGCHAIN)
    assert not [m for m in _warnings(caplog) if m.startswith("CW-PLUGIN")]  # util-genai warns about invalid values


@pytest.mark.parametrize("env", [{"CW_PLUGIN_GENAI_ENABLED": "false"}, {"OTEL_SDK_DISABLED": "true"}])
def test_genai_disabled_installs_nothing(handler_module, monkeypatch, env):
    monkeypatch.setenv(OLD, "true")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    resolver = handler_module.get_content_capturing_mode

    _auto_instrumentation.pre_instrument()

    assert handler_module.get_content_capturing_mode is resolver
    assert _mode(handler_module, LANGCHAIN) == "NO_CONTENT"  # upstream: `true` is not a mode


def test_applies_without_langchain(handler_module, monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "langchain_core", None)  # not importable
    monkeypatch.setenv(OLD, "true")

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()

    assert _mode(handler_module, OTHER) == "SPAN_ONLY"
    assert messages(caplog, logging.INFO) == ["CW-PLUGIN genai: content capture=SPAN_ONLY (translated from true)"]


def test_env_changes_after_the_hook_are_honoured(handler_module, monkeypatch):
    content_capture.content_capture_step()
    monkeypatch.setenv(OLD, "true")
    assert _mode(handler_module, OTHER) == "SPAN_ONLY"
    monkeypatch.setenv(OLD, "NO_CONTENT")
    assert _mode(handler_module, OTHER) == "NO_CONTENT"


# Every call site, and deferring to upstream -------------------------------------------------------------------


def test_every_binding_site_resolves_the_alias(util_genai, monkeypatch):
    monkeypatch.setenv(OLD, "true")
    content_capture.content_capture_step()
    for module in util_genai:  # utils, handler, and invocations created without a handler
        assert module.get_content_capturing_mode().name == "SPAN_ONLY", module.__name__


def test_invocation_created_without_a_handler_resolves_the_alias(monkeypatch):
    _, _, invocation = install_fake_util_genai(monkeypatch)
    monkeypatch.setenv(OLD, "true")
    content_capture.content_capture_step()
    assert invocation.Invocation()._content_capturing_mode.name == "SPAN_ONLY"


@pytest.mark.parametrize("upstream_mode", ["SPAN_ONLY", "SPAN_AND_EVENT"])
def test_a_util_genai_that_resolves_true_itself_wins(monkeypatch, upstream_mode):
    utils, handler, _ = install_fake_util_genai(monkeypatch)

    def resolves_true():  # a future util-genai that still reads the old name and resolves `true` itself
        value = os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "")
        return utils.ContentCapturingMode[upstream_mode] if value == "true" else utils.ContentCapturingMode.NO_CONTENT

    for module in (utils, handler, sys.modules[FAKE_SITES[2]]):
        monkeypatch.setattr(module, "get_content_capturing_mode", resolves_true)
    monkeypatch.setenv(OLD, "true")

    assert _summary() == upstream_mode
    assert _mode(handler, LANGCHAIN) == upstream_mode  # the plugin defers: no override


def test_a_util_genai_that_stops_reading_the_old_name_gets_no_patch(monkeypatch, caplog):
    utils, handler, invocation = install_fake_util_genai(monkeypatch)

    def reads_only_the_new_name():
        value = os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT", "").strip().upper()
        return utils.ContentCapturingMode[value] if value else utils.ContentCapturingMode.NO_CONTENT

    for module in (utils, handler, invocation):
        monkeypatch.setattr(module, "get_content_capturing_mode", reads_only_the_new_name)
    monkeypatch.setenv(OLD, "true")

    assert _summary() == "unknown (translation unavailable)"
    assert _mode(handler, LANGCHAIN) == "NO_CONTENT"  # `true` is not revived under a name util-genai ignores
    assert content_capture._patch == {}
    (warning,) = _warnings(caplog)
    assert f"no longer reads {OLD}" in warning and "Fix: set the content-capture mode util-genai documents" in warning
    assert f"Fix: {OLD}" not in warning


def test_binding_sites_must_be_exactly_the_known_ones(monkeypatch):
    install_fake_util_genai(monkeypatch)
    monkeypatch.setattr(content_capture, "_binding_sites", lambda: set(FAKE_SITES) | {"opentelemetry.util.genai.new"})
    assert content_capture._install() is not None and content_capture._patch == {}


def test_installed_util_genai_has_exactly_the_known_binding_sites():
    """Runs the real source scan in the latest envs (unpinned), so upstream drift fails CI."""
    pytest.importorskip("opentelemetry.util.genai.handler")
    assert content_capture._binding_sites() == set(content_capture._SITES)


def test_false_is_upstreams_own_result(handler_module, monkeypatch):
    monkeypatch.setenv(OLD, "false")
    content_capture.content_capture_step()
    assert _mode(handler_module, LANGCHAIN) == "NO_CONTENT"


def test_upstreams_false_warning_about_true_is_dropped_while_translating(handler_module, monkeypatch, caplog):
    monkeypatch.setenv(OLD, "true")
    content_capture.content_capture_step()
    _mode(handler_module, LANGCHAIN)
    assert not [r for r in caplog.records if r.name == FAKE_SITES[0] and "not a valid option" in r.getMessage()]

    monkeypatch.setenv(NEW, "SPAN_ONLY")  # no translation then: upstream's warning is accurate and kept
    _mode(handler_module, LANGCHAIN)
    assert [r for r in caplog.records if r.name == FAKE_SITES[0] and "not a valid option" in r.getMessage()]


def test_unrecognized_values_keep_upstreams_warning(handler_module, monkeypatch, caplog):
    monkeypatch.setenv(OLD, "bogus")
    content_capture.content_capture_step()
    _mode(handler_module, LANGCHAIN)
    assert [r for r in caplog.records if r.name == FAKE_SITES[0] and "bogus" in r.getMessage()]


# Feature detection ---------------------------------------------------------------------------------------------


def ATTRIBUTE_ACCESS(source):  # pylint: disable=invalid-name
    return source.replace(
        "from opentelemetry.util.genai.utils import ContentCapturingMode, get_content_capturing_mode",
        "import sys\nfrom opentelemetry.util.genai.utils import ContentCapturingMode",
    ).replace(
        "= get_content_capturing_mode()", '= sys.modules["opentelemetry.util.genai.utils"].get_content_capturing_mode()'
    )


@pytest.mark.parametrize(
    "handler_change,invocation_change",
    [
        (lambda s: s.replace("self._content_capturing_mode = get_content_capturing_mode()", "self._mode = None"), None),
        # A refactor that resolves through an attribute: replacing the module-level name would have no effect.
        (ATTRIBUTE_ACCESS, None),
        # Any one site rebound to something else: all or nothing.
        (None, lambda s: s + "\n\ndef get_content_capturing_mode():\n    return None\n"),
        (None, lambda s: s.replace("from opentelemetry.util.genai.utils import get_content_capturing_mode", "")),
    ],
    ids=["init-does-not-resolve", "attribute-access", "invocation-rebinds", "invocation-unbound"],
)
@pytest.mark.parametrize("value", ["true", "True"])
def test_unexpected_util_genai_shape_installs_nothing_and_warns(
    monkeypatch, caplog, handler_change, invocation_change, value
):
    sources = {}
    if handler_change:
        sources["handler_source"] = handler_change(FAKE_HANDLER_SOURCE)
    if invocation_change:
        sources["invocation_source"] = invocation_change(FAKE_INVOCATION_SOURCE)
    modules = install_fake_util_genai(monkeypatch, **sources)
    before = [getattr(module, "get_content_capturing_mode", None) for module in modules]
    monkeypatch.setenv(OLD, value)

    summary = _summary()

    assert [getattr(module, "get_content_capturing_mode", None) for module in modules] == before
    assert summary == "NO_CONTENT (translation unavailable)"
    (warning,) = _warnings(caplog)
    assert f"{OLD}=true means NO_CONTENT" in warning and f"Fix: {OLD}=SPAN_ONLY" in warning


def test_resolver_with_parameters_installs_nothing(monkeypatch):
    utils, _, _ = install_fake_util_genai(monkeypatch)
    source = FAKE_UTILS_SOURCE.replace(
        "def get_content_capturing_mode():", "def get_content_capturing_mode(name=None):"
    )
    exec(compile(source, "<fake utils>", "exec"), utils.__dict__)  # pylint: disable=exec-used
    assert content_capture._install() is not None and content_capture._patch == {}


@pytest.mark.parametrize("value", ["", "SPAN_ONLY", "false"])
def test_unavailable_translation_is_silent_when_nothing_needs_translating(monkeypatch, caplog, value):
    install_fake_util_genai(monkeypatch)
    monkeypatch.setitem(sys.modules, "opentelemetry.util.genai.handler", None)  # import raises ImportError
    monkeypatch.setenv(OLD, value)

    expected = {"": "NO_CONTENT (unset)", "SPAN_ONLY": "SPAN_ONLY", "false": "NO_CONTENT"}
    assert _summary() == expected[value]  # the effective mode
    assert content_capture._patch == {}
    assert _warnings(caplog) == []


def test_attribute_access_stand_in_still_resolves_without_the_patch(monkeypatch):
    """Control for the attribute-access case: the stand-in itself works; only the shape check rejects it."""
    _, handler, _ = install_fake_util_genai(monkeypatch, handler_source=ATTRIBUTE_ACCESS(FAKE_HANDLER_SOURCE))
    monkeypatch.setenv(OLD, "SPAN_ONLY")
    assert handler.TelemetryHandler()._content_capturing_mode.name == "SPAN_ONLY"


def test_patch_is_idempotent_and_reversible(util_genai):
    originals = [module.get_content_capturing_mode for module in util_genai]

    content_capture.content_capture_step()
    patched = [module.get_content_capturing_mode for module in util_genai]
    assert len(set(map(id, patched))) == 1 and patched[0].__wrapped__ is originals[0]
    assert content_capture._install() is None  # second install: no-op
    assert [module.get_content_capturing_mode for module in util_genai] == patched

    content_capture._unpatch()
    assert [module.get_content_capturing_mode for module in util_genai] == originals
    assert not logging.getLogger(FAKE_SITES[0]).filters


# Through the hooks, with the real instrumentor (public signal: the spans) ---------------------------------------


def _chat_span_input(span_exporter):
    # pylint: disable=import-outside-toplevel
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    class NamedFakeChatModel(GenericFakeChatModel):
        model_name: str = "fake-chat-model"  # the instrumentor skips chat models without a model name

    NamedFakeChatModel(messages=iter([AIMessage(content="hi there")])).invoke("hello")
    (chat,) = [s for s in span_exporter.get_finished_spans() if s.attributes.get("gen_ai.operation.name") == "chat"]
    return chat.attributes.get("gen_ai.input.messages")


@pytest.mark.parametrize("value,captured", [("true", True), (None, False)])
def test_pre_hook_through_the_real_instrumentor(span_exporter, monkeypatch, value, captured):
    langchain = pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
    pytest.importorskip("langchain_core")
    if value is not None:
        monkeypatch.setenv(OLD, value)
    before = dict(os.environ)

    _auto_instrumentation.pre_instrument()
    langchain.LangChainInstrumentor().instrument(skip_dep_check=True)

    assert ("hello" in (_chat_span_input(span_exporter) or "")) is captured  # unset: never turned on by the plugin
    assert dict(os.environ) == before


def test_without_the_pre_hook_true_captures_no_content(span_exporter, monkeypatch):
    """Control: proves the `true` case above depends on the plugin."""
    langchain = pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
    pytest.importorskip("langchain_core")
    monkeypatch.setenv(OLD, "true")

    langchain.LangChainInstrumentor().instrument(skip_dep_check=True)
    assert _chat_span_input(span_exporter) is None
