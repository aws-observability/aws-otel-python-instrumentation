# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The LangChain step against a fake instrumentor, with discovery patched (no upstream packages needed)."""
import logging
import sys

import pytest
from genai_helpers import FakeLangChainInstrumentor, fake_upstream_module, messages
from plugins.opentelemetry.cloudwatch.genai import langchain as lc


@pytest.fixture
def discovery(monkeypatch):
    """Controls what the step sees: langchain_core, installed distributions, and the upstream module."""
    state = {"langchain_core": True, "versions": {lc.UPSTREAM_DIST: "1.3b0", "langchain-core": "1.0.0"}}
    monkeypatch.setattr(lc, "_langchain_core_present", lambda: state["langchain_core"])
    monkeypatch.setattr(lc, "distribution_version", lambda name: state["versions"].get(name))
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, fake_upstream_module())
    return state


def _warnings(caplog):
    return messages(caplog, logging.WARNING)


def test_activates_once_with_skip_dep_check(discovery, caplog):
    assert lc.langchain_step() == lc.ACTIVATED
    assert lc.langchain_step() == lc.INSTRUMENTED  # no second instrument()

    assert FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert FakeLangChainInstrumentor.calls == [{}]  # skip_dep_check consumed by instrument(); no provider kwargs
    assert lc.ACTIVATED == "activated (langchain-core only)"
    assert messages(caplog) == []  # the outcome is carried by the summary line only


def test_activation_bypasses_the_dependency_check(discovery):
    # The fake declares `langchain >= 0.3.22, < 2`, which is not installed: a checked instrument() would refuse.
    assert FakeLangChainInstrumentor()._check_dependency_conflicts() is not None
    assert lc.langchain_step() == lc.ACTIVATED


def test_no_langchain_core_is_silent(discovery, caplog):
    discovery["langchain_core"] = False
    del discovery["versions"][lc.UPSTREAM_DIST]

    assert lc.langchain_step() is None
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []


def test_langchain_core_without_upstream_warns_once(discovery, monkeypatch, caplog):
    del discovery["versions"][lc.UPSTREAM_DIST]
    monkeypatch.delitem(sys.modules, lc.UPSTREAM_MODULE)

    assert lc.langchain_step() == lc.NOT_INSTALLED
    assert lc.langchain_step() == lc.NOT_INSTALLED
    assert _warnings(caplog) == [
        "CW-PLUGIN genai: langchain-core is installed but opentelemetry-instrumentation-genai-langchain is not; "
        "install it to get LangChain/LangGraph spans"
    ]
    assert lc.UPSTREAM_MODULE not in sys.modules


@pytest.mark.parametrize("disabled", ["langchain", " flask , langchain ", "*", "flask,*"])
def test_respects_the_disabled_instrumentations_list(discovery, monkeypatch, caplog, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", disabled)

    assert lc.langchain_step() == lc.DISABLED
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []


@pytest.mark.parametrize("disabled", ["flask", "langchain_core", "lang chain", ""])
def test_other_disabled_entries_do_not_match(discovery, monkeypatch, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", disabled)
    assert lc.langchain_step() == lc.ACTIVATED


@pytest.mark.parametrize("version", ["0.3.25", "2.0.0"])
def test_installed_langchain_leaves_the_loaders_decision_silently(discovery, caplog, version):
    discovery["versions"]["langchain"] = version

    assert lc.langchain_step() is None
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []


def test_upstream_is_imported_only_when_forcing(discovery, monkeypatch):
    monkeypatch.delitem(sys.modules, lc.UPSTREAM_MODULE)
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "langchain")

    assert lc.upstream_instrumented() is False  # not imported by the loader: cannot be instrumented
    assert lc.langchain_step() == lc.DISABLED
    assert lc.UPSTREAM_MODULE not in sys.modules


def test_instrument_raising_warns_and_does_not_raise(discovery, caplog):
    FakeLangChainInstrumentor.fail = True

    assert lc.langchain_step() == lc.FAILED
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    (warning,) = _warnings(caplog)
    assert warning.startswith("CW-PLUGIN genai: no LangChain spans: activating ") and "RuntimeError('boom')" in warning


def test_instrumentor_import_failure_warns_and_does_not_raise(discovery, monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, None)  # import raises ImportError

    assert lc.langchain_step() == lc.FAILED
    (warning,) = _warnings(caplog)
    assert "no LangChain spans: activating opentelemetry-instrumentation-genai-langchain failed" in warning


@pytest.mark.parametrize("env", [{"OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": "langchain"}, {}])
def test_already_instrumented_wins_over_disabled_and_installed_langchain(discovery, monkeypatch, caplog, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    discovery["versions"]["langchain"] = "1.4.0"
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)
    FakeLangChainInstrumentor.calls = []

    assert lc.langchain_step() == lc.INSTRUMENTED
    assert FakeLangChainInstrumentor.calls == []
    assert messages(caplog) == []


@pytest.mark.parametrize("find_spec,expected", [(lambda name: object(), True), (lambda name: None, False)])
def test_langchain_core_discovery(monkeypatch, find_spec, expected):
    monkeypatch.setattr(lc.importlib.util, "find_spec", find_spec)
    assert lc._langchain_core_present() is expected


def test_langchain_core_discovery_tolerates_broken_specs(monkeypatch):
    def broken(name):
        raise ValueError("langchain_core.__spec__ is None")

    monkeypatch.setattr(lc.importlib.util, "find_spec", broken)
    assert lc._langchain_core_present() is False
