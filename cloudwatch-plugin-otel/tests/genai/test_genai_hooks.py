# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The hook shim, the step runner and the startup summary line, in process."""
import ast
import importlib
import logging
import os
import sys
import types
from pathlib import Path

import cloudwatch_plugin_otel_hooks as shim
import pytest
from genai_helpers import (
    EVALUABLE,
    NEW,
    OLD,
    FakeLangChainInstrumentor,
    fake_upstream_module,
    install_fake_util_genai,
    messages,
    summary_parts,
)
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import _settings
from plugins.opentelemetry.cloudwatch.genai import langchain as lc

from opentelemetry.test.globals_test import reset_trace_globals

_IMPL = "plugins.opentelemetry.cloudwatch._auto_instrumentation"


# Shim --------------------------------------------------------------------------------------------------------


def test_shim_imports_only_the_standard_library_at_module_level():
    tree = ast.parse(Path(shim.__file__).read_text(encoding="utf-8"))
    top_level = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    names = {alias.name for node in top_level if isinstance(node, ast.Import) for alias in node.names}
    names |= {node.module for node in top_level if isinstance(node, ast.ImportFrom)}
    assert names and all(name.split(".")[0] in sys.stdlib_module_names for name in names), names


@pytest.mark.parametrize(
    "env,imports",
    [
        ({}, True),
        ({"CW_PLUGIN_GENAI_ENABLED": " False "}, False),
        ({"OTEL_SDK_DISABLED": " True "}, False),
        ({"OTEL_SDK_DISABLED": "false"}, True),
        ({"CW_PLUGIN_GENAI_ENABLED": "bogus"}, True),  # `_settings` warns about it
    ],
)
@pytest.mark.parametrize("hook", ["pre_instrument", "post_instrument"])
def test_kill_switch_and_sdk_disabled_return_before_importing_the_plugin_package(monkeypatch, env, imports, hook):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    calls = []
    real_import_module = importlib.import_module
    monkeypatch.setattr(
        importlib, "import_module", lambda name, package=None: calls.append(name) or real_import_module(name, package)
    )
    monkeypatch.setattr(_auto_instrumentation, hook, lambda: None)

    getattr(shim, hook)()
    assert (_IMPL in calls) is imports


def test_settings_uses_the_shims_kill_switch_name():
    assert _settings.CW_PLUGIN_GENAI_ENABLED == shim.CW_PLUGIN_GENAI_ENABLED


@pytest.mark.parametrize("hook", ["pre_instrument", "post_instrument"])
def test_shim_delegates_to_the_implementation(monkeypatch, hook):
    calls = []
    monkeypatch.setattr(_auto_instrumentation, hook, lambda: calls.append(hook))

    getattr(shim, hook)()
    assert calls == [hook]


@pytest.fixture
def failing_import(monkeypatch):
    """Makes importing the implementation fail with ``error``, as a broken or shadowed install would."""
    real_import_module = importlib.import_module
    state = {"error": ModuleNotFoundError("No module named 'opentelemetry.sdk'", name="opentelemetry.sdk")}

    def fake_import_module(name, package=None):
        if name.startswith("plugins.opentelemetry.cloudwatch"):
            raise state["error"]
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr(shim, "_shadow_reported", False)
    return state


def _shim_records(caplog):
    return [r for r in caplog.records if r.name == "plugins.opentelemetry.cloudwatch"]


@pytest.mark.parametrize("hook", ["pre_instrument", "post_instrument"])
def test_shim_import_failure_is_logged_at_debug_and_swallowed(failing_import, caplog, hook):
    caplog.set_level(logging.DEBUG, logger="plugins.opentelemetry.cloudwatch")

    getattr(shim, hook)()

    records = _shim_records(caplog)
    assert [r.levelno for r in records] == [logging.DEBUG]
    assert records[0].exc_info is not None and _IMPL in records[0].getMessage()


def _shadow(monkeypatch, failing_import, directory):
    shadow = types.ModuleType("plugins")
    shadow.__file__ = str(directory / "plugins" / "__init__.py")
    shadow.__path__ = [str(directory / "plugins")]
    monkeypatch.setitem(sys.modules, "plugins", shadow)
    failing_import["error"] = ModuleNotFoundError(
        "No module named 'plugins.opentelemetry'", name="plugins.opentelemetry"
    )
    return shadow


def test_shadowing_plugins_package_is_a_warning_once(failing_import, monkeypatch, caplog, tmp_path):
    caplog.set_level(logging.DEBUG, logger="plugins.opentelemetry.cloudwatch")
    shadow = _shadow(monkeypatch, failing_import, tmp_path)

    shim.pre_instrument()
    shim.post_instrument()

    records = _shim_records(caplog)
    assert [r.levelno for r in records] == [logging.WARNING, logging.DEBUG, logging.DEBUG]
    warning = records[0].getMessage()
    assert shadow.__file__ in warning and "inactive" in warning and "Fix: rename your `plugins` package" in warning
    assert records[0].exc_info is None and records[1].exc_info is not None  # the traceback is at DEBUG


class LoggingHandler(logging.Handler):
    """Looks like OTel's LoggingHandler (as logs auto-instrumentation installs on the root logger)."""

    def emit(self, record):
        pass


LoggingHandler.__module__ = "opentelemetry.sdk._logs._internal"


@pytest.mark.parametrize("root_handlers,printed", [([LoggingHandler()], True), ([logging.NullHandler()], False)])
def test_shadow_warning_is_printed_when_only_otels_handler_is_on_the_root(
    failing_import, monkeypatch, capsys, tmp_path, root_handlers, printed
):
    _shadow(monkeypatch, failing_import, tmp_path)
    monkeypatch.setattr(logging.getLogger(), "handlers", root_handlers)

    shim.pre_instrument()
    assert ("Fix: rename your `plugins` package" in capsys.readouterr().err) is printed


def test_shadow_traceback_is_printed_at_debug_on_the_direct_path(failing_import, monkeypatch, capsys, tmp_path):
    _shadow(monkeypatch, failing_import, tmp_path)
    monkeypatch.setattr(logging.getLogger(), "handlers", [LoggingHandler()])
    logging.getLogger("plugins.opentelemetry.cloudwatch").setLevel(logging.DEBUG)

    shim.pre_instrument()
    err = capsys.readouterr().err
    assert "Fix: rename your `plugins` package" in err and "ModuleNotFoundError" in err


def test_a_failure_while_reporting_never_escapes(failing_import, monkeypatch, tmp_path):
    _shadow(monkeypatch, failing_import, tmp_path)

    def broken(*args, **kwargs):
        raise RuntimeError("reporting bug")

    monkeypatch.setattr(shim, "_warn_shadowed", broken)
    shim.pre_instrument()  # must not raise


def test_editable_install_is_not_called_shadowing(failing_import, monkeypatch, caplog, tmp_path):
    """Editable layout: the shim lives in one directory and `plugins` in another (the source tree)."""
    caplog.set_level(logging.DEBUG, logger="plugins.opentelemetry.cloudwatch")
    source = tmp_path / "src" / "plugins"
    (source / "opentelemetry" / "cloudwatch").mkdir(parents=True)
    (source / "opentelemetry" / "cloudwatch" / "_auto_instrumentation.py").write_text("", "utf-8")
    ours = types.ModuleType("plugins")
    ours.__path__ = [str(source)]
    monkeypatch.setitem(sys.modules, "plugins", ours)
    failing_import["error"] = ModuleNotFoundError("No module named 'plugins.x'", name="plugins.x")

    shim.pre_instrument()
    assert [r.levelno for r in _shim_records(caplog)] == [logging.DEBUG]


def test_a_broken_install_of_our_own_plugins_package_is_not_called_shadowing(failing_import, caplog):
    caplog.set_level(logging.DEBUG, logger="plugins.opentelemetry.cloudwatch")
    # `plugins` resolves to this distribution's own directory: the failure has another cause.
    failing_import["error"] = ModuleNotFoundError(
        "No module named 'plugins.opentelemetry.cloudwatch.genai'", name="plugins.opentelemetry.cloudwatch.genai"
    )

    shim.pre_instrument()
    assert [r.levelno for r in _shim_records(caplog)] == [logging.DEBUG]


@pytest.mark.parametrize("hook", ["pre_instrument", "post_instrument"])
def test_shim_call_failure_is_logged_at_warning_and_swallowed(monkeypatch, caplog, hook):
    def boom():
        raise RuntimeError("bug")

    monkeypatch.setattr(_auto_instrumentation, hook, boom)

    getattr(shim, hook)()

    records = [r for r in caplog.records if r.name == "plugins.opentelemetry.cloudwatch"]
    assert [r.levelno for r in records] == [logging.WARNING]
    assert records[0].getMessage().startswith("CW-PLUGIN: ")
    assert records[0].exc_info is not None and records[0].exc_info[0] is RuntimeError


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_shim_lets_base_exceptions_propagate(monkeypatch, exc):
    def interrupt():
        raise exc()

    monkeypatch.setattr(_auto_instrumentation, "pre_instrument", interrupt)
    with pytest.raises(exc):
        shim.pre_instrument()


def test_shim_lets_base_exceptions_from_the_import_propagate(failing_import):
    failing_import["error"] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        shim.post_instrument()


# Step runner -------------------------------------------------------------------------------------------------


def test_step_lists():
    assert [name for name, _ in _auto_instrumentation._PRE_STEPS] == ["content capture"]
    assert [name for name, _ in _auto_instrumentation._POST_STEPS] == [
        "langchain",
        "session.id",
        "nested chat demotion",
        "agent identity",
    ]


def test_a_failing_step_logs_one_warning_and_the_next_step_runs(monkeypatch, caplog):
    ran = []

    def boom():
        raise ValueError("step bug")

    monkeypatch.setattr(
        _auto_instrumentation, "_PRE_STEPS", [("test.boom", boom), ("test.next", lambda: ran.append(1))]
    )

    _auto_instrumentation.pre_instrument()

    assert ran == [1]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage().startswith("CW-PLUGIN: test.boom setup failed")
    assert warnings[0].exc_info is not None and warnings[0].exc_info[0] is ValueError


@pytest.mark.parametrize("hook,steps", [("pre_instrument", "_PRE_STEPS"), ("post_instrument", "_POST_STEPS")])
@pytest.mark.parametrize("env", [{"CW_PLUGIN_GENAI_ENABLED": "false"}, {"OTEL_SDK_DISABLED": "true"}])
def test_no_step_runs_when_genai_is_disabled(monkeypatch, caplog, hook, steps, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    ran = []
    monkeypatch.setattr(_auto_instrumentation, steps, [("test.count", lambda: ran.append(1))])

    getattr(_auto_instrumentation, hook)()
    assert ran == [] and caplog.records == []


def test_run_records_the_step_result():
    _auto_instrumentation._run("test.value", lambda: "ok")
    assert _auto_instrumentation._results["test.value"] == "ok"


# Summary --------------------------------------------------------------------------------------------------------------


@pytest.fixture
def fake_discovery(monkeypatch, span_exporter):
    """langchain-core and the upstream instrumentor (a fake) installed, `langchain` not; an SDK TracerProvider."""
    monkeypatch.setattr(lc, "_langchain_core_present", lambda: True)
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, fake_upstream_module())
    monkeypatch.setattr(lc, "distribution_version", lambda name: "1.3b0" if name == lc.UPSTREAM_DIST else None)


@pytest.fixture
def stand_in_util_genai(monkeypatch):
    """A stand-in util-genai, so the content-capture patch installs in every env."""
    install_fake_util_genai(monkeypatch)


def _summary(caplog):
    infos = messages(caplog, logging.INFO)
    assert all(m.startswith("CW-PLUGIN genai: ") for m in infos), infos  # the one INFO line
    return infos


def _run_hooks():
    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()


def test_exactly_one_info_line_after_a_full_run(fake_discovery, stand_in_util_genai, caplog):
    _run_hooks()
    (line,) = _summary(caplog)
    assert summary_parts(line) == {"content capture": "NO_CONTENT (unset)", "langchain": lc.ACTIVATED, **EVALUABLE}


def test_summary_without_an_sdk_tracer_provider(fake_discovery, stand_in_util_genai, caplog):
    reset_trace_globals()  # the global provider is the API's proxy again
    _run_hooks()
    (line,) = _summary(caplog)
    skipped = "skipped (not an SDK TracerProvider)"
    assert summary_parts(line) == {
        "content capture": "NO_CONTENT (unset)",
        "langchain": lc.ACTIVATED,
        **{name: skipped for name in EVALUABLE},
    }


@pytest.mark.parametrize(
    "env,expected",
    [
        (
            {OLD: "true"},
            {"content capture": "SPAN_ONLY (translated from true)", "langchain": lc.ACTIVATED, **EVALUABLE},
        ),
        ({OLD: "span_and_event"}, {"content capture": "SPAN_AND_EVENT", "langchain": lc.ACTIVATED, **EVALUABLE}),
        (  # no active GenAI instrumentation: the evaluable steps do not apply
            {"OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": "langchain"},
            {"content capture": "NO_CONTENT (unset)", "langchain": lc.DISABLED},
        ),
    ],
)
def test_summary_line_reflects_the_outcomes(fake_discovery, stand_in_util_genai, monkeypatch, caplog, env, expected):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    _run_hooks()
    (line,) = _summary(caplog)
    assert summary_parts(line) == expected


def test_summary_when_already_instrumented(fake_discovery, stand_in_util_genai, caplog):
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)
    _run_hooks()
    assert summary_parts(_summary(caplog)[0]) == {
        "content capture": "NO_CONTENT (unset)",
        "langchain": lc.INSTRUMENTED,
        **EVALUABLE,
    }


def test_summary_when_activation_fails(fake_discovery, stand_in_util_genai, caplog):
    FakeLangChainInstrumentor.fail = True
    _run_hooks()
    assert _summary(caplog)[0].endswith("langchain=failed")  # nothing instrumenting: no evaluable steps


def test_summary_has_only_the_steps_that_applied(fake_discovery, stand_in_util_genai, monkeypatch, caplog):
    monkeypatch.setattr(lc, "_langchain_core_present", lambda: False)
    _run_hooks()
    assert _summary(caplog) == ["CW-PLUGIN genai: content capture=NO_CONTENT (unset)"]


def test_no_summary_when_no_step_applied(monkeypatch, caplog):
    monkeypatch.setattr(lc, "_langchain_core_present", lambda: False)
    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [("content capture", lambda: None)])
    _run_hooks()
    assert _summary(caplog) == [] and messages(caplog, logging.WARNING) == []


def test_summary_omits_steps_that_raised(fake_discovery, monkeypatch, caplog):
    def boom():
        raise RuntimeError("bug")

    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [("content capture", boom)])
    _run_hooks()

    assert summary_parts(_summary(caplog)[0]) == {"langchain": lc.ACTIVATED, **EVALUABLE}
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


@pytest.mark.parametrize(
    "env",
    [
        {},
        {OLD: "true"},
        {OLD: "bogus"},
        {NEW: "true"},
        {OLD: "SPAN_ONLY", NEW: "NO_CONTENT"},
        {"CW_PLUGIN_GENAI_ENABLED": "false", OLD: "true"},
    ],
)
def test_hooks_never_change_the_environment(monkeypatch, fake_discovery, stand_in_util_genai, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    before = dict(os.environ)

    _run_hooks()

    assert dict(os.environ) == before
