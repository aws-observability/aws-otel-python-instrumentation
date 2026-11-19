# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Switch parsing, gating of the GenAI layer, and its logging rules."""
import io
import logging
import logging.handlers
import os
import queue
import sys

import pytest
from genai_helpers import OLD, messages
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import _settings

SWITCHES = (_settings.CW_PLUGIN_GENAI_ENABLED,)


# Switches ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", SWITCHES)
def test_switches_default_to_true(name, caplog):
    assert _settings.env_flag(name) is True
    assert messages(caplog, logging.WARNING) == []


@pytest.mark.parametrize("name", SWITCHES)
@pytest.mark.parametrize(
    "value,expected",
    [("true", True), ("TRUE", True), (" True ", True), ("false", False), ("False", False), ("\tFALSE\n", False)],
)
def test_switch_values_are_stripped_and_case_insensitive(monkeypatch, caplog, name, value, expected):
    monkeypatch.setenv(name, value)
    assert _settings.env_flag(name) is expected
    assert messages(caplog, logging.WARNING) == []


@pytest.mark.parametrize("value", ["", "   "])
def test_empty_switch_means_default(monkeypatch, caplog, value):
    monkeypatch.setenv(_settings.CW_PLUGIN_GENAI_ENABLED, value)
    assert _settings.env_flag(_settings.CW_PLUGIN_GENAI_ENABLED) is True
    assert _settings.env_flag(_settings.CW_PLUGIN_GENAI_ENABLED, default=False) is False
    assert messages(caplog, logging.WARNING) == []


@pytest.mark.parametrize("value", ["1", "0", "yes", "no", "on", "off", "enabled"])
def test_invalid_switch_means_default_with_one_warning(monkeypatch, caplog, value):
    name = _settings.CW_PLUGIN_GENAI_ENABLED
    monkeypatch.setenv(name, value)

    assert _settings.env_flag(name) is True
    assert _settings.env_flag(name) is True
    assert _settings.env_flag(name, default=False) is False

    warnings = messages(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert warnings[0].startswith("CW-PLUGIN genai: ")
    assert name in warnings[0] and repr(value) in warnings[0]
    assert "is not true or false" in warnings[0]


# Gating ------------------------------------------------------------------------------------------------------


def test_active_when_a_supported_distribution_is_installed(genai_installed, caplog):
    assert _settings.genai_active() is True
    assert messages(caplog) == []


@pytest.mark.parametrize("value", ["true", " TRUE "])
def test_sdk_disabled_turns_the_layer_off(genai_installed, monkeypatch, caplog, value):
    caplog.set_level(logging.DEBUG, logger=_settings.LOGGER_NAME)
    monkeypatch.setenv("OTEL_SDK_DISABLED", value)

    assert _settings.genai_active() is False
    assert [r.levelno for r in caplog.records if r.name == _settings.LOGGER_NAME] == [logging.DEBUG]


def test_sdk_disabled_false_keeps_the_layer_on(genai_installed, monkeypatch):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")
    assert _settings.genai_active() is True


def test_kill_switch_turns_the_layer_off(genai_installed, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger=_settings.LOGGER_NAME)
    monkeypatch.setenv(_settings.CW_PLUGIN_GENAI_ENABLED, "False")

    assert _settings.genai_active() is False
    assert messages(caplog, logging.INFO) == [] and messages(caplog, logging.WARNING) == []
    assert any("CW_PLUGIN_GENAI_ENABLED=false" in m for m in messages(caplog, logging.DEBUG))


@pytest.mark.parametrize(
    "value,agent_mode", [("true", True), ("TRUE", True), (" true", False), ("yes", False), ("", False)]
)
def test_agent_mode_flag_is_parsed_like_adot(monkeypatch, value, agent_mode):
    # ADOT 0.23.0 _utils.py:77 compares `.lower() == "true"` without stripping.
    monkeypatch.setenv("AGENT_OBSERVABILITY_ENABLED", value)
    assert _settings.adot_agent_mode() is agent_mode


def test_adot_agent_mode_does_not_turn_the_layer_off(genai_installed, monkeypatch):
    monkeypatch.setenv("AGENT_OBSERVABILITY_ENABLED", "true")
    monkeypatch.setenv("OTEL_PYTHON_DISTRO", "aws_distro")
    assert _settings.genai_status() == "active"


@pytest.mark.parametrize(
    "env,expected",
    [
        ({}, ("AWS_GENAI_INSTRUMENTATION", "auto")),
        ({"AWS_GENAI_INSTRUMENTATION": "Enabled"}, ("AWS_GENAI_INSTRUMENTATION", "enabled")),
        ({"AWS_GENAI_INSTRUMENTATION": "bogus"}, ("AWS_GENAI_INSTRUMENTATION", "auto")),
        ({"AWS_AGENTIC_INSTRUMENTATION": "DISABLED"}, ("AWS_AGENTIC_INSTRUMENTATION", "disabled")),
        (
            {"AWS_GENAI_INSTRUMENTATION": "auto", "AWS_AGENTIC_INSTRUMENTATION": "disabled"},
            ("AWS_GENAI_INSTRUMENTATION", "auto"),
        ),
    ],
)
def test_adot_genai_mode_is_parsed_like_adot(monkeypatch, env, expected):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert _settings.adot_genai_mode() == expected


def test_no_supported_distribution_means_inactive_and_silent(genai_not_installed, caplog):
    caplog.set_level(logging.DEBUG, logger=_settings.LOGGER_NAME)

    env = dict(os.environ)
    assert _settings.genai_active() is False
    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()

    assert dict(os.environ) == env
    assert messages(caplog, logging.INFO) == [] and messages(caplog, logging.WARNING) == []
    assert any("none of opentelemetry-instrumentation-genai-langchain is installed" in m for m in messages(caplog))


def test_distribution_check_reads_metadata_without_importing(monkeypatch):
    module = "opentelemetry.instrumentation.genai.langchain"
    monkeypatch.delitem(sys.modules, module, raising=False)

    monkeypatch.setattr(_settings, "SUPPORTED_GENAI_DISTRIBUTIONS", ("no-such-distribution-cw-plugin", "pytest"))
    assert _settings.genai_distribution_installed() is True
    monkeypatch.setattr(_settings, "SUPPORTED_GENAI_DISTRIBUTIONS", ("no-such-distribution-cw-plugin",))
    assert _settings.genai_distribution_installed() is False
    assert module not in sys.modules


def test_gating_is_computed_once(monkeypatch):
    calls = []

    def installed():
        calls.append(1)
        return True

    monkeypatch.setattr(_settings, "genai_distribution_installed", installed)
    monkeypatch.setattr(_settings, "langchain_core_present", lambda: True)
    assert _settings.genai_active() is True
    monkeypatch.setenv(_settings.CW_PLUGIN_GENAI_ENABLED, "false")
    assert _settings.genai_active() is True  # cached: the env change is not seen
    assert len(calls) == 1


@pytest.mark.parametrize(
    "value,disabled",
    [
        ("langchain", True),
        (" flask , langchain ,", True),
        ("flask,*", True),
        ("flask", False),
        ("", False),
        ("lang chain", False),
    ],
)
def test_instrumentation_disabled(monkeypatch, value, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", value)
    assert _settings.instrumentation_disabled("langchain") is disabled


# Logging -----------------------------------------------------------------------------------------------------


def _emit_through_fallback(root_handlers):
    stream = io.StringIO()
    handler = _settings._StderrFallbackHandler(stream)
    root = logging.getLogger()
    saved = root.handlers[:]
    root.handlers = root_handlers
    try:
        handler.emit(logging.LogRecord("plugins.x", logging.WARNING, __file__, 1, "CW-PLUGIN genai: hi", None, None))
    finally:
        root.handlers = saved
    return stream.getvalue()


class LoggingHandler(logging.Handler):
    """Looks like OTel's LoggingHandler to the fallback check (same class name, an opentelemetry module)."""

    def emit(self, record):
        pass


LoggingHandler.__module__ = "opentelemetry.sdk._logs._internal"


def test_fallback_handler_prints_when_root_has_no_handler_or_only_otels():
    assert "CW-PLUGIN genai: hi" in _emit_through_fallback([])
    assert "CW-PLUGIN genai: hi" in _emit_through_fallback([LoggingHandler()])


@pytest.mark.filterwarnings("ignore::DeprecationWarning")  # the SDK handler is deprecated in favour of contrib's
def test_fallback_handler_recognizes_the_real_otel_logging_handler():
    sdk_logs = pytest.importorskip("opentelemetry.sdk._logs")
    assert "CW-PLUGIN genai: hi" in _emit_through_fallback([sdk_logs.LoggingHandler()])


@pytest.mark.parametrize(
    "handler",
    [
        logging.StreamHandler(io.StringIO()),
        logging.handlers.QueueHandler(queue.SimpleQueue()),
        logging.NullHandler(),
    ],
    ids=["stream", "queue", "null"],
)
def test_fallback_handler_is_silent_once_the_app_has_any_handler(handler):
    assert _emit_through_fallback([handler]) == ""
    assert _emit_through_fallback([LoggingHandler(), handler]) == ""


def test_configure_logging_installs_one_fallback_on_the_genai_logger_and_uses_info():
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    _settings.configure_logging()
    _settings.configure_logging()

    genai = logging.getLogger(_settings.LOGGER_NAME)
    assert sum(isinstance(h, _settings._StderrFallbackHandler) for h in genai.handlers) == 1
    assert genai.level == logging.INFO


def _parent_state():
    parent = logging.getLogger(_settings.PLUGIN_LOGGER_NAME)
    return parent.level, list(parent.handlers)


@pytest.mark.parametrize(
    "value,level", [("debug", logging.DEBUG), (" WARNING ", logging.WARNING), ("error", logging.ERROR)]
)
def test_otel_python_log_level_applies_to_the_genai_logger_only(genai_installed, monkeypatch, value, level):
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", value)
    before = _parent_state()

    _settings.genai_status()

    assert _settings.get_logger().level == level
    assert _parent_state() == before


def _stderr_output(monkeypatch):
    """Routes stderr (fallback handler and logging.lastResort) to a buffer, with no handlers configured."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    monkeypatch.setattr(logging, "lastResort", logging._StderrHandler(logging.WARNING))  # binds the patched stderr
    monkeypatch.setattr(logging.getLogger(), "handlers", [])
    for name in (_settings.PLUGIN_LOGGER_NAME, _settings.LOGGER_NAME):
        logging.getLogger(name).setLevel(logging.NOTSET)
        logging.getLogger(name).handlers[:] = []
    return stream


@pytest.mark.parametrize(
    "state,why",
    [("active", "GenAI layer active: all conditions met"), ("active", "CW-PLUGIN genai: active;")],
)
def test_debug_log_level_shows_why_the_layer_is_active(monkeypatch, state, why):
    stream = _stderr_output(monkeypatch)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "debug")
    monkeypatch.setattr(_settings, "genai_distribution_installed", lambda: True)
    monkeypatch.setattr(_settings, "langchain_core_present", lambda: True)
    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [])  # only the status and the summary matter here
    monkeypatch.setattr(_auto_instrumentation, "_POST_STEPS", [])

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()
    assert why in stream.getvalue()


def test_debug_log_level_changes_nothing_when_the_layer_is_off(genai_not_installed, monkeypatch):
    stream = _stderr_output(monkeypatch)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "debug")
    before = _parent_state()

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()

    assert _parent_state() == before and stream.getvalue() == ""
    assert logging.getLogger(_settings.LOGGER_NAME).handlers == []


def test_error_log_level_silences_genai_warnings_but_not_hook_warnings(genai_installed, monkeypatch):
    # pylint: disable=import-outside-toplevel
    import cloudwatch_plugin_otel_hooks as shim

    stream = _stderr_output(monkeypatch)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "error")
    monkeypatch.setattr(shim, "_genai_possible", lambda: True)

    def boom():
        raise RuntimeError("bug")

    _settings.genai_status()
    _settings.warn_once("k", "a GenAI warning")
    assert stream.getvalue() == ""

    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [("test.boom", boom)])
    shim.pre_instrument()  # the step runner's WARNING
    monkeypatch.setattr(_auto_instrumentation, "post_instrument", boom)
    shim.post_instrument()  # the shim's WARNING
    assert "CW-PLUGIN: test.boom setup failed" in stream.getvalue()
    assert "CW-PLUGIN: post_instrument failed" in stream.getvalue()


def test_logger_level_set_by_the_app_is_kept():
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.ERROR)
    _settings.configure_logging()
    assert _settings.get_logger().level == logging.ERROR


def _logging_state():
    loggers = [logging.getLogger(name) for name in (_settings.PLUGIN_LOGGER_NAME, _settings.LOGGER_NAME)]
    loggers += [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith("plugins.")]
    return {logger.name: (logger.level, list(logger.handlers)) for logger in loggers}


def test_inactive_layer_changes_no_logging_configuration(genai_not_installed, monkeypatch):
    for logger in (logging.getLogger(_settings.PLUGIN_LOGGER_NAME), logging.getLogger(_settings.LOGGER_NAME)):
        logger.setLevel(logging.NOTSET)
        logger.handlers[:] = []
    monkeypatch.setenv(OLD, "true")
    monkeypatch.setenv(_settings.CW_PLUGIN_GENAI_ENABLED, "maybe")  # a warning while computing the status
    before = _logging_state()

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()

    assert _logging_state() == before


def test_active_layer_configures_the_genai_logger_only(genai_installed):
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    logging.getLogger(_settings.LOGGER_NAME).handlers[:] = []
    before = _parent_state()

    assert _settings.genai_active() is True
    genai = logging.getLogger(_settings.LOGGER_NAME)
    assert genai.level == logging.INFO
    assert any(isinstance(h, _settings._StderrFallbackHandler) for h in genai.handlers)
    assert _parent_state() == before


def test_warn_once_emits_each_key_once(caplog):
    _settings.warn_once("k", "first %s", 1)
    _settings.warn_once("k", "again")
    _settings.warn_once("other", "other")

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, "CW-PLUGIN genai: first 1"),
        (logging.WARNING, "CW-PLUGIN genai: other"),
    ]
    assert all(r.name == _settings.LOGGER_NAME for r in caplog.records)
