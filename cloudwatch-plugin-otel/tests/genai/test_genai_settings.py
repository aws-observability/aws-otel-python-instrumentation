# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The kill switch, the disabled-instrumentations list, and the GenAI logging rules."""
import io
import logging
import logging.handlers
import queue
import sys

import pytest
from genai_helpers import messages
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import _settings

KILL_SWITCH = _settings.CW_PLUGIN_GENAI_ENABLED


# Switches ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", "   "])
def test_kill_switch_defaults_to_enabled(monkeypatch, caplog, value):
    if value is not None:
        monkeypatch.setenv(KILL_SWITCH, value)
    assert _settings.genai_enabled() is True
    assert messages(caplog, logging.WARNING) == []


@pytest.mark.parametrize(
    "value,expected",
    [("true", True), ("TRUE", True), (" True ", True), ("false", False), ("False", False), ("\tFALSE\n", False)],
)
def test_kill_switch_values_are_stripped_and_case_insensitive(monkeypatch, caplog, value, expected):
    monkeypatch.setenv(KILL_SWITCH, value)
    assert _settings.genai_enabled() is expected
    assert messages(caplog, logging.WARNING) == []


@pytest.mark.parametrize("value", ["1", "0", "yes", "no", "on", "off", "enabled"])
def test_invalid_kill_switch_means_enabled_with_one_warning(monkeypatch, caplog, value):
    monkeypatch.setenv(KILL_SWITCH, value)

    assert _settings.genai_enabled() is True
    assert _settings.genai_enabled() is True

    warnings = messages(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert warnings[0].startswith("CW-PLUGIN genai: ")
    assert KILL_SWITCH in warnings[0] and repr(value) in warnings[0]
    assert "is not true or false" in warnings[0]


@pytest.mark.parametrize("value,enabled", [("true", False), (" TRUE ", False), ("false", True), ("", True)])
def test_sdk_disabled_disables_genai(monkeypatch, value, enabled):
    monkeypatch.setenv("OTEL_SDK_DISABLED", value)
    assert _settings.genai_enabled() is enabled


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


def test_get_logger_installs_one_fallback_on_the_genai_logger_and_uses_info():
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    _settings.get_logger()
    _settings.get_logger()

    genai = logging.getLogger(_settings.LOGGER_NAME)
    assert sum(isinstance(h, _settings._StderrFallbackHandler) for h in genai.handlers) == 1
    assert genai.level == logging.INFO


def _parent_state():
    parent = logging.getLogger(_settings.PLUGIN_LOGGER_NAME)
    return parent.level, list(parent.handlers)


@pytest.mark.parametrize(
    "value,level", [("debug", logging.DEBUG), (" WARNING ", logging.WARNING), ("error", logging.ERROR)]
)
def test_otel_python_log_level_applies_to_the_genai_logger_only(monkeypatch, value, level):
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", value)
    before = _parent_state()

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


def test_debug_log_level_shows_genai_debug_lines(monkeypatch):
    stream = _stderr_output(monkeypatch)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "debug")
    _settings.get_logger().debug("a GenAI debug line")
    assert "a GenAI debug line" in stream.getvalue()


def test_summary_line_reaches_stderr_without_logging_configuration(monkeypatch):
    stream = _stderr_output(monkeypatch)
    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [("test.step", lambda: "done")])
    monkeypatch.setattr(_auto_instrumentation, "_POST_STEPS", [])

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()
    assert "CW-PLUGIN genai: test.step=done" in stream.getvalue()


def test_error_log_level_silences_genai_warnings_but_not_hook_warnings(monkeypatch):
    # pylint: disable=import-outside-toplevel
    import cloudwatch_plugin_otel_hooks as shim

    stream = _stderr_output(monkeypatch)
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "error")

    def boom():
        raise RuntimeError("bug")

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
    assert _settings.get_logger().level == logging.ERROR


def _logging_state():
    loggers = [logging.getLogger(name) for name in (_settings.PLUGIN_LOGGER_NAME, _settings.LOGGER_NAME)]
    loggers += [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith("plugins.")]
    return {logger.name: (logger.level, list(logger.handlers)) for logger in loggers}


def test_hooks_with_nothing_to_say_change_no_logging_configuration(monkeypatch):
    for logger in (logging.getLogger(_settings.PLUGIN_LOGGER_NAME), logging.getLogger(_settings.LOGGER_NAME)):
        logger.setLevel(logging.NOTSET)
        logger.handlers[:] = []
    monkeypatch.setattr(_auto_instrumentation, "_PRE_STEPS", [("test.silent", lambda: None)])
    monkeypatch.setattr(_auto_instrumentation, "_POST_STEPS", [("test.silent2", lambda: None)])
    before = _logging_state()

    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()

    assert _logging_state() == before


def test_get_logger_configures_the_genai_logger_only():
    logging.getLogger(_settings.LOGGER_NAME).setLevel(logging.NOTSET)
    logging.getLogger(_settings.LOGGER_NAME).handlers[:] = []
    before = _parent_state()

    genai = _settings.get_logger()
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


def test_debug_once_has_its_own_keys(caplog):
    caplog.set_level(logging.DEBUG, logger=_settings.LOGGER_NAME)
    _settings.warn_once("k", "a warning")
    _settings.debug_once("k", "a debug line")
    _settings.debug_once("k", "again")

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, "CW-PLUGIN genai: a warning"),
        (logging.DEBUG, "CW-PLUGIN genai: a debug line"),
    ]
