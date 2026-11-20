# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The GenAI kill switch and logging, shared by every GenAI integration. Nothing here affects span metrics."""

import importlib.metadata
import logging
import os
import sys
from typing import Optional, Set

# The shim owns the kill switch name: it checks it before importing `plugins`.
from cloudwatch_plugin_otel_hooks import CW_PLUGIN_GENAI_ENABLED, is_otel_logging_handler

from opentelemetry.instrumentation.environment_variables import OTEL_PYTHON_DISABLED_INSTRUMENTATIONS

PLUGIN_LOGGER_NAME = "plugins.opentelemetry.cloudwatch"
LOGGER_NAME = PLUGIN_LOGGER_NAME + ".genai"
# GenAI lines start with this; the hook shim and step runner use "CW-PLUGIN: ".
LOG_PREFIX = "CW-PLUGIN genai: "
_LOG_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}


def genai_enabled() -> bool:
    """False with ``OTEL_SDK_DISABLED=true`` or ``CW_PLUGIN_GENAI_ENABLED=false`` (both stripped, any case). The
    kill switch defaults to true; any value other than true/false keeps the default and logs one WARNING."""
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    raw = os.environ.get(CW_PLUGIN_GENAI_ENABLED, "")
    value = raw.strip().lower()
    if value not in ("", "true", "false"):
        warn_once("switch", "%s=%r is not true or false; using true", CW_PLUGIN_GENAI_ENABLED, raw)
    return value != "false"


def instrumentation_disabled(entry_point_name: str) -> bool:
    """Whether the loader skips ``entry_point_name``: its list is split on "," and stripped, and "*" (the loader's
    SKIPPED_INSTRUMENTATIONS_WILDCARD) disables every instrumentor."""
    disabled = [name.strip() for name in os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "").split(",")]
    return entry_point_name in disabled or "*" in disabled


def distribution_version(name: str) -> Optional[str]:
    """The installed version of distribution ``name``, from its metadata (nothing is imported), or None."""
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


class _StderrFallbackHandler(logging.StreamHandler):
    """Prints plugin records to stderr unless the root logger has a handler other than OTel's ``LoggingHandler``,
    so the lines stay visible when only OTLP logging is set up, and nothing prints twice otherwise."""

    def emit(self, record: logging.LogRecord) -> None:
        if not any(not is_otel_logging_handler(handler) for handler in logging.getLogger().handlers):
            super().emit(record)


def env_log_level() -> Optional[int]:
    return _LOG_LEVELS.get(os.environ.get("OTEL_PYTHON_LOG_LEVEL", "").strip().lower())


def get_logger() -> logging.Logger:
    """The GenAI logger, configured on first use, so a process where no GenAI step has anything to say sees no
    logging change. Only this logger is touched: its level (OTEL_PYTHON_LOG_LEVEL, else INFO) unless the app set
    one, and the stderr fallback handler. Idempotent."""
    genai_logger = logging.getLogger(LOGGER_NAME)
    if genai_logger.level == logging.NOTSET:
        genai_logger.setLevel(env_log_level() or logging.INFO)
    if not any(isinstance(handler, _StderrFallbackHandler) for handler in genai_logger.handlers):
        handler = _StderrFallbackHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        genai_logger.addHandler(handler)
    return genai_logger


_warned: Set[str] = set()  # keys already logged by warn_once
_debugged: Set[str] = set()  # keys already logged by debug_once


def warn_once(key: str, msg: str, *args) -> None:
    """Logs ``LOG_PREFIX + msg`` at WARNING the first time ``key`` is seen in this process."""
    if key not in _warned:
        _warned.add(key)
        get_logger().warning(LOG_PREFIX + msg, *args)


def debug_once(key: str, msg: str, *args) -> None:
    """Logs ``LOG_PREFIX + msg`` at DEBUG the first time ``key`` is seen in this process."""
    if key not in _debugged:
        _debugged.add(key)
        get_logger().debug(LOG_PREFIX + msg, *args)
