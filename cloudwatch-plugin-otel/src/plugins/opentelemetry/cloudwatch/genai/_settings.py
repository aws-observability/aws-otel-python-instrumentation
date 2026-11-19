# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Switches, gating and logging for the plugin's GenAI layer. Nothing here affects span metrics."""

import importlib.metadata
import importlib.util
import logging
import os
import sys
from typing import Optional, Set, Tuple

# The shim owns the kill switch name and the supported distributions: it gates on them before importing `plugins`.
from cloudwatch_plugin_otel_hooks import CW_PLUGIN_GENAI_ENABLED, SUPPORTED_GENAI_DISTRIBUTIONS, is_otel_logging_handler

from opentelemetry.instrumentation.environment_variables import OTEL_PYTHON_DISABLED_INSTRUMENTATIONS

PLUGIN_LOGGER_NAME = "plugins.opentelemetry.cloudwatch"
LOGGER_NAME = PLUGIN_LOGGER_NAME + ".genai"
# GenAI lines start with this; the hook shim and step runner use "CW-PLUGIN: ".
LOG_PREFIX = "CW-PLUGIN genai: "
_LOG_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}


def env_flag(name: str, default: bool = True) -> bool:
    """``true``/``false`` (stripped, any case); unset or empty means ``default``; else ``default`` + a WARNING."""
    value = os.environ.get(name, "").strip().lower()
    if value in ("true", "false"):
        return value == "true"
    if value:
        warn_once("switch:" + name, "%s=%r is not true or false; using %s", name, os.environ[name], default)
    return default


def sdk_disabled() -> bool:
    return os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true"


def instrumentation_disabled(entry_point_name: str) -> bool:
    """Whether the loader skips ``entry_point_name``: its list is split on "," and stripped, and "*" (the loader's
    SKIPPED_INSTRUMENTATIONS_WILDCARD) disables every instrumentor."""
    disabled = [name.strip() for name in os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "").split(",")]
    return entry_point_name in disabled or "*" in disabled


def adot_agent_mode() -> bool:
    """ADOT's agent-observability flag, parsed as ADOT 0.23.0 does (_utils.py:75-77: ``.lower() == "true"``)."""
    return os.environ.get("AGENT_OBSERVABILITY_ENABLED", "false").lower() == "true"


def adot_genai_mode() -> Tuple[str, str]:
    """``(variable, mode)`` of ADOT's GenAI instrumentation switch, parsed as ADOT 0.23.0 does
    (aws_opentelemetry_distro.py:279-290): AWS_GENAI_INSTRUMENTATION, else the legacy AWS_AGENTIC_INSTRUMENTATION,
    lower-cased; anything but enabled/disabled means auto."""
    name = "AWS_GENAI_INSTRUMENTATION"
    if name not in os.environ and "AWS_AGENTIC_INSTRUMENTATION" in os.environ:
        name = "AWS_AGENTIC_INSTRUMENTATION"
    mode = os.environ.get(name, "auto").lower()
    return name, (mode if mode in ("enabled", "disabled") else "auto")


def distribution_version(name: str) -> Optional[str]:
    """The installed version of distribution ``name``, from its metadata (nothing is imported), or None."""
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def genai_distribution_installed() -> bool:
    return any(distribution_version(name) is not None for name in SUPPORTED_GENAI_DISTRIBUTIONS)


_status: Optional[str] = None


def _compute_status() -> Tuple[str, str]:
    """``(status, reason)``."""
    if sdk_disabled():
        return "off", "OTEL_SDK_DISABLED=true"
    if not env_flag(CW_PLUGIN_GENAI_ENABLED):
        return "off", CW_PLUGIN_GENAI_ENABLED + "=false"
    if not genai_distribution_installed():
        return "off", "none of " + ", ".join(SUPPORTED_GENAI_DISTRIBUTIONS) + " is installed"
    if not langchain_core_present():
        return "off", "langchain_core is not importable"
    return "active", "all conditions met"


def genai_status() -> str:
    """``active`` or ``off``, computed once. Logging is configured (on the GenAI logger only) only when active."""
    global _status  # pylint: disable=global-statement
    if _status is None:
        _status, reason = _compute_status()
        if _status == "active":
            configure_logging()  # first, so OTEL_PYTHON_LOG_LEVEL=debug shows the reason below
        get_logger().debug("GenAI layer %s: %s", _status, reason)
    return _status


def genai_active() -> bool:
    return genai_status() == "active"


def langchain_core_present() -> bool:
    """Found without importing it."""
    try:
        return importlib.util.find_spec("langchain_core") is not None
    except (ImportError, ValueError):
        return False


def dist_name(entry_point) -> str:
    """The distribution name of an entry point, or "unknown"."""
    dist = getattr(entry_point, "dist", None)
    return (dist.metadata.get("Name") if dist is not None else None) or "unknown"


class _StderrFallbackHandler(logging.StreamHandler):
    """Prints plugin records to stderr unless the root logger has a handler other than OTel's ``LoggingHandler``,
    so the lines stay visible when only OTLP logging is set up, and nothing prints twice otherwise."""

    def emit(self, record: logging.LogRecord) -> None:
        if not any(not is_otel_logging_handler(handler) for handler in logging.getLogger().handlers):
            super().emit(record)


def env_log_level() -> Optional[int]:
    return _LOG_LEVELS.get(os.environ.get("OTEL_PYTHON_LOG_LEVEL", "").strip().lower())


def configure_logging() -> None:
    """Idempotent; only when the layer is not off, and only the GenAI logger: sets its level
    (OTEL_PYTHON_LOG_LEVEL, else INFO) unless the app set one, and adds the stderr fallback handler."""
    genai_logger = get_logger()
    if genai_logger.level == logging.NOTSET:
        genai_logger.setLevel(env_log_level() or logging.INFO)
    if not any(isinstance(handler, _StderrFallbackHandler) for handler in genai_logger.handlers):
        handler = _StderrFallbackHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        genai_logger.addHandler(handler)


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


_warned: Set[str] = set()


def warn_once(key: str, msg: str, *args) -> None:
    """Logs ``LOG_PREFIX + msg`` at WARNING the first time ``key`` is seen in this process."""
    if key not in _warned:
        _warned.add(key)
        get_logger().warning(LOG_PREFIX + msg, *args)
