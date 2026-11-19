# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Targets of the plugin's pre/post instrument entry points. Standard library only, and outside ``plugins``,
because an app's own ``plugins/`` can shadow that namespace package, ``plugins.opentelemetry.cloudwatch`` imports
the SDK eagerly, and the loader does not guard hook calls (a raise would abort all auto-instrumentation). Never
raises an ``Exception``; ``BaseException`` propagates."""

import importlib
import importlib.metadata
import logging
import os
import sys
import traceback

_IMPLEMENTATION = "plugins.opentelemetry.cloudwatch._auto_instrumentation"
CW_PLUGIN_GENAI_ENABLED = "CW_PLUGIN_GENAI_ENABLED"  # kill switch for every GenAI feature
# The GenAI layer can be active only when one of these distributions is installed (later releases add more).
SUPPORTED_GENAI_DISTRIBUTIONS = ("opentelemetry-instrumentation-genai-langchain",)
_LOGGER_NAME = _IMPLEMENTATION.rpartition(".")[0]
_shadow_reported = False


def is_otel_logging_handler(handler: logging.Handler) -> bool:
    """OTel's ``LoggingHandler`` (SDK or opentelemetry-instrumentation-logging), by class name in the hierarchy,
    so neither has to be imported."""
    return any(
        c.__name__ == "LoggingHandler" and c.__module__.startswith("opentelemetry.") for c in type(handler).__mro__
    )


def _shadowing_plugins(exc: Exception):
    """Where a foreign ``plugins`` package hiding ours lives, or None if ``exc`` has another cause. Ours is any
    ``plugins.__path__`` entry holding the implementation (so editable installs count as ours)."""
    if not isinstance(exc, ImportError) or (exc.name or "").split(".")[0] != "plugins":
        return None
    paths = list(getattr(sys.modules.get("plugins"), "__path__", []))
    if any(os.path.isfile(os.path.join(p, "opentelemetry", "cloudwatch", "_auto_instrumentation.py")) for p in paths):
        return None
    return getattr(sys.modules.get("plugins"), "__file__", None) or ", ".join(paths) or "unknown location"


def _warn_shadowed(logger: logging.Logger, shadow: str, exc: Exception) -> None:
    msg = (
        f"CW-PLUGIN: a `plugins` package earlier on sys.path ({shadow}) shadows cloudwatch-plugin-otel's; the plugin "
        "(span metrics and GenAI) is inactive. Fix: rename your `plugins` package or move it off sys.path"
    )
    logger.warning(msg)  # logging.lastResort prints it when no handler is configured at all
    logger.debug("import error", exc_info=exc)
    root = logging.getLogger().handlers
    # ...but not when the root logger has only OTel's LoggingHandler (ADOT agent mode): print it ourselves.
    if root and logger.isEnabledFor(logging.WARNING) and all(is_otel_logging_handler(h) for h in root):
        print(msg, file=sys.stderr)
        if logger.isEnabledFor(logging.DEBUG):
            traceback.print_exception(exc, file=sys.stderr)


def _genai_possible() -> bool:
    """Cheap gate, so processes without GenAI never import ``plugins.*`` from the hooks (the hooks only do GenAI
    work today). ``_settings`` decides the rest, including warning about an invalid kill-switch value."""
    if os.environ.get(CW_PLUGIN_GENAI_ENABLED, "").strip().lower() == "false":
        return False
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    for name in SUPPORTED_GENAI_DISTRIBUTIONS:
        try:
            importlib.metadata.distribution(name)
            return True
        except importlib.metadata.PackageNotFoundError:
            continue
    return False


def _call(hook_name: str) -> None:
    global _shadow_reported  # pylint: disable=global-statement
    logger = logging.getLogger(_LOGGER_NAME)
    try:
        if not _genai_possible():  # inside the try: metadata finders can raise
            return
        hook = getattr(importlib.import_module(_IMPLEMENTATION), hook_name)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        try:
            shadow = _shadowing_plugins(exc)
            if shadow is not None and not _shadow_reported:
                _shadow_reported = True
                _warn_shadowed(logger, shadow, exc)
            else:
                logger.debug("Could not import %s.%s; hooks skipped", _IMPLEMENTATION, hook_name, exc_info=True)
        except Exception:  # pylint: disable=broad-exception-caught
            pass  # reporting must never break the loader
        return
    try:
        hook()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("CW-PLUGIN: %s failed; continuing without it", hook_name, exc_info=True)


def pre_instrument() -> None:
    _call("pre_instrument")


def post_instrument() -> None:
    _call("post_instrument")
