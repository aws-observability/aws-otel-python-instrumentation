# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The plugin's ``opentelemetry_pre_instrument`` / ``opentelemetry_post_instrument`` hooks (via the
``cloudwatch_plugin_otel_hooks`` shim).

They run only under auto-instrumentation (``opentelemetry-instrument`` or ``auto_instrumentation.initialize()``):
after the distro and configurator, pre hooks run before every ``opentelemetry_instrumentor`` entry point and post
hooks after them. The loader does not guard hook calls, so every step is isolated by ``_run``. Each step decides
whether it applies, and returns None when it does not.

A pre step that sets env vars, or changes how instrumentors resolve them, must run before any ``instrument()``:
util-genai resolves its content-capture mode in ``TelemetryHandler.__init__`` (built by a GenAI instrumentor's
``_instrument()``), and the semantic-convention opt-in is fixed by the first ``instrument()`` call.
"""

import importlib
import logging
from typing import Any, Dict

from plugins.opentelemetry.cloudwatch.genai import _settings

_GENAI = "plugins.opentelemetry.cloudwatch.genai."
_LOGGER = logging.getLogger(_settings.PLUGIN_LOGGER_NAME)


def _lazy(module: str, function: str):
    """A step that imports its module only when it runs, so an import error is isolated like any other."""

    def step():
        return getattr(importlib.import_module(_GENAI + module), function)()

    return step


# (name in the summary line, step)
_PRE_STEPS = [("content capture", _lazy("content_capture", "content_capture_step"))]
# After the LangChain step, which may start the instrumentor these steps depend on. The providers exist by now and
# no application span has started.
_POST_STEPS = [
    ("langchain", _lazy("langchain", "langchain_step")),
    ("session.id", _lazy("session", "session_step")),
    ("nested chat demotion", _lazy("nested_client", "nested_client_step")),
    ("agent identity", _lazy("resource", "agent_identity_step")),
]

_results: Dict[str, Any] = {}  # step name -> return value, for the summary


def _run(name: str, func) -> None:
    try:
        _results[name] = func()
    except Exception:  # pylint: disable=broad-exception-caught
        _LOGGER.warning("CW-PLUGIN: %s setup failed; continuing without it", name, exc_info=True)


def pre_instrument() -> None:
    if _settings.genai_enabled():
        for name, func in _PRE_STEPS:
            _run(name, func)


def post_instrument() -> None:
    if _settings.genai_enabled():
        for name, func in _POST_STEPS:
            _run(name, func)
        _run("summary", _log_summary)


def _log_summary() -> None:
    """One INFO line, when any step applied."""
    parts = [f"{name}={result}" for name, result in _results.items() if result is not None]
    if parts:
        _settings.get_logger().info("%s%s", _settings.LOG_PREFIX, "; ".join(parts))
