# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The plugin's ``opentelemetry_pre_instrument`` / ``opentelemetry_post_instrument`` hooks (via the
``cloudwatch_plugin_otel_hooks`` shim).

They run only under auto-instrumentation (``opentelemetry-instrument`` or ``auto_instrumentation.initialize()``):
after the distro and configurator, pre hooks run before every ``opentelemetry_instrumentor`` entry point and post
hooks after them. The loader does not guard hook calls, so every step is isolated by ``_run``.

A pre step that sets env vars, or changes how instrumentors resolve them, must run before any ``instrument()``:
util-genai resolves its content-capture mode in ``TelemetryHandler.__init__`` (built by a GenAI instrumentor's
``_instrument()``), and the semantic-convention opt-in is fixed by the first ``instrument()`` call.
"""

import importlib
import logging
from typing import Any, Dict, Set

from plugins.opentelemetry.cloudwatch.genai import _settings

_GENAI = "plugins.opentelemetry.cloudwatch.genai."
_LOGGER = logging.getLogger(_settings.PLUGIN_LOGGER_NAME)


def _lazy(module: str, function: str):
    """A GenAI step that imports ``module`` only when it runs (isolating import errors), and not at all when the
    layer is off: processes without GenAI import ``_settings`` only, never the step modules or util-genai."""

    def step():
        if _settings.genai_status() == "off":
            return None
        return getattr(importlib.import_module(_GENAI + module), function)()

    return step


_PRE_STEPS = [("genai.content_capture", _lazy("content_capture", "content_capture_step"))]
_POST_STEPS = [("genai.langchain_activation", _lazy("langchain", "langchain_activation_step"))]

_done: Set[str] = set()  # hooks that already ran in this process
_results: Dict[str, Any] = {}  # step name -> return value, for the summary


def _run(name: str, func) -> None:
    try:
        _results[name] = func()
    except Exception:  # pylint: disable=broad-exception-caught
        _LOGGER.warning("CW-PLUGIN: %s setup failed; continuing without it", name, exc_info=True)


def _run_once(hook: str, steps) -> bool:
    if hook in _done:
        _LOGGER.debug("%s already ran in this process; skipping", hook)
        return False
    _done.add(hook)
    for name, func in steps:
        _run(name, func)
    return True


def pre_instrument() -> None:
    _run_once("pre_instrument", _PRE_STEPS)


def post_instrument() -> None:
    if _run_once("post_instrument", _POST_STEPS):
        _run("genai.summary", _log_genai_summary)


def _log_genai_summary() -> None:
    """The GenAI layer's one INFO line, when it is active."""
    if not _settings.genai_active():
        return
    # pylint: disable=import-outside-toplevel
    from plugins.opentelemetry.cloudwatch.genai import content_capture, langchain

    others = [dist for dist, _ in langchain.others_active()]
    content = content_capture.summary(_results.get("genai.content_capture"), langchain.upstream_active(), others)
    outcome = _results.get("genai.langchain_activation") or "failed"
    _settings.get_logger().info("CW-PLUGIN genai: active; content capture=%s; langchain=%s", content, outcome)
