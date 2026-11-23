# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Honor ``OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true`` for util-genai instrumentations, without
writing the environment (a pre step). The plugin never turns content capture on by itself: unset stays
util-genai's NO_CONTENT.

Why ``true``: the AgentCore documentation (and ADOT) tell users to set ``true``, and the botocore and older
openai-v2 instrumentations accept only ``true``. util-genai accepts only the mode names and reads ``true`` as
NO_CONTENT, so without this, users who follow those docs silently get no util-genai content and AgentCore
Evaluations cannot score their traces.

``true`` or ``SPAN_ONLY``: ``true`` makes botocore, openai-v2 and (through this module) util-genai instrumentations
capture content on spans; ``SPAN_ONLY`` works for util-genai instrumentations only and turns botocore content off.
Both are fine for AgentCore Evaluations.

Limits: ``true`` is not translated while ``OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT`` is set, so that name wins
once util-genai reads it. Code that imported util-genai's resolver by name before the hook keeps the original.
Installs without ``.py`` sources cannot be scanned, so nothing is installed there (a warning if ``true`` is set).
When it retires: see the workaround marker on ``_resolve``.
"""

import ast
import dis
import functools
import importlib
import importlib.util
import inspect
import logging
import os
from typing import Any, Dict, Optional, Set, Tuple

from plugins.opentelemetry.cloudwatch.genai._settings import distribution_version, get_logger, warn_once

CAPTURE_MESSAGE_CONTENT = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"  # what util-genai 1.3b0 reads
CAPTURE_CONTENT = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT"  # semantic-conventions-genai#543; not read by 1.3b0
VALID_MODES = ("NO_CONTENT", "SPAN_ONLY", "EVENT_ONLY", "SPAN_AND_EVENT")
_RESOLVER = "get_content_capturing_mode"
_UTILS = "opentelemetry.util.genai.utils"
# Every util-genai module that binds the resolver by name (util-genai 1.3b0): its definition, the handler and the
# invocations (which resolve it themselves when created without a handler).
_SITES = (_UTILS, "opentelemetry.util.genai.handler", "opentelemetry.util.genai._invocation")

_patch: Dict[str, Any] = {}  # what the installed patch replaced; empty when none is installed
_patch_skipped: Dict[str, bool] = {}  # why nothing was installed, when util-genai no longer reads the old name


def _value() -> str:
    return os.environ.get(CAPTURE_MESSAGE_CONTENT, "").strip()


def _translates() -> bool:
    """`true` in the old name, and the new name unset (should util-genai read it, it must win)."""
    return _value().lower() == "true" and not os.environ.get(CAPTURE_CONTENT, "").strip()


# CW-PLUGIN-WORKAROUND(retire-when: util-genai resolves `true` itself (the patch then defers
# automatically) or stops reading OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT (the patch then installs
# nothing); or contrib botocore/openai-v2 accept the mode names and the AgentCore docs and ADOT's agent-mode
# default switch to SPAN_ONLY)
def _resolve(original, mode_enum) -> Tuple[Any, bool]:
    """``(mode, translated)``: util-genai's own result, except SPAN_ONLY where it read `true` as NO_CONTENT (like
    semantic-conventions-genai#543, util-genai accepts only the mode names). The original is always called first,
    so a util-genai that resolves `true` itself makes this a no-op."""
    mode = original()
    if mode is mode_enum.NO_CONTENT and _translates():
        return mode_enum.SPAN_ONLY, True
    return mode, False


class _DropTranslatedTrueWarning(logging.Filter):
    """Drops util-genai's "true is not a valid option ... Defaulting to NO_CONTENT" while the plugin translates it."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args if isinstance(record.args, tuple) else ()
        translated = len(args) > 1 and str(args[0]).strip().lower() == "true" and args[1] == CAPTURE_MESSAGE_CONTENT
        return not (translated and _translates())


def _binding_sites() -> Set[str]:
    """Every util-genai module that defines or imports the resolver by name, found by parsing the installed
    sources (nothing is imported)."""
    spec = importlib.util.find_spec("opentelemetry.util.genai")
    found = set()
    for root in (spec.submodule_search_locations or []) if spec else []:
        for directory, _, files in os.walk(root):
            for file in (f for f in files if f.endswith(".py")):
                path = os.path.join(directory, file)
                with open(path, encoding="utf-8") as source:
                    tree = ast.parse(source.read(), path)
                if any(
                    (isinstance(node, ast.FunctionDef) and node.name == _RESOLVER)
                    or (isinstance(node, ast.ImportFrom) and any(alias.name == _RESOLVER for alias in node.names))
                    for node in ast.walk(tree)
                ):
                    parts = os.path.relpath(path[: -len(".py")], root).split(os.sep)
                    found.add(".".join(["opentelemetry.util.genai", *parts[: -1 if parts[-1] == "__init__" else None]]))
    return found


def _names(func) -> Set[str]:
    return {i.argval for i in dis.get_instructions(func) if isinstance(i.argval, str)}


def _sites():
    """``(modules, original resolver, ContentCapturingMode)``, or None unless the sites are exactly the known ones
    and every site has the expected shape."""
    try:
        if _binding_sites() != set(_SITES):
            return None
        modules = [importlib.import_module(name) for name in _SITES]
        original, mode_enum = getattr(modules[0], _RESOLVER), modules[0].ContentCapturingMode
        # It must still read the old name: never revive `true` under a name util-genai stopped reading.
        if not inspect.signature(original).parameters and CAPTURE_MESSAGE_CONTENT not in _names(original):
            _patch_skipped["old name not read"] = True
            return None
        ok = (
            not inspect.signature(original).parameters
            and all(getattr(module, _RESOLVER, None) is original for module in modules)
            # The handler must call the module-level name replaced here (LOAD_GLOBAL on 3.10 through 3.14).
            and any(
                i.opname == "LOAD_GLOBAL" and i.argval == _RESOLVER
                for i in dis.get_instructions(modules[1].TelemetryHandler.__init__)
            )
            and all(mode in mode_enum.__members__ for mode in VALID_MODES)
        )
    except Exception:  # pylint: disable=broad-exception-caught
        get_logger().debug("util-genai patch targets not found", exc_info=True)
        return None
    return (modules, original, mode_enum) if ok else None


def _install() -> Optional[str]:
    """Installs the patch at every site, or at none. Returns None, or util-genai's version when its shape is
    unexpected and nothing was installed."""
    if _patch:
        return None
    sites = _sites()
    if sites is None:
        return distribution_version("opentelemetry-util-genai") or "(unknown version)"
    modules, original, mode_enum = sites

    @functools.wraps(original)
    def get_content_capturing_mode():
        return _resolve(original, mode_enum)[0]

    log_filter = _DropTranslatedTrueWarning()
    logging.getLogger(_UTILS).addFilter(log_filter)
    _patch.update(modules=modules, original=original, mode_enum=mode_enum, filter=log_filter)
    for module in modules:
        setattr(module, _RESOLVER, get_content_capturing_mode)
    return None


def _unpatch() -> None:
    """Restores util-genai. For tests."""
    if _patch:
        for module in _patch["modules"]:
            setattr(module, _RESOLVER, _patch["original"])
        logging.getLogger(_UTILS).removeFilter(_patch["filter"])
        _patch.clear()
    _patch_skipped.clear()


def content_capture_step() -> Optional[str]:
    """The ``content capture`` pre step: the summary's content part, or None when util-genai is not importable."""
    if importlib.util.find_spec("opentelemetry.util.genai") is None:
        return None
    value, unavailable = _value(), _install()
    if unavailable is not None and _patch_skipped:
        if value:
            msg = "util-genai %s no longer reads %s, so %s=%s is not translated. Fix: set the content-capture mode "
            old = CAPTURE_MESSAGE_CONTENT
            warn_once("unavailable", msg + "util-genai documents", unavailable, old, old, value)
        return "unknown (translation unavailable)"
    if unavailable is not None and value.lower() == "true":
        msg = "%s=true means NO_CONTENT for util-genai %s (the plugin's translation is unavailable). "
        warn_once(
            "unavailable", msg + "Fix: %s=SPAN_ONLY", CAPTURE_MESSAGE_CONTENT, unavailable, CAPTURE_MESSAGE_CONTENT
        )
        return "NO_CONTENT (translation unavailable)"
    if not value:
        return "NO_CONTENT (unset)"
    if value.lower() == "true":  # the patch is installed here
        mode, translated = _resolve(_patch["original"], _patch["mode_enum"])
        return mode.name + (" (translated from true)" if translated else "")
    return value.upper() if value.upper() in VALID_MODES else "NO_CONTENT"  # util-genai warns about other values
