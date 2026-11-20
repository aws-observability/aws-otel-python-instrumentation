# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Activate the upstream LangChain instrumentor when only ``langchain-core`` is installed (a post step).

The instrumentor declares ``langchain >= 0.3.22, < 2`` but needs only ``langchain_core``, so the loader skips it in
apps that do not install ``langchain``, typically LangGraph apps (open-telemetry/opentelemetry-python-genai#915).
The plugin bypasses the dependency check in exactly that case. When ``langchain`` is installed, the loader's
decision stands.
"""

import importlib
import importlib.util
import sys
from typing import Optional

from plugins.opentelemetry.cloudwatch.genai._settings import distribution_version, instrumentation_disabled, warn_once

ENTRY_POINT_NAME = "langchain"
UPSTREAM_DIST = "opentelemetry-instrumentation-genai-langchain"
UPSTREAM_MODULE = "opentelemetry.instrumentation.genai.langchain"

ACTIVATED = "activated (langchain-core only)"
INSTRUMENTED = "instrumented"
NOT_INSTALLED = "not installed"
DISABLED = "disabled"
FAILED = "failed"


def _langchain_core_present() -> bool:
    """Found without importing it."""
    try:
        return importlib.util.find_spec("langchain_core") is not None
    except (ImportError, ValueError):
        return False


def _upstream_instrumented() -> bool:
    """Read from the instrumentor singleton without importing the module (if the loader never imported it, it is
    not instrumenting) or calling the class (that would re-run ``__init__`` on it)."""
    cls = getattr(sys.modules.get(UPSTREAM_MODULE), "LangChainInstrumentor", None)
    instance = getattr(cls, "_instance", None)
    return instance is not None and instance.is_instrumented_by_opentelemetry


def langchain_step() -> Optional[str]:
    """The ``langchain`` post step: the summary outcome, or None when it does not apply."""
    if not _langchain_core_present():
        return None
    if distribution_version(UPSTREAM_DIST) is None:
        warn_once(
            "upstream",
            "langchain-core is installed but %s is not; install it to get LangChain/LangGraph spans",
            UPSTREAM_DIST,
        )
        return NOT_INSTALLED
    if _upstream_instrumented():
        return INSTRUMENTED
    if instrumentation_disabled(ENTRY_POINT_NAME):
        return DISABLED
    if distribution_version("langchain") is not None:
        return None  # the loader's dependency check decided
    return _activate()


# CW-PLUGIN-WORKAROUND(expires: 2027-03-31; retire-when: open-telemetry/opentelemetry-python-genai#915 is fixed
# (PR #916) so the dependency check accepts langchain-core alone)
def _activate() -> str:
    """``instrument(skip_dep_check=True)`` with the global providers, as the loader does. Never raises."""
    try:
        importlib.import_module(UPSTREAM_MODULE).LangChainInstrumentor().instrument(skip_dep_check=True)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        warn_once("activate", "no LangChain spans: activating %s failed: %r", UPSTREAM_DIST, exc)
        return FAILED
    return ACTIVATED
