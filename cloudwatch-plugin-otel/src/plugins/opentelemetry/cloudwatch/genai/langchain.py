# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Activate the upstream LangChain instrumentor when only ``langchain-core`` is installed (a post step).

The instrumentor declares ``langchain >= 0.3.22, < 2`` but needs only ``langchain_core``, so the loader skips it in
LangGraph apps that do not install ``langchain`` (open-telemetry/opentelemetry-python-genai#915, fix in #916). The
plugin forces it only then: never when ``langchain`` is installed, when another LangChain instrumentor is active,
or when ``langchain`` is disabled.
"""

import importlib
import importlib.metadata
import sys
from typing import List, Optional, Tuple

# `packaging` comes with opentelemetry-instrumentation's declared dependencies.
from packaging.version import InvalidVersion, Version
from plugins.opentelemetry.cloudwatch.genai._settings import (
    adot_agent_mode,
    adot_genai_mode,
    dist_name,
    distribution_version,
    genai_active,
    get_logger,
    instrumentation_disabled,
    warn_once,
)

ENTRY_POINT_NAME = "langchain"
UPSTREAM_DIST = "opentelemetry-instrumentation-genai-langchain"
UPSTREAM_MODULE = "opentelemetry.instrumentation.genai.langchain"
LANGCHAIN_CORE_FLOOR = "0.3.49"  # what langchain 0.3.22, the instrumentor's floor, requires

ACTIVATED = "activated (langchain-core only; opentelemetry-python-genai#915)"
INSTRUMENTED_BY_LOADER = "instrumented by loader"
DISABLED = "disabled"
FAILED = "failed"


def _instance(module_name: str, attr: str):
    """The instrumentor singleton in an already imported module, read without calling the class (that would
    re-run ``__init__`` on it); None if the module was never imported, so nothing in it can be instrumenting."""
    module = sys.modules.get(module_name)
    return getattr(getattr(module, attr, None), "_instance", None) if module is not None else None


def upstream_active() -> bool:
    instance = _instance(UPSTREAM_MODULE, "LangChainInstrumentor")
    return instance is not None and instance.is_instrumented_by_opentelemetry


def _entry_points():
    return importlib.metadata.entry_points(group="opentelemetry_instrumentor")


def others_active() -> List[Tuple[str, str]]:
    """``(distribution, entry point name)`` of every other LangChain instrumentor that is instrumenting."""
    found = []
    for entry_point in _entry_points():
        try:
            dist = dist_name(entry_point)
            if "langchain" not in entry_point.name or dist == UPSTREAM_DIST:
                continue
            instance = _instance(entry_point.module, entry_point.attr)
            if instance is not None and instance.is_instrumented_by_opentelemetry:
                found.append((dist, entry_point.name))
        except Exception:  # pylint: disable=broad-exception-caught
            get_logger().debug("could not probe %s", entry_point, exc_info=True)
    return found


def existing_outcome(upstream: bool, others: List[Tuple[str, str]]) -> Optional[str]:
    """The outcome when LangChain must be left as it is, else None. An active instrumentor comes before the
    disabled switches, so the summary never says `disabled` while spans flow."""
    if upstream:
        return INSTRUMENTED_BY_LOADER
    if others:
        return f"skipped: {others[0][0]} already instruments LangChain"
    return DISABLED if instrumentation_disabled(ENTRY_POINT_NAME) else None


def _duplicate_fix(dist: str, name: str) -> str:
    # Reinstalling without the [langchain] extra does not remove the package, hence "and uninstall".
    drop_extra = f"drop the [langchain] extra and uninstall {UPSTREAM_DIST}"
    if name == ENTRY_POINT_NAME:  # the disabled list cannot tell two `langchain` entry points apart
        return f"{drop_extra}, or uninstall {dist}"
    if name == "aws_langchain" and adot_agent_mode():  # ADOT setdefaults the disabled list in agent mode
        variable, _ = adot_genai_mode()
        return f"unset {variable} (ADOT's auto mode then defers to the upstream instrumentor), or {drop_extra}"
    return f"add {name} to OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"


def langchain_activation_step() -> Optional[str]:
    """The ``genai.langchain_activation`` post step: the summary outcome, or None when the layer is off. The same
    everywhere, ADOT agent mode included: fill the gap only when no LangChain instrumentor is active."""
    if not genai_active():
        return None
    upstream, others = upstream_active(), others_active()
    for dist, name in others if upstream else ():
        msg = "LangChain spans are duplicated (%s and %s). Fix: %s"
        warn_once("duplicate:" + dist, msg, UPSTREAM_DIST, dist, _duplicate_fix(dist, name))
    return existing_outcome(upstream, others) or _activate()


# CW-PLUGIN-WORKAROUND(expires: 2027-03-31; retire-when: open-telemetry/opentelemetry-python-genai#915 is fixed
# (PR #916) so the dependency check accepts langchain-core alone)
def _activate() -> str:
    """``instrument(skip_dep_check=True)`` with the global providers, like the loader. Never raises."""
    core = distribution_version("langchain-core")
    try:
        too_old = core is not None and Version(core) < Version(LANGCHAIN_CORE_FLOOR)
    except InvalidVersion:
        too_old = False
    if too_old:
        warn_once("core", "no LangChain spans: langchain-core %s is older than %s", core, LANGCHAIN_CORE_FLOOR)
        return FAILED
    try:
        instrumentor = importlib.import_module(UPSTREAM_MODULE).LangChainInstrumentor()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        warn_once("import", "no LangChain spans: %s could not be imported: %r", UPSTREAM_DIST, exc)
        return FAILED
    langchain = distribution_version("langchain")
    if langchain is not None:  # the loader's dependency check is authoritative then
        requires = "; ".join(instrumentor.instrumentation_dependencies())
        msg = "no LangChain spans: `langchain` %s is installed but %s is not active (it requires %s). "
        warn_once("langchain", msg + "Fix: use a langchain version in that range", langchain, UPSTREAM_DIST, requires)
        return FAILED
    try:
        instrumentor.instrument(skip_dep_check=True)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        warn_once("activate", "no LangChain spans: activating %s failed: %r", UPSTREAM_DIST, exc)
        return FAILED
    return ACTIVATED
