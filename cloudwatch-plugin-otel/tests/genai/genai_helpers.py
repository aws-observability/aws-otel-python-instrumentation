# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Shared fakes and helpers for the GenAI tests (importable because pytest puts this directory on sys.path)."""
import importlib.machinery
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest
from plugins.opentelemetry.cloudwatch.genai import _settings

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

# Every env var the GenAI steps read.
GENAI_ENV = (
    "CW_PLUGIN_GENAI_ENABLED",
    "OTEL_SDK_DISABLED",
    "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS",
    "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
    "OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT",
    "OTEL_PYTHON_LOG_LEVEL",
    "OTEL_RESOURCE_ATTRIBUTES",
    "AGENTCORE_RUNTIME_URL",
)

OLD = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"
NEW = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT"
PLUGIN_DIST = "cloudwatch-plugin-otel"
# The session.id, demotion and resource steps, with an SDK TracerProvider and an active LangChain instrumentor.
EVALUABLE = {"session.id": "on", "nested chat demotion": "on", "agent identity": "service.type"}
HERE = Path(__file__).parent


class FakeLangChainInstrumentor(BaseInstrumentor):
    """Stands in for the upstream instrumentor; declares the same unmet dependency."""

    calls = []
    fail = False

    def instrumentation_dependencies(self):
        return ("langchain >= 0.3.22, < 2",)

    def _instrument(self, **kwargs):
        if self.fail:
            raise RuntimeError("boom")
        type(self).calls.append(kwargs)

    def _uninstrument(self, **kwargs):
        pass


def fake_upstream_module(cls=None):
    """A stand-in for ``opentelemetry.instrumentation.genai.langchain`` exposing ``cls`` as LangChainInstrumentor."""
    module = types.ModuleType("opentelemetry.instrumentation.genai.langchain")
    module.LangChainInstrumentor = cls or FakeLangChainInstrumentor
    return module


# BaseInstrumentor subclasses are per-class singletons, so each test starts and ends with them uninstrumented.
FAKE_INSTRUMENTORS = (FakeLangChainInstrumentor,)


def plugin_records(caplog, level=None):
    """The plugin's log records captured by ``caplog``, optionally only those at ``level``."""
    return [
        record
        for record in caplog.records
        if record.name.startswith(_settings.PLUGIN_LOGGER_NAME) and (level is None or record.levelno == level)
    ]


def messages(caplog, level=None):
    return [record.getMessage() for record in plugin_records(caplog, level)]


def summary_parts(line):
    """``{step: outcome}`` of the startup summary line."""
    assert line.startswith("CW-PLUGIN genai: "), line
    return dict(part.split("=", 1) for part in line[len("CW-PLUGIN genai: ") :].split("; "))


def plugin_entry_points(group):
    """This package's real, installed entry points for ``group`` (requires a non-editable install)."""
    # pylint: disable=import-outside-toplevel
    from opentelemetry.util._importlib_metadata import entry_points

    found = [ep for ep in entry_points(group=group) if ep.dist is not None and ep.dist.name == PLUGIN_DIST]
    if not found:
        pytest.fail(
            f"{PLUGIN_DIST} is not installed with its {group} entry point; install it (pip install --no-deps .) "
            "so the tests exercise the real packaging"
        )
    return found


def write_stub_dist(directory, name, version, entry_points="", module_name=None, module_code=""):
    """Writes a minimal installed distribution (dist-info, optionally a module) into ``directory``."""
    directory = Path(directory)
    dist_info = directory / f"{name.replace('-', '_')}-{version}.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n", "utf-8")
    if entry_points:
        (dist_info / "entry_points.txt").write_text(entry_points, "utf-8")
    if module_name:
        (directory / f"{module_name}.py").write_text(module_code, "utf-8")


def child_env(**extra):
    """The parent's env without any OTEL_* or GenAI switch, plus ``extra``: children start from a known state."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_") and k not in GENAI_ENV}
    env.update(extra)
    return env


def run_child(script, *args, env=None, timeout=300):
    """Runs ``script`` from this directory in a clean ``python -I`` child; returns (JSON report, stderr)."""
    proc = subprocess.run(
        [sys.executable, "-I", str(HERE / script), *map(str, args)],
        capture_output=True,
        text=True,
        env=child_env() if env is None else env,
        timeout=timeout,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1]), proc.stderr


FAKE_UTILS_SOURCE = """
import enum
import logging
import os

logger = logging.getLogger(__name__)


class ContentCapturingMode(enum.Enum):
    NO_CONTENT = 0
    SPAN_ONLY = 1
    EVENT_ONLY = 2
    SPAN_AND_EVENT = 3


def get_content_capturing_mode():
    value = os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "").strip()
    if not value:
        return ContentCapturingMode.NO_CONTENT
    try:
        return ContentCapturingMode[value.upper()]
    except KeyError:
        logger.warning(
            "%s is not a valid option for `%s` environment variable. Must be one of %s. Defaulting to `NO_CONTENT`.",
            value,
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
            ", ".join(e.name for e in ContentCapturingMode),
        )
        return ContentCapturingMode.NO_CONTENT
"""

FAKE_HANDLER_SOURCE = """
from opentelemetry.util.genai.utils import ContentCapturingMode, get_content_capturing_mode


class TelemetryHandler:
    def __init__(self, tracer_provider=None, meter_provider=None, logger_provider=None, completion_hook=None,
                 instrumentation_scope_name=None, instrumentation_scope_version=None):
        self._content_capturing_mode = get_content_capturing_mode()
"""

FAKE_INVOCATION_SOURCE = """
from opentelemetry.util.genai.utils import get_content_capturing_mode


class Invocation:
    def __init__(self, content_capturing_mode=None):
        self._content_capturing_mode = (
            get_content_capturing_mode() if content_capturing_mode is None else content_capturing_mode
        )
"""

FAKE_SITES = (
    "opentelemetry.util.genai.utils",
    "opentelemetry.util.genai.handler",
    "opentelemetry.util.genai._invocation",
)


def install_fake_util_genai(monkeypatch, handler_source=FAKE_HANDLER_SOURCE, invocation_source=FAKE_INVOCATION_SOURCE):
    """Stand-ins for util-genai 1.3b0's package and its utils, handler and _invocation modules (the resolver's
    binding sites), registered in sys.modules. Returns ``(utils, handler, invocation)``."""
    # pylint: disable=import-outside-toplevel
    from plugins.opentelemetry.cloudwatch.genai import content_capture

    package = types.ModuleType("opentelemetry.util.genai")
    package.__spec__ = importlib.machinery.ModuleSpec(package.__name__, None, is_package=True)
    monkeypatch.setitem(sys.modules, package.__name__, package)  # what find_spec sees
    # There are no source files to scan for the stand-ins: report exactly the known binding sites.
    monkeypatch.setattr(content_capture, "_binding_sites", lambda: set(FAKE_SITES))
    modules = []
    for name, source in zip(FAKE_SITES, (FAKE_UTILS_SOURCE, handler_source, invocation_source)):
        module = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, module)
        exec(compile(source, f"<fake {name}>", "exec"), module.__dict__)  # pylint: disable=exec-used
        modules.append(module)
    return tuple(modules)
