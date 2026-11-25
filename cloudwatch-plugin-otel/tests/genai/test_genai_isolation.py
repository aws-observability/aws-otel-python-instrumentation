# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The hooks never break the auto-instrumentation loader (real loader, in a clean ``python -I`` child)."""
import json
import subprocess
import sys
from pathlib import Path

import pytest
from genai_helpers import plugin_entry_points, run_child

CHILD = Path(__file__).with_name("child_isolation.py")
PLUGIN_LOGGER = "plugins.opentelemetry.cloudwatch"

SENTINEL_MODULE = """
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

RAN = False


class SentinelInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return ()

    def _instrument(self, **kwargs):
        global RAN
        RAN = True

    def _uninstrument(self, **kwargs):
        pass
"""


@pytest.fixture
def workdir(tmp_path):
    (tmp_path / "cw_plugin_sentinel.py").write_text(SENTINEL_MODULE, encoding="utf-8")
    dist_info = tmp_path / "cw_plugin_sentinel-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text("Metadata-Version: 2.1\nName: cw-plugin-sentinel\nVersion: 1.0\n", "utf-8")
    (dist_info / "entry_points.txt").write_text(
        "[opentelemetry_instrumentor]\ncw_plugin_sentinel = cw_plugin_sentinel:SentinelInstrumentor\n", "utf-8"
    )
    return tmp_path


def _run_child(scenario, workdir):
    # The hooks must be the real installed entry points, not something the child invents.
    expected = {
        "opentelemetry_pre_instrument": [ep.value for ep in plugin_entry_points("opentelemetry_pre_instrument")],
        "opentelemetry_post_instrument": [ep.value for ep in plugin_entry_points("opentelemetry_post_instrument")],
    }
    proc = subprocess.run(
        [sys.executable, "-I", str(CHILD), scenario, str(workdir)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    report = json.loads(proc.stdout.strip().splitlines()[-1])
    assert report["loader_returned"] is True
    assert report["sentinel_ran"] is True, "an instrumentor loaded between the hooks did not run"
    assert report["entry_points"]["opentelemetry_pre_instrument"] == expected["opentelemetry_pre_instrument"]
    assert report["entry_points"]["opentelemetry_post_instrument"] == expected["opentelemetry_post_instrument"]
    assert report["entry_points"]["opentelemetry_pre_instrument"] == ["cloudwatch_plugin_otel_hooks:pre_instrument"]
    return report


def _plugin_records(report, level):
    return [r for r in report["records"] if r["name"].startswith(PLUGIN_LOGGER) and r["level"] == level]


def test_shadowing_plugins_package_does_not_break_the_loader(workdir):
    shadow = workdir / "plugins"
    shadow.mkdir()
    (shadow / "__init__.py").write_text("# the application's own plugins package\n", encoding="utf-8")

    report = _run_child("shadow", workdir)

    assert report["plugins_file"] == str(shadow / "__init__.py")
    records = [r for r in report["records"] if r["name"] == PLUGIN_LOGGER]
    # Warned once (pre), with the traceback at DEBUG, then quiet (post).
    assert [r["level"] for r in records] == ["WARNING", "DEBUG", "DEBUG"]
    assert "ModuleNotFoundError" in records[1]["exc"] and records[0]["exc"] == ""
    warning = records[0]["msg"]
    assert str(shadow / "__init__.py") in warning and "inactive" in warning and "Fix: rename" in warning


def test_shadow_warning_is_visible_without_any_logging_configuration(workdir):
    """Python's logging.lastResort prints the WARNING to stderr even when no handler is configured."""
    shadow = workdir / "plugins"
    shadow.mkdir()
    (shadow / "__init__.py").write_text("", encoding="utf-8")

    _, stderr = run_child("child_isolation.py", "shadow-no-logging", workdir)
    assert "CW-PLUGIN: a `plugins` package earlier on sys.path" in stderr


def test_a_raising_step_logs_one_warning_and_the_loader_completes(workdir):
    report = _run_child("raising_step", workdir)

    warnings = _plugin_records(report, "WARNING")
    assert len(warnings) == 1
    assert warnings[0]["msg"].startswith("CW-PLUGIN: test.raising_step setup failed")
    assert "RuntimeError: injected step failure" in warnings[0]["exc"]


def test_missing_opentelemetry_sdk_does_not_break_the_loader(workdir):
    report = _run_child("no_sdk", workdir)

    assert report["sdk_imported"] is False
    debug = _plugin_records(report, "DEBUG")
    assert len(debug) == 2
    assert all("opentelemetry.sdk" in r["exc"] for r in debug)
    assert _plugin_records(report, "WARNING") == []
