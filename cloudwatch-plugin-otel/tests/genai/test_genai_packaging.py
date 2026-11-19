# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in (CW_PLUGIN_TEST_BUILD_WHEEL=1): the built wheel ships the shim at the top level, the GenAI package,
both hook groups and the extra."""
import importlib.util
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(
    os.environ.get("CW_PLUGIN_TEST_BUILD_WHEEL") != "1",
    reason="building the wheel is opt-in: CW_PLUGIN_TEST_BUILD_WHEEL=1",
)


def _build_command(out_dir):
    if shutil.which("uv"):
        return ["uv", "build", "--wheel", "--out-dir", str(out_dir), str(PROJECT)]
    if importlib.util.find_spec("build") is not None:
        return [sys.executable, "-m", "build", "--wheel", "--outdir", str(out_dir), str(PROJECT)]
    return None


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    """The wheel at CW_PLUGIN_WHEEL (CI checks the one it uploads), else one built from this checkout."""
    path = os.environ.get("CW_PLUGIN_WHEEL")
    if not path:
        out_dir = tmp_path_factory.mktemp("wheel")
        command = _build_command(out_dir)
        if command is None:
            pytest.skip("neither uv nor the build module is available")
        proc = subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        (path,) = out_dir.glob("*.whl")
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        entry_points = archive.read(next(n for n in names if n.endswith(".dist-info/entry_points.txt"))).decode()
        metadata = archive.read(next(n for n in names if n.endswith(".dist-info/METADATA"))).decode()
    return names, entry_points, metadata


def test_shim_is_at_the_top_level(wheel):
    names, _, _ = wheel
    assert "cloudwatch_plugin_otel_hooks.py" in names


def test_genai_modules_are_shipped(wheel):
    names, _, _ = wheel
    genai = {n for n in names if n.startswith("plugins/opentelemetry/cloudwatch/genai/") and n.endswith(".py")}
    expected = {"__init__.py", "_settings.py", "content_capture.py", "langchain.py"}
    assert genai == {"plugins/opentelemetry/cloudwatch/genai/" + name for name in expected}
    assert "plugins/opentelemetry/cloudwatch/_auto_instrumentation.py" in names


def test_entry_points(wheel):
    _, entry_points, _ = wheel
    assert "[opentelemetry_pre_instrument]\ncloudwatch_plugin = cloudwatch_plugin_otel_hooks:pre_instrument" in (
        entry_points
    )
    assert "[opentelemetry_post_instrument]\ncloudwatch_plugin = cloudwatch_plugin_otel_hooks:post_instrument" in (
        entry_points
    )
    assert (
        "[opentelemetry_instrumentor]\n"
        "span_metrics = plugins.opentelemetry.cloudwatch.span_metrics.instrumentor:SpanMetricsInstrumentor"
    ) in entry_points


def test_langchain_extra_and_no_new_required_dependencies(wheel):
    _, _, metadata = wheel
    lines = metadata.splitlines()
    assert "Provides-Extra: langchain" in lines
    assert "Requires-Dist: opentelemetry-instrumentation-genai-langchain<2,>=1.3b0; extra == 'langchain'" in lines
    # The GenAI packages come only through the extra (the exact core floors are not this test's business).
    required = [line for line in lines if line.startswith("Requires-Dist:") and "extra ==" not in line]
    assert required and not [line for line in required if any(word in line for word in ("genai", "langchain"))]
