# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for adot_sitecustomize.py, the entry point the operator's PYTHONPATH loads: it runs the bootstrap from the
payload root and, when the bootstrap deactivates, removes the directories the bootstrap does not know about."""

import os
import py_compile
import shutil
import subprocess

import pytest
from conftest import AUTO_INSTRUMENTATION, STATE_SCRIPT, TOOL_DIRECTORY, operator_pythonpath, run_python

# Interpreters older than the payload supports, which the operator injects into just the same. They must parse both
# files and start the application with its own packages.
OLD_INTERPRETERS = ["2.7", "3.5", "3.6", "3.7", "3.8", "3.9"]


def _entries_inside(paths, payload):
    return [entry for entry in paths if entry == str(payload) or entry.startswith(str(payload) + os.sep)]


def test_runs_the_bootstrap_from_the_payload_root(payload):
    state, _ = run_python(operator_pythonpath(payload))
    assert state["initialized"]
    # The shim's own directory stays, as it always has: upstream initialize() keys its child-process guard on it.
    assert str(payload / AUTO_INSTRUMENTATION) in state["sys_path"]


def test_removes_every_payload_directory_when_the_bootstrap_deactivates(payload, tmp_path):
    customer_entry = tmp_path / "customer"
    customer_entry.mkdir()
    state, _ = run_python(operator_pythonpath(payload, customer_entry), {"FAKE_INITIALIZE": "raise"})
    # Includes the auto_instrumentation directory, which only the operator adds, and cp3<minor>, which the bootstrap
    # inserts but does not remove.
    assert _entries_inside(state["sys_path"], payload) == []
    assert state["pythonpath"] == str(customer_entry)


def test_child_processes_do_not_load_the_payload_after_a_skip(payload):
    # The parent's own initialize() call is forgotten first, so that the child's state reports the child alone.
    child = (
        "import json, os, subprocess, sys; "
        "os.environ.pop('FAKE_INITIALIZE_CALLED', None); "
        f"child = subprocess.run([sys.executable, '-c', {STATE_SCRIPT!r}], capture_output=True, text=True); "
        "print(child.stdout, end='')"
    )
    state, _ = run_python(operator_pythonpath(payload), {"FAKE_INITIALIZE": "raise"}, script=child)
    assert _entries_inside(state["sys_path"], payload) == []
    assert not state["initialized"]


def test_deactivates_when_the_bootstrap_is_missing(payload):
    (payload / "_otel_packaging_sitecustomize.py").unlink()
    state, stderr = run_python(operator_pythonpath(payload))
    assert not state["initialized"]
    assert "cannot auto-instrument Python process: cannot run" in stderr
    assert _entries_inside(state["sys_path"], payload) == []
    assert state["pythonpath"] == ""
    assert state["prefix"] == ""


def test_deactivates_when_the_bootstrap_does_not_compile(payload):
    (payload / "_otel_packaging_sitecustomize.py").write_text("def broken(:\n")
    state, stderr = run_python(operator_pythonpath(payload))
    assert not state["initialized"]
    assert "SyntaxError" in stderr
    assert _entries_inside(state["sys_path"], payload) == []


def test_shim_and_bootstrap_compile_on_this_interpreter(payload, tmp_path):
    for source in (TOOL_DIRECTORY / "adot_sitecustomize.py", payload / "_otel_packaging_sitecustomize.py"):
        py_compile.compile(str(source), cfile=str(tmp_path / "compiled.pyc"), doraise=True)


@pytest.mark.skipif(
    os.environ.get("RUN_DOCKER_TESTS") != "1" or shutil.which("docker") is None,
    reason="starts python:<version>-slim containers; set RUN_DOCKER_TESTS=1",
)
@pytest.mark.parametrize("version", OLD_INTERPRETERS)
def test_old_interpreters_start_without_the_payload(payload, tmp_path, version):
    # Stands in for the payload's urllib3 2.x, which uses syntax that older interpreters cannot run: if the payload
    # stayed on sys.path, importing it would fail exactly as the application's own import of urllib3 did.
    (payload / "shadowed_by_payload").mkdir()
    (payload / "shadowed_by_payload" / "__init__.py").write_text("X = int | str\n")
    app = tmp_path / "app"
    (app / "shadowed_by_payload").mkdir(parents=True)
    (app / "shadowed_by_payload" / "__init__.py").write_text("SOURCE = 'application'\n")
    mount = "/otel-auto-instrumentation-python"
    pythonpath = f"{mount}/{AUTO_INSTRUMENTATION.as_posix()}:{mount}:/app"
    script = (
        "import os, sys, shadowed_by_payload; "
        "print(shadowed_by_payload.SOURCE); "
        f"print([p for p in sys.path if p.startswith('{mount}')]); "
        "print(repr(os.environ.get('PYTHONPATH')))"
    )
    result = subprocess.run(
        [
            "docker", "run", "--rm", "--network", "none",
            "-v", f"{payload}:{mount}:ro", "-v", f"{app}:/app:ro",
            "-e", "PYTHONPATH=" + pythonpath, "-e", "PYTHONDONTWRITEBYTECODE=1",
            f"python:{version}-slim", "python", "-c", script,
        ],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["application", "[]", "'/app'"]
    assert f"unsupported Python version: {version}" in result.stderr
