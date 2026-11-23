# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for build.sh: what it installs into a payload, and that it refuses inputs it cannot vouch for."""

import re
import shutil
import subprocess

from conftest import AUTO_INSTRUMENTATION, TOOL_DIRECTORY, build_payload, supported_minors


def test_installs_the_shim_in_place_of_upstreams_sitecustomize(payload):
    installed = (payload / AUTO_INSTRUMENTATION / "sitecustomize.py").read_text()
    assert installed == (TOOL_DIRECTORY / "adot_sitecustomize.py").read_text()


def test_installs_the_patched_upstream_bootstrap_at_the_payload_root(payload):
    bootstrap = (payload / "_otel_packaging_sitecustomize.py").read_text()
    for patch_file in sorted((TOOL_DIRECTORY / "patches").glob("*.patch")):
        number = patch_file.name.split("-", 1)[0]
        assert f"ADOT patch {number}:" in bootstrap, patch_file.name
    # Nothing named sitecustomize.py at the root: upstream initialize() keeps child processes it starts from
    # re-running auto-instrumentation by removing the auto_instrumentation directory from PYTHONPATH, which only works
    # if no other sitecustomize.py is on it.
    assert not (payload / "sitecustomize.py").exists()


def test_sets_the_python_allowlist_to_the_supported_minors(payload):
    bootstrap = (payload / "_otel_packaging_sitecustomize.py").read_text()
    expected = f"_SUPPORTED_PYTHON_MINORS = ({', '.join(supported_minors())},)  # supported-python-minors"
    assert bootstrap.count("# supported-python-minors") == 1
    assert expected in bootstrap


def test_supported_minors_match_the_distro_classifiers():
    pyproject = (TOOL_DIRECTORY.parent.parent / "aws-opentelemetry-distro" / "pyproject.toml").read_text()
    classifiers = re.findall(r'"Programming Language :: Python :: 3\.([0-9]+)"', pyproject)
    assert sorted(classifiers, key=int) == supported_minors()


def test_lists_only_opentelemetry_distributions_for_every_supported_minor(payload):
    for minor in supported_minors():
        manifest = (payload / ("cp3" + minor) / "all-dependencies.txt").read_text().splitlines()
        requirements = [line for line in manifest if line and not line.startswith("#")]
        # urllib3 is in the payload too, but general-purpose libraries are not checked.
        assert requirements == [
            "aws-opentelemetry-distro==0.23.0",
            "opentelemetry-api==1.45.2",
            "opentelemetry-sdk==1.45.2",
        ]


def test_refuses_a_vendored_file_that_does_not_match_the_pinned_sha256(tmp_path):
    tool_copy = tmp_path / "otel-packaging-python"
    shutil.copytree(TOOL_DIRECTORY, tool_copy, ignore=shutil.ignore_patterns("tests", "__pycache__"))
    with open(tool_copy / "vendor" / "sitecustomize.py", "a", encoding="utf-8") as vendored:
        vendored.write("# modified\n")
    result = build_payload(tmp_path / "payload", tool_directory=tool_copy)
    assert result.returncode != 0
    assert "UPSTREAM pins" in result.stderr
    assert not (tmp_path / "payload" / "_otel_packaging_sitecustomize.py").exists()


def test_refuses_a_directory_that_is_not_an_installed_payload(tmp_path):
    result = subprocess.run(
        ["bash", str(TOOL_DIRECTORY / "build.sh"), str(tmp_path)], capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert "not an installed ADOT Python payload" in result.stderr
