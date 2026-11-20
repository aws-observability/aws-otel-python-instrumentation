# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the OpenTelemetry Packaging bootstrap (vendor/sitecustomize.py) as build.sh installs it, with ADOT's
patches: the decisions it makes in a process started the way the operator starts it."""

import re
import sys

from conftest import operator_pythonpath, run_python, write_distribution


def _bundle_entries(state, payload):
    return [entry for entry in state["sys_path"] if entry == str(payload) or entry.startswith(str(payload) + "/")]


def _first_site_packages_index(state):
    return next(index for index, entry in enumerate(state["sys_path"]) if entry.endswith("site-packages"))


def test_initializes_on_a_supported_python(payload):
    pythonpath = operator_pythonpath(payload)
    state, stderr = run_python(pythonpath)
    assert state["initialized"]
    assert state["pythonpath"] == pythonpath
    assert "cannot auto-instrument" not in stderr


def test_skips_an_unsupported_python_before_initializing(payload):
    # Narrow the allowlist to exclude the running interpreter, which is what an unsupported Python sees.
    bootstrap = payload / "_otel_packaging_sitecustomize.py"
    text = bootstrap.read_text()
    text = re.sub(r"_SUPPORTED_PYTHON_MINORS = \([0-9, ]*\)", "_SUPPORTED_PYTHON_MINORS = (99,)", text)
    bootstrap.write_text(text)
    state, stderr = run_python(operator_pythonpath(payload))
    assert not state["initialized"]
    assert f"unsupported Python version: {sys.version_info[0]}.{sys.version_info[1]}" in stderr
    assert _bundle_entries(state, payload) == []
    assert state["pythonpath"] == ""
    assert state["prefix"] == ""


def test_initialization_failure_deactivates(payload, tmp_path):
    customer_entry = tmp_path / "customer"
    customer_entry.mkdir()
    state, stderr = run_python(operator_pythonpath(payload, customer_entry), {"FAKE_INITIALIZE": "raise"})
    assert "RuntimeError: fake initialization failure" in stderr
    assert _bundle_entries(state, payload) == []
    assert state["pythonpath"] == str(customer_entry)
    assert state["prefix"] == ""


# ADOT patch 0001: an application that ships OpenTelemetry packages of its own (Apache Airflow depends on
# opentelemetry-exporter-otlp) used to be treated as already instrumented and skipped.
def test_application_opentelemetry_packages_do_not_deactivate(payload, tmp_path):
    site_packages = tmp_path / "app-site-packages"
    write_distribution(site_packages, "opentelemetry_exporter_otlp-1.27.0", "opentelemetry-exporter-otlp", "1.27.0")
    write_distribution(site_packages, "opentelemetry_sdk-1.27.0", "opentelemetry-sdk", "1.27.0")
    state, stderr = run_python(operator_pythonpath(payload) + ":" + str(site_packages))
    assert state["initialized"]
    assert "cannot auto-instrument" not in stderr


def test_application_opentelemetry_packages_are_reported_in_debug_runs(payload, tmp_path):
    site_packages = tmp_path / "app-site-packages"
    write_distribution(site_packages, "opentelemetry_sdk-1.27.0", "opentelemetry-sdk", "1.27.0")
    state, stderr = run_python(
        operator_pythonpath(payload) + ":" + str(site_packages), {"OTEL_INJECTOR_LOG_LEVEL": "debug"}
    )
    assert state["initialized"]
    assert "opentelemetry-sdk 1.27.0" in stderr
    assert "Continuing with auto-instrumentation" in stderr


# ADOT patch 0002: the bundle used to be moved to the end of sys.path, behind the application's site-packages.
def test_bundle_stays_ahead_of_site_packages(payload):
    state, _ = run_python(operator_pythonpath(payload))
    bundle_index = state["sys_path"].index(str(payload))
    assert bundle_index < _first_site_packages_index(state)
    assert state["sys_path"][bundle_index + 1] == str(payload / f"cp3{sys.version_info[1]}")


# ADOT patch 0003: the upstream bundle used to set OTEL_*_EXPORTER to its own exporters, which the ADOT payload does
# not contain, so initialization failed whenever the operator left them unset.
def test_does_not_select_exporters(payload):
    state, _ = run_python(operator_pythonpath(payload))
    assert state["initialized"]
    assert state["exporters"] == {"TRACES": None, "METRICS": None, "LOGS": None}


def test_keeps_exporters_the_operator_set(payload):
    state, _ = run_python(operator_pythonpath(payload), {"OTEL_TRACES_EXPORTER": "otlp", "OTEL_LOGS_EXPORTER": "none"})
    assert state["exporters"] == {"TRACES": "otlp", "METRICS": None, "LOGS": "none"}


def test_opentelemetry_package_shadowed_by_an_earlier_entry_deactivates(payload, tmp_path):
    # The SDK pins the exact API version it was built with, so an older API ahead of the bundle cannot work.
    vendor = tmp_path / "vendor"
    write_distribution(vendor, "opentelemetry_api-1.27.0", "opentelemetry-api", "1.27.0")
    state, stderr = run_python(operator_pythonpath(payload, vendor))
    assert not state["initialized"]
    assert "dependency conflicts: opentelemetry-api (requires ==1.45.2, found 1.27.0)" in stderr
    assert _bundle_entries(state, payload) == []
    assert state["pythonpath"] == str(vendor)


def test_general_library_shadowed_by_an_earlier_entry_does_not_deactivate(payload, tmp_path):
    vendor = tmp_path / "vendor"
    write_distribution(vendor, "urllib3-1.26.20", "urllib3", "1.26.20")
    state, stderr = run_python(operator_pythonpath(payload, vendor))
    assert state["initialized"]
    assert "cannot auto-instrument" not in stderr
