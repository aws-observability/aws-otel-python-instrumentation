# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The installed distribution (tox installs it without -e) ships the hook shim at the top level, and its entry
points resolve to it."""
import importlib.metadata
from pathlib import Path

import cloudwatch_plugin_otel_hooks as shim
from genai_helpers import PLUGIN_DIST


def test_the_shim_is_imported_from_the_installed_distribution():
    installed = Path(importlib.metadata.distribution(PLUGIN_DIST).locate_file("cloudwatch_plugin_otel_hooks.py"))
    assert installed.is_file() and installed.resolve() == Path(shim.__file__).resolve()


def test_entry_points_resolve_to_the_shim_and_span_metrics_is_kept():
    entry_points = {(ep.group, ep.name): ep for ep in importlib.metadata.distribution(PLUGIN_DIST).entry_points}

    for hook in ("pre_instrument", "post_instrument"):
        entry_point = entry_points.pop((f"opentelemetry_{hook}", "cloudwatch_plugin"))
        assert entry_point.value == f"cloudwatch_plugin_otel_hooks:{hook}"
        assert entry_point.load() is getattr(shim, hook)
    span_metrics = entry_points.pop(("opentelemetry_instrumentor", "span_metrics"))
    assert span_metrics.value == "plugins.opentelemetry.cloudwatch.span_metrics.instrumentor:SpanMetricsInstrumentor"
    assert not entry_points
