# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""What the session, nested-client and resource post steps share: whether they apply, and the SDK pipeline they
change. Each step is idempotent rather than aware of other distros: it changes nothing that is already in place."""

from plugins.opentelemetry.cloudwatch.genai import langchain
from plugins.opentelemetry.cloudwatch.genai._settings import debug_once

from opentelemetry import trace

NOT_SDK = "skipped (not an SDK TracerProvider)"


def genai_instrumentation_active() -> bool:
    """Whether a GenAI instrumentation the plugin bridges is instrumenting, however it was started (the loader or
    the plugin). Only LangChain today; each future integration adds its own check here."""
    return langchain.upstream_instrumented()


def sdk_tracer_provider():
    """The global tracer provider if it is an SDK one (it can take span processors), else None after one DEBUG
    line."""
    provider = trace.get_tracer_provider()
    if hasattr(provider, "add_span_processor"):
        return provider
    debug_once("not sdk", "%s is not an SDK TracerProvider; session.id, demotion and resource steps skipped", provider)
    return None


def has_span_processor(provider, cls) -> bool:
    """Whether ``provider`` already runs a ``cls`` instance, so a repeated post hook adds no second one. Only the
    plugin's own processor classes are looked for. Reads the SDK's private processor tuple
    (``TracerProvider._active_span_processor._span_processors``, SDK 1.42 and later); without it, False."""
    active = getattr(provider, "_active_span_processor", None)
    return any(isinstance(p, cls) for p in getattr(active, "_span_processors", ()))
