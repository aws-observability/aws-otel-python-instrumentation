# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import os
import sys

import pytest
from genai_helpers import FAKE_INSTRUMENTORS, GENAI_ENV, FakeLangChainInstrumentor, fake_upstream_module
from plugins.opentelemetry.cloudwatch import _auto_instrumentation
from plugins.opentelemetry.cloudwatch.genai import _settings, content_capture
from plugins.opentelemetry.cloudwatch.genai import langchain as lc

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.test.globals_test import reset_logging_globals, reset_trace_globals
from opentelemetry.trace import set_tracer_provider


def _real_langchain_instrumentor():
    try:
        # pylint: disable=import-outside-toplevel
        from opentelemetry.instrumentation.genai.langchain import LangChainInstrumentor
    except ImportError:
        return None
    return LangChainInstrumentor


def _uninstrument_all():
    classes = list(FAKE_INSTRUMENTORS)
    real = _real_langchain_instrumentor()
    if real is not None:
        classes.append(real)
    for cls in classes:
        instrumentor = cls()
        if instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.uninstrument()


def reset_plugin_state():
    _auto_instrumentation._results.clear()
    _settings._warned.clear()
    _settings._debugged.clear()
    content_capture._unpatch()


@pytest.fixture(autouse=True)
def clean_genai_state():
    saved_env = dict(os.environ)
    for name in GENAI_ENV:
        os.environ.pop(name, None)
    loggers = [logging.getLogger(_settings.PLUGIN_LOGGER_NAME), logging.getLogger(_settings.LOGGER_NAME)]
    saved_loggers = [(logger, logger.level, list(logger.handlers)) for logger in loggers]
    reset_trace_globals()  # each test sets the providers it needs; the post steps change the global ones
    reset_logging_globals()
    reset_plugin_state()
    FakeLangChainInstrumentor.calls = []
    FakeLangChainInstrumentor.fail = False
    _uninstrument_all()
    yield
    _uninstrument_all()
    reset_plugin_state()
    reset_trace_globals()
    reset_logging_globals()
    for logger, level, handlers in saved_loggers:
        logger.setLevel(level)
        logger.handlers[:] = handlers
    os.environ.clear()
    os.environ.update(saved_env)


@pytest.fixture
def span_exporter():
    """A fresh SDK TracerProvider, set globally, with an in-memory exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    reset_trace_globals()
    set_tracer_provider(provider)
    yield exporter
    provider.shutdown()
    reset_trace_globals()


@pytest.fixture
def genai_active(monkeypatch):
    """A bridged GenAI instrumentation is active: the (fake) LangChain instrumentor is instrumenting."""
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, fake_upstream_module())
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)
