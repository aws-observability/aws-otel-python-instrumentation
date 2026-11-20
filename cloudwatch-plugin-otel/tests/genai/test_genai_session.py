# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""``session.id`` from W3C baggage (no upstream packages needed)."""
import pytest
from plugins.opentelemetry.cloudwatch.genai import session

from opentelemetry import baggage, context, propagate, trace
from opentelemetry.trace import SpanKind


@pytest.fixture
def session_on(genai_active, span_exporter):
    """The session step run against a fresh SDK TracerProvider; yields the exporter."""
    assert session.session_step() == session.ON
    return span_exporter


def _request_tree(request_context=None):
    """A SERVER span with the GenAI and client spans of one request under it, as instrumentations nest them. The
    extracted ``request_context`` is attached first, as the WSGI and ASGI instrumentations do."""
    tracer = trace.get_tracer("test")
    token = context.attach(request_context) if request_context is not None else None
    try:
        with tracer.start_as_current_span("POST /invocations", kind=SpanKind.SERVER):
            with tracer.start_as_current_span("invoke_agent weather_agent"):
                with tracer.start_as_current_span("chat model", kind=SpanKind.CLIENT):
                    with tracer.start_as_current_span("chat", kind=SpanKind.CLIENT):  # botocore's
                        pass
                with tracer.start_as_current_span("execute_tool get_weather"):
                    pass
    finally:
        if token is not None:
            context.detach(token)


def _session_ids(exporter):
    return {span.name: span.attributes.get(session.SESSION_ID) for span in exporter.get_finished_spans()}


def test_baggage_session_id_is_on_every_span_of_the_request(session_on):
    _request_tree(baggage.set_baggage("session.id", "S1"))
    trace.get_tracer("test").start_span("startup").end()  # outside any request

    ids = _session_ids(session_on)
    assert ids.pop("startup") is None
    assert len(ids) == 5 and set(ids.values()) == {"S1"}, ids


def test_baggage_header_through_the_global_textmap(session_on):
    _request_tree(propagate.extract({"baggage": "session.id=S1"}))
    assert set(_session_ids(session_on).values()) == {"S1"}


def test_an_existing_session_id_is_not_overwritten(session_on):
    ctx = propagate.extract({"baggage": "session.id=S1"})
    trace.get_tracer("test").start_span("mine", context=ctx, attributes={"session.id": "app"}).end()
    assert _session_ids(session_on) == {"mine": "app"}


@pytest.mark.parametrize("carrier", [{}, {"baggage": "other=1"}, {"baggage": "session.id="}])
def test_no_baggage_session_id_no_attribute(session_on, carrier):
    _request_tree(propagate.extract(carrier))
    assert set(_session_ids(session_on).values()) == {None}


def test_the_global_textmap_is_left_alone(genai_active, span_exporter):
    textmap = propagate.get_global_textmap()
    session.session_step()
    assert propagate.get_global_textmap() is textmap


def test_the_processor_is_added_once(genai_active, span_exporter):
    session.session_step()
    session.session_step()
    processors = trace.get_tracer_provider()._active_span_processor._span_processors
    assert len([p for p in processors if isinstance(p, session.SessionIdSpanProcessor)]) == 1


def test_a_processor_registered_by_an_earlier_run_is_not_duplicated(genai_active, span_exporter):
    """The check finds only the plugin's own processor; it makes a repeated post hook idempotent."""
    provider = trace.get_tracer_provider()
    existing = session.SessionIdSpanProcessor()
    provider.add_span_processor(existing)

    assert session.session_step() == session.ON
    processors = provider._active_span_processor._span_processors
    assert [p for p in processors if isinstance(p, session.SessionIdSpanProcessor)] == [existing]
