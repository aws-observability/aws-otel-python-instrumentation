# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Collection, Optional

from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.trace import Span

from openinference.instrumentation import OITracer, TraceConfig
from openinference.instrumentation.llama_index.package import _instruments
from openinference.instrumentation.llama_index.version import __version__

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class LlamaIndexInstrumentor(BaseInstrumentor):  # type: ignore
    """
    An instrumentor for LlamaIndex
    """

    __slots__ = (
        "_config",
        "_span_handler",
        "_event_handler",
    )

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        if not (tracer_provider := kwargs.get("tracer_provider")):
            tracer_provider = trace_api.get_tracer_provider()
        if not (config := kwargs.get("config")):
            config = TraceConfig()
        else:
            assert isinstance(config, TraceConfig)
        
        tracer = OITracer(
            trace_api.get_tracer(__name__, __version__, tracer_provider),
            config=config,
        )
        
        from llama_index.core.instrumentation import (  # type: ignore[attr-defined]
            get_dispatcher,
        )

        from ._handler import EventHandler, _SpanHandler

        self._span_handler = _SpanHandler(
            tracer=tracer,
            separate_trace_from_runtime_context=bool(
                kwargs.get("separate_trace_from_runtime_context")
            ),
        )
        self._event_handler = EventHandler(span_handler=self._span_handler)
        dispatcher = get_dispatcher()
        for span_handler in dispatcher.span_handlers:
            if isinstance(span_handler, type(self._span_handler)):
                break
        else:
            dispatcher.add_span_handler(self._span_handler)
        for event_handler in dispatcher.event_handlers:
            if isinstance(event_handler, type(self._event_handler)):
                break
        else:
            dispatcher.add_event_handler(self._event_handler)

    def _uninstrument(self, **kwargs: Any) -> None:
        if self._event_handler is None:
            return
        from llama_index.core.instrumentation import (  # type: ignore[attr-defined]
            get_dispatcher,
        )

        dispatcher = get_dispatcher()
        dispatcher.span_handlers[:] = filter(
            lambda h: not isinstance(h, type(self._span_handler)),
            dispatcher.span_handlers,
        )
        dispatcher.event_handlers[:] = filter(
            lambda h: not isinstance(h, type(self._event_handler)),
            dispatcher.event_handlers,
        )
        self._event_handler = None


def get_current_span() -> Optional[Span]:
    from llama_index.core.instrumentation.span import active_span_id
    from openinference.instrumentation.llama_index._handler import _SpanHandler

    if not isinstance(id_ := active_span_id.get(), str):
        return None
    instrumentor = LlamaIndexInstrumentor()
    try:
        span_handler = instrumentor._span_handler
    except AttributeError:
        return None
    if not isinstance(span_handler, _SpanHandler):
        return None
    if (span := span_handler.open_spans.get(id_)) is None:
        return None
    return span._otel_span