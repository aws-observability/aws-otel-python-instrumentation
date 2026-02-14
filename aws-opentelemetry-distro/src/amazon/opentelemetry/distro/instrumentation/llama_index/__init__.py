# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Collection, Optional

from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.trace import Span

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class LlamaIndexInstrumentor(BaseInstrumentor):  # type: ignore
    """
    An instrumentor for LlamaIndex
    """

    __slots__ = (
        "_span_handler",
        "_event_handler",
    )

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("llama-index-core >= 0.10.43",)

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)

        from llama_index.core.instrumentation import get_dispatcher  # type: ignore[attr-defined]

        from ._handler import EventHandler, _SpanHandler

        self._span_handler = _SpanHandler(
            tracer=tracer,
            separate_trace_from_runtime_context=bool(kwargs.get("separate_trace_from_runtime_context")),
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
        from llama_index.core.instrumentation import get_dispatcher  # type: ignore[attr-defined]

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

    from amazon.opentelemetry.distro.instrumentation.llama_index._handler import _SpanHandler

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
