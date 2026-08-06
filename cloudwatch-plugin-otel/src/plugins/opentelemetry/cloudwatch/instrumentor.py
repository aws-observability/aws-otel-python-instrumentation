# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
This library derives call-count and duration metrics from every span produced by
the active tracer pipeline, and ensures spans are recorded even when the root
sampler would drop them.

It registers a ``SpanMetricsConnector`` (a read-only ``SpanProcessor``) on the
installed ``TracerProvider`` and wraps the provider's root sampler with an
``AlwaysRecordSampler`` so that ``DROP`` decisions become ``RECORD_ONLY`` -- the
spans still reach the processor to be counted, without changing what gets exported.

Usage
-----

Auto-instrumentation
********************

Installing this package registers a ``span_metrics`` entry point in the
``opentelemetry_instrumentor`` group, so ``opentelemetry-instrument`` picks it up
with no code changes:

.. code-block:: sh

    opentelemetry-instrument python your_app.py

The configurator installs the ``MeterProvider`` before instrumentors load, so the
derived metrics bind to a real meter automatically.

Manual
******

.. code-block:: python

    from plugins.opentelemetry.cloudwatch import SpanMetricsInstrumentor

    # Attach to the active provider (or pass tracer_provider=/meter_provider=).
    SpanMetricsInstrumentor().instrument()

Attach the pieces directly if you manage your own pipeline:

.. code-block:: python

    from plugins.opentelemetry.cloudwatch import (
        AlwaysRecordSampler,
        SpanMetricsConnector,
    )

    # Wrap the root sampler so dropped spans are still recorded.
    tracer_provider.sampler = AlwaysRecordSampler(tracer_provider.sampler)
    tracer_provider.add_span_processor(SpanMetricsConnector(meter_provider))

.. note::

    The connector binds its meter at construction time. Set the global
    ``MeterProvider`` (or pass ``meter_provider=`` to ``instrument()``) *before*
    instrumenting, otherwise the derived metrics are dropped to a NoOp meter.

Uninstrument
************

The SDK has no API to detach a span processor, so ``uninstrument()`` restores the
original sampler and calls ``shutdown()`` on the processor (it stops recording) as
a best effort; the processor stays attached to the provider.
"""

import logging
from typing import Collection, Optional

from plugins.opentelemetry.cloudwatch.connector.span_metrics_connector import SpanMetricsConnector
from plugins.opentelemetry.cloudwatch.sampler.always_record_sampler import AlwaysRecordSampler

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.sdk.trace.sampling import Sampler

_logger = logging.getLogger(__name__)


class SpanMetricsInstrumentor(BaseInstrumentor):
    def __init__(self) -> None:
        super().__init__()
        self._processor: Optional[SpanMetricsConnector] = None
        self._tracer_provider = None
        self._original_sampler: Optional[Sampler] = None
        self._installed_sampler: Optional[AlwaysRecordSampler] = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("opentelemetry-sdk >= 1.30.0",)

    def _instrument(self, **kwargs) -> None:
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()

        if not hasattr(tracer_provider, "add_span_processor"):
            _logger.warning(
                "Active tracer provider %s has no add_span_processor; "
                "SpanMetricsConnector was not registered. Set an SDK TracerProvider "
                "before instrumenting.",
                type(tracer_provider).__name__,
            )
            return

        if tracer_provider is not self._tracer_provider:
            self._tracer_provider = tracer_provider
            self._original_sampler = None
            self._installed_sampler = None
            self._processor = None

        self._set_always_record_sampler(tracer_provider)
        self._set_span_metrics_connector(tracer_provider, kwargs.get("meter_provider"))

    def _uninstrument(self, **kwargs) -> None:
        if self._installed_sampler is not None:
            if getattr(self._tracer_provider, "sampler", None) is self._installed_sampler:
                self._tracer_provider.sampler = self._original_sampler
            self._installed_sampler.enabled = False

        if self._processor is not None:
            self._processor.shutdown()

    def _set_always_record_sampler(self, tracer_provider) -> None:
        root_sampler = getattr(tracer_provider, "sampler", None)
        if root_sampler is None:
            return

        if self._installed_sampler is not None and root_sampler is self._original_sampler:
            self._installed_sampler.enabled = True
            tracer_provider.sampler = self._installed_sampler
            return

        if isinstance(root_sampler, AlwaysRecordSampler):
            return

        self._original_sampler = root_sampler
        self._installed_sampler = AlwaysRecordSampler(root_sampler)
        tracer_provider.sampler = self._installed_sampler

    def _set_span_metrics_connector(self, tracer_provider, meter_provider) -> None:
        if self._processor is not None:
            self._processor.enabled = True
            return

        self._processor = SpanMetricsConnector(meter_provider=meter_provider)
        tracer_provider.add_span_processor(self._processor)
