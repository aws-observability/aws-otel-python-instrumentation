# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
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
        self._set_connector(tracer_provider, kwargs.get("meter_provider"))

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

    def _set_connector(self, tracer_provider, meter_provider) -> None:
        if self._processor is not None:
            self._processor.enabled = True
            return

        self._processor = SpanMetricsConnector(meter_provider=meter_provider)
        tracer_provider.add_span_processor(self._processor)
