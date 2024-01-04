import os
from logging import getLogger
from typing import Optional
from opentelemetry import context as context_api
from opentelemetry.environment_variables import OTEL_TRACES_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_PYTHON_ID_GENERATOR, OTEL_PROPAGATORS
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_PROTOCOL
from opentelemetry.sdk.trace import TracerProvider, SpanProcessor, ReadableSpan, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import get_current_span, set_tracer_provider
from opentelemetry.instrumentation.distro import BaseDistro
logger = getLogger(__name__)

class RemoteAttributesSpanProcessor(SpanProcessor):
    def on_start(
            self,
            span: "Span",
            parent_context: Optional[context_api.Context] = None,
    ) -> None:
        """Called when a :class:`opentelemetry.trace.Span` is started.
        This method is called synchronously on the thread that starts the
        span, therefore it should not block or throw an exception.
        Args:
            span: The :class:`opentelemetry.trace.Span` that just started.
            parent_context: The parent context of the span that just started.
        """
        print("====================")
        span.set_attribute("aws.detect", "true")
        parent_span = get_current_span(parent_context)
        if not isinstance(parent_span, ReadableSpan):
            print("==================== Return")
            return
        # application = parent_span.attributes.get("aws.remote.application")
        # operation = parent_span.attributes.get("aws.remote.operation")
        application = parent_span.attributes.get("http.server_name")
        operation = parent_span.attributes.get("http.method")
        if application:
            print("==================== set aws.remote.application")
            span.set_attribute("aws.remote.application", application)
        if operation:
            print("==================== set aws.remote.operation")
            span.set_attribute("aws.remote.operation", operation)

class AWSTracerProvider(TracerProvider):
    def __init__(
            self
    ):
        super(AWSTracerProvider, self).__init__()
        self.add_span_processor(RemoteAttributesSpanProcessor())
        self.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

class AWSDistro(BaseDistro):
    def _configure(self, **kwargs):
        logger.info("configure tracing using aws")
        # set_tracer_provider(AWSTracerProvider())
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "console")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "console")
        os.environ.setdefault(OTEL_EXPORTER_OTLP_PROTOCOL, "grpc")
        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")
        os.environ.setdefault(OTEL_PROPAGATORS, "xray")