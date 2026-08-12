# cloudwatch-plugin-otel

Amazon CloudWatch plugin for OpenTelemetry. The package derives call-count and
duration metrics from spans produced by the active OpenTelemetry tracer
pipeline.

## Span metrics

The span metrics instrumentor registers a span processor that emits:

- `traces.span.metrics.calls`, a counter incremented once per ended span.
- `traces.span.metrics.duration`, a histogram of span duration in seconds.

Both metrics use the `cloudwatch.plugin.otel.span_metrics` instrumentation
scope. They include `service.name`, `span.name`, `span.kind`, `status.code`, and
supported HTTP, RPC, database, messaging, and error semantic-convention
attributes when those attributes are present on the span.

The instrumentor also wraps the active root sampler. A `DROP` decision becomes
`RECORD_ONLY`, allowing the span processor to derive metrics without changing
which spans are exported.

## Usage

Installing this package registers `SpanMetricsInstrumentor` as an
`opentelemetry_instrumentor` entry point. It is loaded automatically when the
application runs with OpenTelemetry auto-instrumentation:

```shell
opentelemetry-instrument python app.py
```

It can also be enabled programmatically:

```python
from plugins.opentelemetry.cloudwatch import SpanMetricsInstrumentor

SpanMetricsInstrumentor().instrument()
```

Pass `tracer_provider` or `meter_provider` to `instrument()` when using
application-managed providers:

```python
SpanMetricsInstrumentor().instrument(
    tracer_provider=tracer_provider,
    meter_provider=meter_provider,
)
```

The connector binds its meter when it is created. Configure the global
`MeterProvider`, or pass `meter_provider`, before instrumenting so the derived
metrics are recorded.

## License

This project is licensed under the Apache-2.0 License. See [LICENSE](./LICENSE).
