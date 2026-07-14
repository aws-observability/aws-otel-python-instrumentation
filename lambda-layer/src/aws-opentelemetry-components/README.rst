AWS OpenTelemetry Components
============================

Installation
------------

::

    pip install aws-opentelemetry-components


Shared components for the AWS Distro for OpenTelemetry Python, bundled into a single
distribution. Consumed by ``aws-opentelemetry-distro``.

This package provides two component sets:

Application Signals
    Span processors, sampler, and metric-attribute generation used to derive
    service/dependency metrics and propagate metric-correlation attributes
    (``amazon.opentelemetry.application_signals``).

ServiceEvents
    Deep observability instrumentation including function-level invocation metrics,
    HTTP endpoint performance tracking, and automated error investigation on failures.
    Registers as an OpenTelemetry instrumentor
    (``amazon.opentelemetry.serviceevents``).

References
----------

* `OpenTelemetry Project <https://opentelemetry.io/>`_
* `AWS Distro for OpenTelemetry <https://aws-otel.github.io/>`_
