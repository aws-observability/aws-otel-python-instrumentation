AWS Distro For OpenTelemetry Python Distro
============================================

Installation
------------

::

    pip install aws-opentelemetry-distro


This package provides Amazon Web Services distribution of the OpenTelemetry Python Instrumentation, which allows for auto-instrumentation of Python applications.

SigV4 exporter entry points
--------------------------

Select ``otlp/sigv4`` independently for logs, metrics, and traces to use the
existing AWS OTLP HTTP exporters through OpenTelemetry exporter discovery::

    pip install 'aws-opentelemetry-distro[patch]'
    export OTEL_LOGS_EXPORTER=otlp/sigv4
    export OTEL_METRICS_EXPORTER=otlp/sigv4
    export OTEL_TRACES_EXPORTER=otlp/sigv4
    export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://logs.us-west-2.amazonaws.com/v1/logs
    export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=https://monitoring.us-west-2.amazonaws.com/v1/metrics
    export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://xray.us-west-2.amazonaws.com/v1/traces
    export OTEL_EXPORTER_OTLP_LOGS_HEADERS=x-aws-log-group=my-group,x-aws-log-stream=my-stream

These entry points always use HTTP/protobuf. Signing services default to ``logs``,
``monitoring``, and ``xray`` for logs, metrics, and traces, respectively. Override
them with these optional environment variables::

    export OTEL_EXPORTER_OTLP_SIGV4_SERVICE={service_name}
    export OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE={service_name}
    export OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE={service_name}
    export OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE={service_name}

Each signal-specific service overrides the shared ``OTEL_EXPORTER_OTLP_SIGV4_SERVICE``;
when both are unset, the signal's default applies. An explicit ``service`` constructor
argument takes precedence over the environment.

Credentials come from botocore's standard credential chain. All constructor arguments
are optional. The signing region uses an explicit ``aws_region`` first, then ``AWS_REGION`` or
``AWS_DEFAULT_REGION``, then the configured AWS endpoint's region, and finally
the AWS profile/session region. An explicit ``session`` is preserved; otherwise
one is created from the AWS environment/profile. Endpoints, headers, timeouts,
and TLS certificates use their standard signal-specific environment variables,
falling back to generic OTLP settings when omitted.
For Python logging auto-instrumentation, also set
``OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true``.

References
----------

* `OpenTelemetry Project <https://opentelemetry.io/>`_
* `Example using opentelemetry-distro <https://opentelemetry.io/docs/instrumentation/python/distro/>`_
