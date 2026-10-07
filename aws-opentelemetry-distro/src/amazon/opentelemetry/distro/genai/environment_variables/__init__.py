# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Environment variable names shared by AWS GenAI instrumentation."""

AGENT_OBSERVABILITY_ENABLED = "AGENT_OBSERVABILITY_ENABLED"
"""
.. envvar:: AGENT_OBSERVABILITY_ENABLED

Set to ``true`` to apply the following environment variable defaults for agent
observability. Variables you have already configured are preserved. Default: ``false``.

* ``OTEL_TRACES_EXPORTER=otlp``
* ``OTEL_LOGS_EXPORTER=otlp``
* ``OTEL_METRICS_EXPORTER=awsemf``
* ``OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true``
* ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://xray.<region>.amazonaws.com/v1/traces``
* ``OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://logs.<region>.amazonaws.com/v1/logs``
* ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS``: ``http``, ``sqlalchemy``, ``psycopg2``,
  ``pymysql``, ``sqlite3``, ``aiopg``, ``asyncpg``, ``mysql_connector``, ``urllib3``,
  ``requests``, ``system_metrics``, ``google-genai``, ``jinja2`` (comma-separated).
* ``OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true``
* ``OTEL_PYTHON_LOG_CORRELATION=true``
* ``OTEL_AWS_APPLICATION_SIGNALS_ENABLED=false``
* ``OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS=false``
* ``CREWAI_DISABLE_TELEMETRY=true``

The trace and log endpoints are configured only when ``OTEL_EXPORTER_OTLP_ENDPOINT``
is not set and an AWS Region can be determined. Their DNS suffix follows the AWS partition.
"""

AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT = "AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT"
"""
.. envvar:: AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT

Setting this to ``true`` is recommended to keep captured content in span attributes.
Default: ``false``. Captured content is removed from span attributes and sent to a
separate logs pipeline. If that pipeline is disabled, the content is discarded.

.. warning::

    This variable will be deprecated in a future release. Captured content will remain
    in span attributes as ADOT aligns with the latest OTel GenAI semantic conventions.
"""

AWS_GENAI_INSTRUMENTATION = "AWS_GENAI_INSTRUMENTATION"
"""
.. envvar:: AWS_GENAI_INSTRUMENTATION

Controls AWS native agentic instrumentors when AGENT_OBSERVABILITY_ENABLED=true.
This switch only governs the aws_* side; third-party instrumentors are never disabled
by ADOT — uninstall them or use OTEL_PYTHON_DISABLED_INSTRUMENTATIONS to opt out.

Values:
    * ``auto`` (default, also when unset): load aws_* unless a same-library third-party is registered.
    * ``enabled``: load all aws_* unconditionally.
    * ``disabled``: skip all aws_*.
"""

AWS_AGENTIC_INSTRUMENTATION = "AWS_AGENTIC_INSTRUMENTATION"
"""
.. envvar:: AWS_AGENTIC_INSTRUMENTATION

.. warning::

    This variable is deprecated. Use :envvar:`AWS_GENAI_INSTRUMENTATION` instead.
"""
