AWS Distro for OpenTelemetry MCP Instrumentation
=================================================

This instrumentation traces client and server operations performed with the
`Model Context Protocol Python SDK <https://github.com/modelcontextprotocol/python-sdk>`_
and emits telemetry that follows OpenTelemetry's semantic conventions.

Features
--------

* Creates spans for MCP client and server requests and notifications.
* Propagates OpenTelemetry context across stdio, SSE, and streamable HTTP
  transports.

Installation
------------

Install the ADOT distro and a supported MCP SDK version:

.. code-block:: console

    pip install aws-opentelemetry-distro "mcp>=1.10.0"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when the MCP SDK is installed:

.. code-block:: console

    opentelemetry-instrument python app.py

No application tracing code or MCP hook registration is required.

Configuration
-------------

MCP instrumentation suppresses redundant HTTP client and ASGI spans by default.
Set ``OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION=false`` to retain those spans:

.. code-block:: console

    export OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION=false

Disable the instrumentation
---------------------------

Add ``aws_mcp`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before starting the
application. Include any other disabled instrumentations in the same
comma-separated value:

.. code-block:: console

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_mcp
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry trace semantic conventions <https://opentelemetry.io/docs/specs/semconv/general/trace/>`_
* `Model Context Protocol documentation <https://modelcontextprotocol.io/>`_
