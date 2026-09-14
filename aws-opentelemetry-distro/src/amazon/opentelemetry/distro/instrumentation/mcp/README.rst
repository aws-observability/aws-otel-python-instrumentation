AWS Distro for OpenTelemetry MCP Instrumentation
=================================================

This instrumentation traces client and server operations performed with the
`Model Context Protocol Python SDK <https://github.com/modelcontextprotocol/python-sdk>`_
and emits telemetry that follows OpenTelemetry's semantic conventions.

This instrumentation supports version 1.x of the MCP Python SDK. MCP Python SDK
2.x is not currently supported.

Features
--------

* Creates spans for MCP client and server requests and notifications.
* Propagates OpenTelemetry context across stdio, SSE, and streamable HTTP
  transports.

Installation
------------

Install the distribution and a supported MCP SDK version:

::

    pip install aws-opentelemetry-distro "mcp>=1.10.0,<2"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when the MCP SDK is installed:

::

    opentelemetry-instrument python app.py

No application tracing code or MCP hook registration is required.

Configuration
-------------

MCP instrumentation suppresses HTTP client and ASGI spans by default. Set
``AWS_INSTRUMENTATION_MCP_SUPPRESS_HTTP_INSTRUMENTATION=false`` to retain
those spans:

::

    export AWS_INSTRUMENTATION_MCP_SUPPRESS_HTTP_INSTRUMENTATION=false

.. pull-quote::

    **Note**

    ``OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION`` is the legacy environment
    variable name and remains supported as a fallback when
    ``AWS_INSTRUMENTATION_MCP_SUPPRESS_HTTP_INSTRUMENTATION`` is not set.

Disable the instrumentation
---------------------------

Add ``aws_mcp`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before starting the
application. Include any other disabled instrumentations in the same
comma-separated value:

::

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_mcp
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry MCP semantic conventions <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/mcp.md>`_
* `Model Context Protocol documentation <https://modelcontextprotocol.io/>`_
