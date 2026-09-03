AWS Distro for OpenTelemetry LangChain Instrumentation
=======================================================

This instrumentation traces applications built with
`LangChain <https://www.langchain.com/>`_ and emits telemetry that follows
OpenTelemetry's Generative AI semantic conventions.

Features
--------

* Creates spans for chains, agents, model calls, and tools.
* Records model, token usage, message, tool, and operation attributes when they
  are available from LangChain callbacks.

Installation
------------

Install the ADOT distro and a supported LangChain version:

.. code-block:: console

    pip install aws-opentelemetry-distro "langchain>=0.3.21"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when LangChain is installed:

.. code-block:: console

    opentelemetry-instrument python app.py

No application tracing code or LangChain callback registration is required.

Disable the instrumentation
---------------------------

Add ``aws_langchain`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

.. code-block:: console

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_langchain
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry generative AI semantic conventions <https://opentelemetry.io/docs/specs/semconv/gen-ai/>`_
* `LangChain documentation <https://python.langchain.com/docs/>`_
