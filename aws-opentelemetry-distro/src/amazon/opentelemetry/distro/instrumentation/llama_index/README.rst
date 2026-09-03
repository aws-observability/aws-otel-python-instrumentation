AWS Distro for OpenTelemetry LlamaIndex Instrumentation
========================================================

This instrumentation traces applications built with
`LlamaIndex <https://www.llamaindex.ai/>`_ and emits telemetry that follows
OpenTelemetry's Generative AI semantic conventions.

Features
--------

* Creates spans from LlamaIndex dispatcher span and event handlers.
* Traces agent workflows, model calls, embeddings, retrieval operations, and
  tools.
* Records model, token usage, message, tool, and operation attributes when they
  are available from LlamaIndex.

Installation
------------

Install the ADOT distro and a supported LlamaIndex core version:

.. code-block:: console

    pip install aws-opentelemetry-distro "llama-index-core>=0.13.0"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when LlamaIndex is installed:

.. code-block:: console

    opentelemetry-instrument python app.py

No application tracing code or LlamaIndex handler registration is required.

Disable the instrumentation
---------------------------

Add ``aws_llama-index`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

.. code-block:: console

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_llama-index
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry generative AI semantic conventions <https://opentelemetry.io/docs/specs/semconv/gen-ai/>`_
* `LlamaIndex documentation <https://docs.llamaindex.ai/>`_
