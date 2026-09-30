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

Install the distribution and supported LlamaIndex core and workflows versions:

::

    pip install aws-opentelemetry-distro \
        "llama-index-core>=0.13.0,<1" \
        "llama-index-workflows>=1.0.1,!=2.24.0,<3"

Known issue
-----------

``llama-index-workflows==2.24.0`` is not supported. That release can fail while
initializing LlamaIndex agents with
``TypeError: unhashable type: 'FunctionAgent'`` before any model or tool runs.
Because execution stops before those operations occur, the expected model,
tool, and downstream spans are not created. Install a workflows version
matching ``>=1.0.1,!=2.24.0,<3``.

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when LlamaIndex is installed:

::

    opentelemetry-instrument python app.py

No application tracing code or LlamaIndex handler registration is required.

Disable the instrumentation
---------------------------

Add ``aws_llama-index`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

::

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_llama-index
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry GenAI agent spans semantic conventions <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md>`_
* `LlamaIndex documentation <https://docs.llamaindex.ai/>`_
