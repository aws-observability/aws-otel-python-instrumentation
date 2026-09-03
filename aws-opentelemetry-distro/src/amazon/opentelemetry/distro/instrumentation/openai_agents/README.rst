AWS Distro for OpenTelemetry OpenAI Agents Instrumentation
===========================================================

This instrumentation traces applications built with the
`OpenAI Agents SDK <https://github.com/openai/openai-agents-python>`_ and emits
telemetry that follows OpenTelemetry's Generative AI semantic conventions.

Features
--------

* Creates spans for agents, model generations, tools, guardrails, and handoffs.
* Records model, token usage, message, tool, and operation attributes when they
  are available from the Agents SDK.

Installation
------------

Install the ADOT distro and a supported OpenAI Agents SDK version:

.. code-block:: console

    pip install aws-opentelemetry-distro "openai-agents>=0.3.3"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when the OpenAI Agents SDK is installed:

.. code-block:: console

    opentelemetry-instrument python app.py

No application tracing code or Agents SDK trace processor registration is
required.

Disable the instrumentation
---------------------------

Add ``aws_openai_agents`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

.. code-block:: console

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_openai_agents
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry generative AI semantic conventions <https://opentelemetry.io/docs/specs/semconv/gen-ai/>`_
* `OpenAI Agents SDK documentation <https://openai.github.io/openai-agents-python/>`_
