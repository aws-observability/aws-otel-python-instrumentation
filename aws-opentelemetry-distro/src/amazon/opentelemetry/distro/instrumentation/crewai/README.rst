AWS Distro for OpenTelemetry CrewAI Instrumentation
====================================================

This instrumentation traces applications built with `CrewAI <https://www.crewai.com/>`_
and emits telemetry that follows OpenTelemetry's Generative AI semantic
conventions.

Features
--------

* Creates spans for CrewAI crews, workflows, tasks, tools, and model calls.
* Records model, token usage, message, tool, and operation attributes when they
  are available from CrewAI events.

Installation
------------

Install the ADOT distro and a supported CrewAI version:

.. code-block:: console

    pip install aws-opentelemetry-distro "crewai>=1.10.0"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when CrewAI is installed:

.. code-block:: console

    opentelemetry-instrument python app.py

No application tracing code or CrewAI callback registration is required.

Disable the instrumentation
---------------------------

Add ``aws_crewai`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before starting
the application. Include any other disabled instrumentations in the same
comma-separated value:

.. code-block:: console

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_crewai
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry generative AI semantic conventions <https://opentelemetry.io/docs/specs/semconv/gen-ai/>`_
* `CrewAI documentation <https://docs.crewai.com/>`_
