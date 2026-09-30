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

Install the distribution and a supported CrewAI version:

::

    pip install aws-opentelemetry-distro "crewai>=1.10.0,<2"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when CrewAI is installed:

::

    opentelemetry-instrument python app.py

No application tracing code or CrewAI callback registration is required.

Configuration
-------------

We recommend setting ``CREWAI_DISABLE_TELEMETRY=true`` if you are using
CrewAI. This disables CrewAI's built-in telemetry and prevents conflicting
instrumentation or duplicate telemetry.

::

    export CREWAI_DISABLE_TELEMETRY=true

Disable the instrumentation
---------------------------

Add ``aws_crewai`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before starting
the application. Include any other disabled instrumentations in the same
comma-separated value:

::

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_crewai
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry GenAI agent spans semantic conventions <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md>`_
* `CrewAI documentation <https://docs.crewai.com/>`_
