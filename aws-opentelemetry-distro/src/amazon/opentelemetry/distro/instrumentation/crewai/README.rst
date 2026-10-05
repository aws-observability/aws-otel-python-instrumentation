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

Sensitive data
--------------

.. admonition:: WARNING!
    :class: warning

    This instrumentation may capture sensitive data like task prompts, model
    responses, system instructions, agent goals and backstories, and tool
    arguments and results.

The following attributes may contain sensitive data:

* ``gen_ai.input.messages``
* ``gen_ai.output.messages``
* ``gen_ai.system_instructions``
* ``gen_ai.agent.description``
* ``gen_ai.tool.call.arguments``
* ``gen_ai.tool.call.result``
* ``gen_ai.tool.definitions``
* ``gen_ai.tool.description``

Set the following environment variable to redact these attributes:

::

    export AWS_REDACT_SPAN_ATTRIBUTES='gen_ai.input.messages,gen_ai.output.messages,gen_ai.system_instructions,gen_ai.agent.description,gen_ai.tool.call.arguments,gen_ai.tool.call.result,gen_ai.tool.definitions,gen_ai.tool.description'

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
