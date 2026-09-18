AWS Distro for OpenTelemetry OpenAI Agents Instrumentation
===========================================================

This instrumentation traces applications built with the
`OpenAI Agents SDK <https://github.com/openai/openai-agents-python>`_ and emits
telemetry that follows OpenTelemetry's Generative AI semantic conventions.

Features
--------

* Creates spans for agents, model generations, tools, and handoffs.
* Records model, token usage, message, tool, and operation attributes when they
  are available from the Agents SDK.

Installation
------------

Install the distribution and a supported OpenAI Agents SDK version:

::

    pip install aws-opentelemetry-distro "openai-agents>=0.3.3,<1"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when the OpenAI Agents SDK is installed:

::

    opentelemetry-instrument python app.py

No application tracing code or Agents SDK trace processor registration is
required.

Configuration
-------------

OpenAI Agents SDK tracing
~~~~~~~~~~~~~~~~~~~~~~~~~

This instrumentation uses the SDK's trace events to create OpenTelemetry spans.
Setting ``OPENAI_AGENTS_DISABLE_TRACING=true`` or passing
``RunConfig(tracing_disabled=True)`` disables those events and effectively
disables this instrumentation.

If you configure SDK tracing through the environment, leave it enabled:

::

    export OPENAI_AGENTS_DISABLE_TRACING=false

If your application supplies a ``RunConfig``, leave tracing enabled:

::

    from agents import RunConfig, Runner


    result = Runner.run_sync(
        agent,
        "Write a haiku about observability.",
        run_config=RunConfig(tracing_disabled=False),
    )

OpenAI trace export
~~~~~~~~~~~~~~~~~~~

Set ``AWS_INSTRUMENTATION_OPENAI_AGENTS_DISABLE_OPENAI_EXPORT=true`` to
disable exporting traces to the OpenAI backend while retaining this
instrumentation:

::

    export AWS_INSTRUMENTATION_OPENAI_AGENTS_DISABLE_OPENAI_EXPORT=true

Disable the instrumentation
---------------------------

Add ``aws_openai_agents`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

::

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_openai_agents
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry GenAI agent spans semantic conventions <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md>`_
* `OpenAI Agents SDK documentation <https://openai.github.io/openai-agents-python/>`_
