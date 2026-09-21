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

Install the distribution and a supported LangChain version:

::

    pip install aws-opentelemetry-distro "langchain>=0.3.21,<2"

Usage
-----

The instrumentation is registered with OpenTelemetry Python auto-instrumentation
and is loaded when LangChain is installed:

::

    opentelemetry-instrument python app.py

Configuration
-------------

Third-party tracing
~~~~~~~~~~~~~~~~~~~

We recommend setting ``LANGFUSE_TRACING_ENABLED=false`` if you are using
Langfuse with LangChain. This turns off Langfuse tracing so the two
integrations do not produce duplicate or conflicting telemetry.

::

    export LANGFUSE_TRACING_ENABLED=false

LangGraph span classification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you build an agent with LangGraph's low-level orchestration framework, you
can customize how the instrumentation represents the graph in telemetry. By
default, each ``StateGraph`` execution is recorded as an agent span. Use these
LangGraph configuration values to change or refine that classification:

* ``otel_agent_span=True`` explicitly marks the graph as an agent.
* ``otel_workflow_span=True`` marks the graph as a workflow and overrides the
  default agent classification.
* ``agent_name`` sets the agent name and marks the graph as an agent.
* ``agent_type`` marks the graph as an agent.
* ``run_name`` names an automatically classified LangGraph agent span. A
  ``run_name`` passed when invoking the graph takes precedence over a name in
  the graph configuration or compiled graph.

Setting ``otel_agent_span=False`` does not turn off the default ``StateGraph``
agent classification.

This example builds and invokes a ``StateGraph`` using LangGraph's documented
pattern. Pass the instrumentation settings in the ``metadata`` field of the
LangChain ``RunnableConfig`` supplied to ``graph.invoke``:

::

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.graph import END, START, MessagesState, StateGraph


    def respond(state: MessagesState):
        user_message = state["messages"][-1]
        return {
            "messages": [
                AIMessage(content=f"Received: {user_message.content}")
            ]
        }


    builder = StateGraph(MessagesState)
    builder.add_node("respond", respond)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)

    graph = builder.compile(name="SupportGraph")
    result = graph.invoke(
        {"messages": [HumanMessage(content="Hello")]},
        config={
            "run_name": "SupportGraphRun",
            "metadata": {
                "otel_agent_span": True,
                "agent_name": "SupportAgent",
                "agent_type": "support",
            },
        },
    )

    print(result["messages"][-1].content)

To record the graph as a workflow, set ``otel_workflow_span=True`` in
``metadata`` and omit ``otel_agent_span``, ``agent_name``, and ``agent_type``.
The workflow setting overrides the default agent classification.

Disable the instrumentation
---------------------------

Add ``aws_langchain`` to ``OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`` before
starting the application. Include any other disabled instrumentations in the
same comma-separated value:

::

    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_langchain
    opentelemetry-instrument python app.py

References
----------

* `OpenTelemetry GenAI agent spans semantic conventions <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md>`_
* `LangGraph Graph API <https://docs.langchain.com/oss/python/langgraph/use-graph-api>`_
* `LangChain RunnableConfig <https://python.langchain.com/api_reference/core/runnables/langchain_core.runnables.config.RunnableConfig.html>`_
