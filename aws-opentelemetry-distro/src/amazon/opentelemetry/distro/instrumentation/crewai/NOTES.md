# CrewAI Instrumentor Design Notes

## Wrapping targets and their CrewAI source references

### Crew.kickoff → crew_kickoff span
- https://github.com/crewAIInc/crewAI/blob/06d953bf46c636ff9f2d64f45574493d05fb7771/lib/crewai/src/crewai/crew.py#L676-L679
- The span name "crew_kickoff {crew_name}" does not conform to any current OTel semantic
  conventions. This is because CrewAI's workflow can contain multiple agents but there currently
  does not exist any semantic convention naming schema to capture this system.
- As of OTel semconv v1.39.0, there are no semantic conventions that support
  multi-agent systems. We intentionally do not set gen_ai.provider.name or
  gen_ai.request.model here because a Crew can contain multiple agents with different
  providers/models. Per-agent provider/model info is captured in child invoke_agent spans.
- TODO: Revisit span attributes when OTel semconv adds multi-agent system support.

### Task._execute_core → invoke_agent span
- https://github.com/crewAIInc/crewAI/blob/06d953bf46c636ff9f2d64f45574493d05fb7771/lib/crewai/src/crewai/task.py#L604-L608

### ToolUsage._use → execute_tool span (text-based tool calling)
- https://github.com/crewAIInc/crewAI/blob/main/lib/crewai/src/crewai/tools/tool_usage.py

### BaseTool.run / Tool.run → execute_tool span (LLM native tool calling)
- https://github.com/crewAIInc/crewAI/blob/main/lib/crewai/src/crewai/tools/base_tool.py
- As of 1.9.0 these need to be instrumented to handle LLM native tool calling.
- Tool class (@tool decorator) overrides BaseTool.run, so both must be wrapped separately.

### LLM.call → chat span
- https://github.com/crewAIInc/crewAI/blob/main/lib/crewai/src/crewai/llm.py
- Central method for all CrewAI LLM calls, regardless of provider (Bedrock, LiteLLM, Anthropic, etc.)
- Captures input/output messages and token usage that other wrappers don't have access to.

## Provider/model extraction
- LLM instance has .model and .provider attributes
- Model string format: "provider/model_name" (e.g. "bedrock/us.anthropic.claude-sonnet-4")
- PROVIDER_MAP maps provider prefixes to OTel semantic convention names

## Event bus approach (v2)
- CrewAI emits events via crewai_event_bus for all lifecycle stages
- Events have event_id (UUID4, unique per event) and started_event_id (links completed back to started)
- source parameter in event handlers is the originating object (Crew, Agent, LLM, etc.)
- Event pairs: Started/Completed/Failed for crew, agent, task, tool, llm
- Token usage available via source.get_token_usage_summary() (public API, returns UsageMetrics)
- LLMCallCompletedEvent has response, messages, model, call_type fields
