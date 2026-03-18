"""
Comprehensive test to verify CrewAI correlation keys.
"""

import os
os.environ['CREWAI_DISABLE_TELEMETRY'] = 'true'
os.environ['OPENAI_API_KEY'] = 'fake-key'

# Initialize instrumentation FIRST
from amazon.opentelemetry.distro.instrumentation.crewai import CrewAIInstrumentor
instrumentor = CrewAIInstrumentor()
instrumentor.instrument()

from unittest.mock import patch
from crewai import LLM, Agent, Crew, Task
from crewai.tools import tool
import json
import time
from openai.types.chat import ChatCompletion

from amazon.opentelemetry.distro.instrumentation.crewai._event_handler import OpenTelemetryEventHandler
from crewai.events.types.crew_events import CrewKickoffStartedEvent, CrewKickoffCompletedEvent
from crewai.events.types.agent_events import AgentExecutionStartedEvent, AgentExecutionCompletedEvent
from crewai.events.types.tool_usage_events import ToolUsageStartedEvent, ToolUsageFinishedEvent
from crewai.events.types.llm_events import LLMCallStartedEvent, LLMCallCompletedEvent, LLMCallFailedEvent

events_log = []

orig = OpenTelemetryEventHandler._handle_event
def logging_handle(self, source, event):
    event_name = type(event).__name__
    
    # Compute keys based on proposed schema
    if isinstance(event, (CrewKickoffStartedEvent, CrewKickoffCompletedEvent)):
        key = ('crew', id(source))
    elif isinstance(event, (AgentExecutionStartedEvent, AgentExecutionCompletedEvent)):
        task_id = getattr(event, 'task_id', None)
        key = ('agent', id(source), task_id)
    elif isinstance(event, (ToolUsageStartedEvent, ToolUsageFinishedEvent)):
        tool_name = getattr(event, 'tool_name', None)
        key = ('tool', id(source), tool_name)
    elif isinstance(event, (LLMCallStartedEvent, LLMCallCompletedEvent, LLMCallFailedEvent)):
        call_id = getattr(event, 'call_id', None)
        key = ('llm', id(source), call_id)
    else:
        key = ('other', id(source))
    
    events_log.append({'event_name': event_name, 'key': key, 'event': event})
    print(f'{event_name:40s} key={key}')
    
    if len(key) > 2 and key[2] is None:
        print(f'  ⚠️  WARNING: Key field is None!')
    
    return orig(self, source, event)

OpenTelemetryEventHandler._handle_event = logging_handle

def make_response(content='', tool_calls=None):
    return ChatCompletion(
        id='chatcmpl-mock',
        object='chat.completion',
        created=int(time.time()),
        model='gpt-4',
        choices=[{
            'index': 0,
            'message': {'role': 'assistant', 'content': content, 'tool_calls': tool_calls or []},
            'finish_reason': 'tool_calls' if tool_calls else 'stop',
            'logprobs': None
        }],
        usage={'prompt_tokens': 10, 'completion_tokens': 20, 'total_tokens': 30}
    )

def make_tool_call(call_id, name, args):
    return {'id': call_id, 'type': 'function', 'function': {'name': name, 'arguments': args}}

print("="*80)
print("SCENARIO 1: Single agent, no tools")
print("="*80)
events_log = []
with patch('openai.resources.chat.completions.Completions.create') as mock:
    mock.return_value = make_response('Final Answer: Done')
    llm = LLM(model='openai/gpt-4', max_retry_limit=0)
    agent = Agent(role='Worker', goal='Work', backstory='Worker', llm=llm)
    task = Task(description='Work', expected_output='Done', agent=agent)
    Crew(agents=[agent], tasks=[task]).kickoff()

print("\nVerifying pairs:")
agent_events = [e for e in events_log if e['key'][0] == 'agent']
if agent_events:
    starts = [e for e in agent_events if 'Started' in e['event_name']]
    ends = [e for e in agent_events if 'Completed' in e['event_name']]
    for s in starts:
        matches = [e for e in ends if e['key'] == s['key']]
        if matches:
            print(f"  ✓ {s['event_name']} <-> {matches[0]['event_name']}")
        else:
            print(f"  ✗ {s['event_name']} NO MATCH | key={s['key']}")

llm_events = [e for e in events_log if e['key'][0] == 'llm']
if llm_events:
    starts = [e for e in llm_events if 'Started' in e['event_name']]
    ends = [e for e in llm_events if 'Completed' in e['event_name'] or 'Failed' in e['event_name']]
    for s in starts:
        matches = [e for e in ends if e['key'] == s['key']]
        if matches:
            print(f"  ✓ {s['event_name']} <-> {matches[0]['event_name']}")
        else:
            print(f"  ✗ {s['event_name']} NO MATCH | key={s['key']}")

print("\n" + "="*80)
print("SCENARIO 2: Single agent with tool")
print("="*80)
events_log = []

@tool
def my_tool(x: str) -> str:
    """A tool."""
    return f"Result: {x}"

with patch('openai.resources.chat.completions.Completions.create') as mock:
    mock.side_effect = [
        make_response('', [make_tool_call('call_1', 'my_tool', json.dumps({"x":"test"}))]),
        make_response('Final Answer: Done')
    ]
    llm = LLM(model='openai/gpt-4', max_retry_limit=0)
    agent = Agent(role='Worker', goal='Work', backstory='Worker', llm=llm, tools=[my_tool])
    task = Task(description='Work', expected_output='Done', agent=agent)
    Crew(agents=[agent], tasks=[task]).kickoff()

print("\nVerifying pairs:")
tool_events = [e for e in events_log if e['key'][0] == 'tool']
if tool_events:
    starts = [e for e in tool_events if 'Started' in e['event_name']]
    ends = [e for e in tool_events if 'Finished' in e['event_name']]
    for s in starts:
        matches = [e for e in ends if e['key'] == s['key']]
        if matches:
            print(f"  ✓ {s['event_name']} <-> {matches[0]['event_name']}")
        else:
            print(f"  ✗ {s['event_name']} NO MATCH | key={s['key']}")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("Check the output above for:")
print("1. Any WARNING about None key fields")
print("2. Any MISMATCH between start and end events")
print("3. Verify agent events have task_id populated")
print("4. Verify tool events have tool_name populated")
print("5. Verify LLM events have call_id populated")
