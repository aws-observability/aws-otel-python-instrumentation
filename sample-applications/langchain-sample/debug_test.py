#!/usr/bin/env python3
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"The weather in {city} is 72°F and sunny."


llm = ChatBedrock(model_id="anthropic.claude-3-haiku-20240307-v1:0", region_name="us-west-2")
agent = create_react_agent(llm, [get_weather])
response = agent.invoke({"messages": [HumanMessage(content="What is the weather in Paris?")]})
print(response)
