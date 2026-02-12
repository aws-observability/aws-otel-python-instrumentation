# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

import uvicorn
from langchain.agents import create_agent
from langchain_aws import ChatBedrock
from langchain_core.tools import tool
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"The weather in {city} is 72°F and sunny."


@tool
def add_numbers(a: int, b: int) -> int:
    """Add two numbers together."""
    return a + b


llm = ChatBedrock(
    model_id="anthropic.claude-3-haiku-20240307-v1:0",
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
)
tools = [get_weather, add_numbers]
agent = create_agent(llm, tools, name="WeatherMathAgent")


async def chat(request: Request):
    data = await request.json()
    message = data.get("message", "Hello")

    response = agent.invoke({"messages": [("human", message)]})
    return JSONResponse({"response": response["messages"][-1].content})


async def health(request: Request):
    return JSONResponse({"status": "healthy"})


app = Starlette(
    routes=[
        Route("/chat", chat, methods=["POST"]),
        Route("/health", health, methods=["GET"]),
    ]
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
