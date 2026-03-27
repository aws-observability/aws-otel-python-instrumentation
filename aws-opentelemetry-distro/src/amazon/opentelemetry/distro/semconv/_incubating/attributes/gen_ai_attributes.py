# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Utility module holding attribute keys for incubating Gen AI semantic conventions.
Remove this once we've contributed them to upstream.
"""

GEN_AI_RUNTIME_ID = "gen_ai.runtime.id"
GEN_AI_BROWSER_ID = "gen_ai.browser.id"
GEN_AI_CODE_INTERPRETER_ID = "gen_ai.code_interpreter.id"
GEN_AI_MEMORY_ID = "gen_ai.memory.id"
GEN_AI_GATEWAY_ID = "gen_ai.gateway.id"

# GenAI Agent attributes from OTel semantic conventions spec v1.39.0
# https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-agent-spans/
GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_PROVIDER_NAME = "gen_ai.provider.name"
GEN_AI_AGENT_ID = "gen_ai.agent.id"
GEN_AI_AGENT_NAME = "gen_ai.agent.name"
GEN_AI_AGENT_DESCRIPTION = "gen_ai.agent.description"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_REQUEST_TEMPERATURE = "gen_ai.request.temperature"
GEN_AI_REQUEST_MAX_TOKENS = "gen_ai.request.max_tokens"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_TOOL_DESCRIPTION = "gen_ai.tool.description"
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"
GEN_AI_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT = "gen_ai.tool.call.result"
GEN_AI_TOOL_DEFINITIONS = "gen_ai.tool.definitions"
GEN_AI_TOOL_TYPE = "gen_ai.tool.type"
GEN_AI_SYSTEM_INSTRUCTIONS = "gen_ai.system_instructions"
