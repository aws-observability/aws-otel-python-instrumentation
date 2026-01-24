# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Utility module holding attribute keys for incubating Gen AI semantic conventions
not yet available in opentelemetry-semantic-conventions 0.54b1.
Remove attributes once they are contributed to upstream.
"""

GEN_AI_RUNTIME_ID = "gen_ai.runtime.id"
GEN_AI_BROWSER_ID = "gen_ai.browser.id"
GEN_AI_CODE_INTERPRETER_ID = "gen_ai.code_interpreter.id"
GEN_AI_MEMORY_ID = "gen_ai.memory.id"
GEN_AI_GATEWAY_ID = "gen_ai.gateway.id"

# GenAI Agent attributes from OTel semantic conventions spec v1.39.0
# https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-agent-spans/
GEN_AI_PROVIDER_NAME = "gen_ai.provider.name"
GEN_AI_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT = "gen_ai.tool.call.result"
GEN_AI_TOOL_DEFINITIONS = "gen_ai.tool.definitions"
GEN_AI_SYSTEM_INSTRUCTIONS = "gen_ai.system_instructions"

# GenAI operation name values
GEN_AI_OPERATION_CHAT = "chat"
GEN_AI_OPERATION_TEXT_COMPLETION = "text_completion"
GEN_AI_OPERATION_INVOKE_AGENT = "invoke_agent"
GEN_AI_OPERATION_EXECUTE_TOOL = "execute_tool"
