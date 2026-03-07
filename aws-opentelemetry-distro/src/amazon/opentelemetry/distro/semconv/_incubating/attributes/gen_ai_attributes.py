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

# MCP Semantic Conventions v1.39.0
# https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/
MCP_METHOD_NAME = "mcp.method.name"
MCP_PROTOCOL_VERSION = "mcp.protocol.version"
MCP_RESOURCE_URI = "mcp.resource.uri"
MCP_SESSION_ID = "mcp.session.id"
JSONRPC_REQUEST_ID = "jsonrpc.request.id"
JSONRPC_PROTOCOL_VERSION = "jsonrpc.protocol.version"
RPC_RESPONSE_STATUS_CODE = "rpc.response.status_code"


class MCPMethodValue:
    COMPLETION_COMPLETE = "completion/complete"
    ELICITATION_CREATE = "elicitation/create"
    INITIALIZE = "initialize"
    LOGGING_SET_LEVEL = "logging/setLevel"
    NOTIFICATIONS_CANCELLED = "notifications/cancelled"
    NOTIFICATIONS_INITIALIZED = "notifications/initialized"
    NOTIFICATIONS_MESSAGE = "notifications/message"
    NOTIFICATIONS_PROGRESS = "notifications/progress"
    NOTIFICATIONS_PROMPTS_LIST_CHANGED = "notifications/prompts/list_changed"
    NOTIFICATIONS_RESOURCES_LIST_CHANGED = "notifications/resources/list_changed"
    NOTIFICATIONS_RESOURCES_UPDATED = "notifications/resources/updated"
    NOTIFICATIONS_ROOTS_LIST_CHANGED = "notifications/roots/list_changed"
    NOTIFICATIONS_TOOLS_LIST_CHANGED = "notifications/tools/list_changed"
    PING = "ping"
    PROMPTS_GET = "prompts/get"
    PROMPTS_LIST = "prompts/list"
    RESOURCES_LIST = "resources/list"
    RESOURCES_READ = "resources/read"
    RESOURCES_SUBSCRIBE = "resources/subscribe"
    RESOURCES_TEMPLATES_LIST = "resources/templates/list"
    RESOURCES_UNSUBSCRIBE = "resources/unsubscribe"
    ROOTS_LIST = "roots/list"
    SAMPLING_CREATE_MESSAGE = "sampling/createMessage"
    TOOLS_CALL = "tools/call"
    TOOLS_LIST = "tools/list"
