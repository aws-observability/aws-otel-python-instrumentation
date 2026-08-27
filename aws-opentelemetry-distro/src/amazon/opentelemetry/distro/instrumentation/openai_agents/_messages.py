# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from pydantic import BaseModel

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import content_to_parts

_FINISH_REASON_MAP = {
    "end_turn": "stop",
    "stop": "stop",
    "tool_calls": "tool_call",
    "tool_use": "tool_call",
    "max_tokens": "length",
    "length": "length",
    "content_filter": "content_filter",
}


def _as_mapping(value: Any) -> Optional[dict[str, Any]]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, BaseModel):
        return value.model_dump()
    return None


def _as_items(items: Any) -> list[Any]:
    if items is None:
        return []
    if isinstance(items, (str, bytes, Mapping, BaseModel)):
        return [items]
    if isinstance(items, Sequence):
        return list(items)
    return [items]


def _prepare_content(content: Any) -> Any:
    if isinstance(content, (Mapping, BaseModel)):
        block = _as_mapping(content) or {}
        block_type = block.get("type")
        if block_type in ("input_text", "output_text", "summary_text"):
            return {"type": "text", "text": block.get("text", "")}
        return block
    if isinstance(content, Sequence) and not isinstance(content, (str, bytes)):
        return [_prepare_content(block) for block in content]
    return content


def _parts(content: Any) -> list[dict[str, Any]]:
    return content_to_parts(_prepare_content(content))


def _parse_arguments(arguments: Any) -> Any:
    if not isinstance(arguments, str):
        return arguments
    try:
        return json.loads(arguments)
    except (TypeError, ValueError):
        return arguments


def _tool_call_parts(tool_calls: Any) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    for raw_tool_call in _as_items(tool_calls):
        tool_call = _as_mapping(raw_tool_call) or {}
        function = _as_mapping(tool_call.get("function")) or {}
        part = {
            "type": "tool_call",
            "id": tool_call.get("id") or tool_call.get("call_id") or "",
            "name": function.get("name") or tool_call.get("name") or "",
            "arguments": _parse_arguments(function.get("arguments", tool_call.get("arguments", {}))),
        }
        parts.append(part)
    return parts


def _reasoning_parts(item: Mapping[str, Any]) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    for summary_item in _as_items(item.get("summary")):
        summary = _as_mapping(summary_item)
        content = summary.get("text") if summary else summary_item
        if content:
            parts.append({"type": "reasoning", "content": str(content)})
    if not parts and item.get("content"):
        parts.append({"type": "reasoning", "content": str(item["content"])})
    return parts


def _raw_finish_reason(item: Mapping[str, Any], has_tool_calls: bool) -> str:
    if has_tool_calls:
        return "tool_call"
    raw_reason = item.get("finish_reason") or item.get("stop_reason")
    if raw_reason is None:
        return "stop"
    return _FINISH_REASON_MAP.get(str(raw_reason), str(raw_reason))


def _normalize_message(  # pylint: disable=too-many-branches
    item: Any, output: bool
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    system_instructions: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []
    item_data = _as_mapping(item)

    if item_data is None:
        parts = _parts(item)
        if parts:
            message = {"role": "assistant" if output else "user", "parts": parts}
            if output:
                message["finish_reason"] = "stop"
            messages.append(message)
        return system_instructions, messages

    item_type = item_data.get("type")
    role = item_data.get("role")

    if item_type == "function_call":
        part = {
            "type": "tool_call",
            "id": item_data.get("call_id") or item_data.get("id") or "",
            "name": item_data.get("name") or "",
            "arguments": _parse_arguments(item_data.get("arguments", {})),
        }
        message = {"role": "assistant", "parts": [part]}
        if output:
            message["finish_reason"] = "tool_call"
        messages.append(message)
        return system_instructions, messages

    if item_type == "function_call_output":
        part = {
            "type": "tool_call_response",
            "id": item_data.get("call_id") or item_data.get("id") or "",
            "response": item_data.get("output", ""),
        }
        message = {"role": "tool", "parts": [part]}
        if output:
            message["finish_reason"] = "stop"
        messages.append(message)
        return system_instructions, messages

    if item_type == "reasoning":
        reasoning = _reasoning_parts(item_data)
        if reasoning:
            message = {"role": "assistant", "parts": reasoning}
            if output:
                message["finish_reason"] = _raw_finish_reason(item_data, False)
            messages.append(message)
        return system_instructions, messages

    parts: list[dict[str, Any]] = []
    if role == "tool":
        parts.append(
            {
                "type": "tool_call_response",
                "id": item_data.get("tool_call_id") or item_data.get("call_id") or "",
                "response": item_data.get("content", ""),
            }
        )
    else:
        parts.extend(_parts(item_data.get("content")))

    tool_call_parts = _tool_call_parts(item_data.get("tool_calls"))
    parts.extend(tool_call_parts)

    if role in ("system", "developer"):
        system_instructions.extend(parts)
        return system_instructions, messages

    if not role:
        role = "assistant" if output else "user"
    message = {"role": role, "parts": parts}
    if output:
        message["finish_reason"] = _raw_finish_reason(item_data, bool(tool_call_parts))
    messages.append(message)
    return system_instructions, messages


def normalize_input_messages(items: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize OpenAI Agents input payloads into GenAI semantic convention messages."""
    system_instructions: list[dict[str, Any]] = []
    conversation: list[dict[str, Any]] = []
    for item in _as_items(items):
        item_system, item_messages = _normalize_message(item, output=False)
        system_instructions.extend(item_system)
        conversation.extend(item_messages)
    return system_instructions, conversation


def normalize_output_messages(items: Any) -> list[dict[str, Any]]:
    """Normalize OpenAI Agents output payloads into GenAI semantic convention messages."""
    messages: list[dict[str, Any]] = []
    for item in _as_items(items):
        _, item_messages = _normalize_message(item, output=True)
        messages.extend(item_messages)
    return messages
