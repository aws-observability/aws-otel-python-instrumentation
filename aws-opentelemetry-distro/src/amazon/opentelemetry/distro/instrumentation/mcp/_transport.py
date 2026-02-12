# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from opentelemetry.context import Context

_MCP_TRANSPORT_CONTEXT = "mcp_transport_context"
_MCP_PARENT_CONTEXT = "mcp_parent_context"


class TransportType(str, Enum):
    PIPE = "pipe"
    TCP = "tcp"


@dataclass
class TransportInfo:
    """Transport information for MCP connections."""

    transport: TransportType
    server_address: Optional[str] = None
    server_port: Optional[int] = None
    session_id: Optional[str] = None


_transport_context: ContextVar[Optional[TransportInfo]] = ContextVar(_MCP_TRANSPORT_CONTEXT, default=None)
_parent_context: ContextVar[Optional[Context]] = ContextVar(_MCP_PARENT_CONTEXT, default=None)


def get_transport_info() -> Optional[TransportInfo]:
    return _transport_context.get()


def set_transport_info(info: TransportInfo) -> None:
    _transport_context.set(info)


def clear_transport_info() -> None:
    _transport_context.set(None)


def get_parent_context() -> Optional[Context]:
    return _parent_context.get()


def set_parent_context(ctx: Context) -> None:
    _parent_context.set(ctx)


def clear_parent_context() -> None:
    _parent_context.set(None)
