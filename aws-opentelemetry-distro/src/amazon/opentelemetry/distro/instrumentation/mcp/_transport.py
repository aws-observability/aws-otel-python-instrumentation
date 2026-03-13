# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

from opentelemetry.context import Context
from opentelemetry.semconv.attributes.network_attributes import NetworkTransportValues

_MCP_CLIENT_TRANSPORT_CONTEXT = "MCP_CLIENT_TRANSPORT_CONTEXT"
_MCP_PARENT_CONTEXT = "MCP_PARENT_CONTEXT"


@dataclass
class ClientTransportMetadata:
    # Stores metadata information about the MCP transport info into local context which
    # is only available when the MCP client connects and is used later when creating spans
    # for individual MCP operations.

    transport: NetworkTransportValues
    server_address: Optional[str] = None
    server_port: Optional[int] = None
    session_id: Optional[str] = None


# Transport metadata is set when the MCP client connects but needed later when creating spans.
_client_transport_context: ContextVar[Optional[ClientTransportMetadata]] = ContextVar(
    _MCP_CLIENT_TRANSPORT_CONTEXT, default=None
)

# Parent context references the mcp.session that span is created in the transport wrapper,
# but it exits scope before send_request is called. Without storing the context, the session span's context
# would be lost and operation spans would have no parent. We preserve the session span's context so operation spans
# created later in wrap_session_send can use it as their parent.
_parent_context: ContextVar[Optional[Context]] = ContextVar(_MCP_PARENT_CONTEXT, default=None)


def get_transport_info() -> Optional[ClientTransportMetadata]:
    return _client_transport_context.get()


def set_transport_info(info: ClientTransportMetadata) -> None:
    _client_transport_context.set(info)


def clear_transport_info() -> None:
    _client_transport_context.set(None)


def get_parent_context() -> Optional[Context]:
    return _parent_context.get()


def set_parent_context(ctx: Context) -> None:
    _parent_context.set(ctx)


def clear_parent_context() -> None:
    _parent_context.set(None)
