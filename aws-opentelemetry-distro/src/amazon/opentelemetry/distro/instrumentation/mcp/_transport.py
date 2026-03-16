# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

from opentelemetry.semconv.attributes.network_attributes import NetworkTransportValues

_MCP_CLIENT_TRANSPORT_CONTEXT = "MCP_CLIENT_TRANSPORT_CONTEXT"


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


def get_transport_info() -> Optional[ClientTransportMetadata]:
    return _client_transport_context.get()


def set_transport_info(info: ClientTransportMetadata) -> None:
    _client_transport_context.set(info)


def clear_transport_info() -> None:
    _client_transport_context.set(None)
