# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import ContextVar
from dataclasses import dataclass
from typing import ClassVar, Optional

from opentelemetry.semconv.attributes.network_attributes import NetworkTransportValues


@dataclass
class McpClientTransportMetadata:
    # Stores metadata information about the MCP transport info into local context which
    # is only available when the MCP client connects and is used later when creating spans
    # for individual MCP operations.

    _context: ClassVar[ContextVar[Optional["McpClientTransportMetadata"]]] = ContextVar(
        "MCP_CLIENT_TRANSPORT_CONTEXT", default=None
    )

    transport: NetworkTransportValues
    server_address: Optional[str] = None
    server_port: Optional[int] = None
    session_id: Optional[str] = None

    @classmethod
    def get(cls) -> Optional["McpClientTransportMetadata"]:
        return cls._context.get()

    @classmethod
    def set(cls, info: "McpClientTransportMetadata") -> None:
        cls._context.set(info)

    @classmethod
    def clear(cls) -> None:
        cls._context.set(None)
