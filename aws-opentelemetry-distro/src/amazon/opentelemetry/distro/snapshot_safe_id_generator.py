# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import INVALID_SPAN_ID, INVALID_TRACE_ID


class SnapshotSafeIdGenerator(RandomIdGenerator):
    """Generates random trace IDs and span IDs from ``os.urandom``, like the upstream ``RandomIdGenerator``.

    The upstream ``RandomIdGenerator`` draws from Python's ``random`` module, whose state lives in process memory.
    Platforms that start processes by restoring a memory snapshot, such as Amazon Bedrock AgentCore Runtime,
    restore that state too, so every restored process generates the same IDs. ``os.urandom`` reads from the
    operating system on every call and keeps no state in the process, so restored processes get distinct IDs.
    """

    # pylint: disable=no-self-use
    def generate_span_id(self) -> int:
        span_id = INVALID_SPAN_ID
        while span_id == INVALID_SPAN_ID:
            span_id = int.from_bytes(os.urandom(8), "big")
        return span_id

    # pylint: disable=no-self-use
    def generate_trace_id(self) -> int:
        trace_id = INVALID_TRACE_ID
        while trace_id == INVALID_TRACE_ID:
            trace_id = int.from_bytes(os.urandom(16), "big")
        return trace_id
