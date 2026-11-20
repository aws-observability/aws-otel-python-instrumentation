# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time

from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import INVALID_SPAN_ID, INVALID_TRACE_ID


class SnapshotSafeIdGenerator(IdGenerator):
    """Generates X-Ray compatible trace IDs and span IDs from ``os.urandom``.

    The default ID generators draw from Python's ``random`` module, whose state lives in process memory.
    Platforms that start processes by restoring a memory snapshot, such as Amazon Bedrock AgentCore Runtime,
    restore that state too, so every restored process generates the same IDs. ``os.urandom`` reads from the
    operating system on every call and keeps no state in the process, so restored processes get distinct IDs.

    Trace IDs keep the X-Ray format: the first 32 bits are the start time in epoch seconds.
    """

    def generate_span_id(self) -> int:
        span_id = INVALID_SPAN_ID
        while span_id == INVALID_SPAN_ID:
            span_id = int.from_bytes(os.urandom(8), "big")
        return span_id

    def generate_trace_id(self) -> int:
        trace_time = int(time.time())
        trace_identifier = INVALID_TRACE_ID
        while trace_identifier == INVALID_TRACE_ID:
            trace_identifier = int.from_bytes(os.urandom(12), "big")
        return (trace_time << 96) + trace_identifier
