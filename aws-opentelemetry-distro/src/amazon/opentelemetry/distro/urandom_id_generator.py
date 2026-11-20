# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from opentelemetry import trace
from opentelemetry.sdk.trace.id_generator import IdGenerator


class UrandomIdGenerator(IdGenerator):
    """ID generator that reads every trace and span ID from ``os.urandom``.

    The upstream ``RandomIdGenerator`` uses ``random.getrandbits``, a Mersenne Twister whose state is held in process
    memory. When a process is restored from a memory snapshot (for example, Amazon Bedrock AgentCore Runtime V2),
    every restored instance inherits the same state and generates the same IDs.
    ``os.urandom`` reads from the operating system's CSPRNG, which the kernel reseeds, so restored instances do not
    produce colliding IDs.
    """

    # pylint: disable=no-self-use
    def generate_span_id(self) -> int:
        span_id = int.from_bytes(os.urandom(8), "big")
        while span_id == trace.INVALID_SPAN_ID:
            span_id = int.from_bytes(os.urandom(8), "big")
        return span_id

    def generate_trace_id(self) -> int:
        trace_id = int.from_bytes(os.urandom(16), "big")
        while trace_id == trace.INVALID_TRACE_ID:
            trace_id = int.from_bytes(os.urandom(16), "big")
        return trace_id

    def is_trace_id_random(self) -> bool:
        return True
