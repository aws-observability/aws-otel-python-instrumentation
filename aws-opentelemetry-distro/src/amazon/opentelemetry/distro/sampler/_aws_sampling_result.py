# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from opentelemetry.sdk.trace.sampling import Decision, SamplingResult
from opentelemetry.trace import TraceState
from opentelemetry.util.types import Attributes


class _AwsSamplingResult(SamplingResult):
    AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY = "xrsr"

    def __init__(
        self,
        decision: Decision,
        attributes: "Attributes" = {},
        trace_state: Optional["TraceState"] = None,
        sampling_rule_name: Optional[str] = None,
    ):
        super().__init__(decision, attributes, trace_state)

        if self.trace_state is None:
            self.trace_state = TraceState()
        self.trace_state = self.trace_state.add(self.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY, sampling_rule_name)
        self._sampling_rule_name = sampling_rule_name
