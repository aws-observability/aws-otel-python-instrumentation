# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from types import MappingProxyType
from typing import Optional

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_XRAY_ADAPTIVE_SAMPLING_CONFIGURED_ATTRIBUTE_KEY,
    AWS_XRAY_SAMPLING_RULE,
)
from opentelemetry.sdk.trace.sampling import Decision, SamplingResult
from opentelemetry.trace import TraceState
from opentelemetry.util.types import Attributes, AttributeValue


class _AwsSamplingResult(SamplingResult):
    AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY = "xrsr"

    def __init__(
        self,
        decision: Decision,
        attributes: "Attributes" = None,
        trace_state: Optional["TraceState"] = None,
        sampling_rule_name: Optional[str] = None,
        sampling_rule_hash: Optional[str] = None,
        has_adaptive_sampling_config: bool = False,
    ):
        # Define attributes that will be set by super()
        self.decision = decision
        self.trace_state = None
        self.attributes = None

        super().__init__(decision, attributes, trace_state)

        # super will have defined self.attributes by this point
        self.__add_attribute(AWS_XRAY_ADAPTIVE_SAMPLING_CONFIGURED_ATTRIBUTE_KEY, has_adaptive_sampling_config)
        if sampling_rule_name is not None:
            self.__add_attribute(AWS_XRAY_SAMPLING_RULE, sampling_rule_name)

        if self.trace_state is None:
            self.trace_state = TraceState()
        if self.trace_state.get(self.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY) is None:
            self.trace_state = self.trace_state.add(self.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY, sampling_rule_hash)

        self._sampling_rule_name = sampling_rule_name

    def __add_attribute(self, key: str, value: AttributeValue):
        self.attributes = MappingProxyType(
            {
                **self.attributes,
                key: value,
            }
        )
