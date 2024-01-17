# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from opentelemetry.sdk.trace.sampling import Decision, Sampler, StaticSampler, SamplingResult
from opentelemetry.util.types import Attributes
from opentelemetry.trace.span import TraceState
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind


class TestAlwaysRecordSampler(TestCase):
    def setUp(self):
        self.static_sampler: Sampler = StaticSampler(Decision.DROP)
        self.mock_sampler: Sampler = MagicMock()
        self.sampler: Sampler = AlwaysRecordSampler(self.mock_sampler)

    def test_get_description(self):
        test_sampler: Sampler = AlwaysRecordSampler(self.static_sampler)
        self.assertIn("AlwaysRecordSampler", test_sampler.get_description())

    def test_record_and_sample_sampling_decision(self):
        self.validateShouldSample(Decision.RECORD_AND_SAMPLE, Decision.RECORD_AND_SAMPLE)

    def validateShouldSample(self, root_decision: Decision, expected_decision: Decision):
        root_result: SamplingResult = self.sampling_result(root_decision)
        self.mock_sampler.should_sample.return_value = root_result
        actual_result: SamplingResult = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes={},
            trace_state=TraceState()
        )

        if root_decision == expected_decision:
            self.assertEqual(actual_result, root_result)
            self.assertEqual(actual_result.decision, root_decision)
        else:
            self.assertNotEqual(actual_result, root_result)
            self.assertNotEqual(actual_result.decision, root_decision)

        self.assertEqual(actual_result.attributes, root_result.attributes)
        self.assertEqual(actual_result.trace_state, root_result.trace_state)

    @staticmethod
    def sampling_result(sampling_decision: Decision):
        sampling_attr: Attributes = {"key": sampling_decision.name}
        sampling_trace_state: TraceState = TraceState()
        sampling_trace_state.add("key", sampling_decision.name)
        sampling_result: SamplingResult = SamplingResult(
            decision=sampling_decision,
            attributes=sampling_attr,
            trace_state=sampling_trace_state
        )
        return sampling_result
