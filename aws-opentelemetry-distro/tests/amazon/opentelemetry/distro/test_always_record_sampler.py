# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult, StaticSampler
from opentelemetry.trace import SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class TestAlwaysRecordSampler(TestCase):
    def setUp(self):
        self.mock_sampler: Sampler = MagicMock()
        self.sampler: Sampler = AlwaysRecordSampler(self.mock_sampler)

    def test_get_description(self):
        static_sampler: Sampler = StaticSampler(Decision.DROP)
        test_sampler: Sampler = AlwaysRecordSampler(static_sampler)
        self.assertEqual("AlwaysRecordSampler{AlwaysOffSampler}", test_sampler.get_description())

    def test_record_and_sample_sampling_decision(self):
        self.validate_should_sample(Decision.RECORD_AND_SAMPLE, Decision.RECORD_AND_SAMPLE)

    def test_record_only_sampling_decision(self):
        self.validate_should_sample(Decision.RECORD_ONLY, Decision.RECORD_ONLY)

    def test_drop_sampling_decision(self):
        self.validate_should_sample(Decision.DROP, Decision.RECORD_ONLY)

    def test_drop_with_both_none_attributes(self):
        root_result = SamplingResult(decision=Decision.DROP, attributes=None, trace_state=TraceState())
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=None,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(len(actual_result.attributes), 0)

    def test_drop_with_both_empty_attributes(self):
        root_result = _build_root_sampling_result(Decision.DROP, {})
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes={},
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {})

    def test_drop_decision_merges_attributes_with_sampler_precedence(self):
        root_result = _build_root_sampling_result(Decision.DROP, {"shared_key": "sampler_value", "sampler_only": "yes"})
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"shared_key": "original_value", "original_only": "yes"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        # Sampler attribute takes precedence
        self.assertEqual(actual_result.attributes.get("shared_key"), "sampler_value")
        self.assertEqual(actual_result.attributes.get("sampler_only"), "yes")
        self.assertEqual(actual_result.attributes.get("original_only"), "yes")

    def test_drop_with_original_none_uses_sampler_attributes(self):
        root_result = _build_root_sampling_result(Decision.DROP, {"sampler_key": "sampler_value"})
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=None,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"sampler_key": "sampler_value"})

    def test_drop_with_sampler_none_uses_original_attributes(self):
        root_result = SamplingResult(decision=Decision.DROP, attributes=None, trace_state=TraceState())
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"original_key": "original_value"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"original_key": "original_value"})

    def test_drop_merges_disjoint_attributes(self):
        root_result = _build_root_sampling_result(Decision.DROP, {"c": "3", "d": "4"})
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"a": "1", "b": "2"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"a": "1", "b": "2", "c": "3", "d": "4"})

    def validate_should_sample(self, root_decision: Decision, expected_decision: Decision):
        root_result: SamplingResult = _build_root_sampling_result(root_decision)
        self.mock_sampler.should_sample.return_value = root_result
        original_attributes = {"key": root_decision.name}
        actual_result: SamplingResult = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        if root_decision == expected_decision:
            self.assertEqual(actual_result, root_result)
            self.assertEqual(actual_result.decision, root_decision)
        else:
            self.assertNotEqual(actual_result, root_result)
            self.assertEqual(actual_result.decision, expected_decision)

        # For non-DROP decisions, attributes should match root result
        # For DROP decisions, attributes are merged (original + sampler)
        if root_decision != Decision.DROP:
            self.assertEqual(actual_result.attributes, root_result.attributes)
        self.assertEqual(actual_result.trace_state, root_result.trace_state)


def _build_root_sampling_result(sampling_decision: Decision, attributes: Attributes = None):
    sampling_attr: Attributes = attributes if attributes is not None else {"key": sampling_decision.name}
    sampling_trace_state: TraceState = TraceState()
    sampling_trace_state.add("key", sampling_decision.name)
    sampling_result: SamplingResult = SamplingResult(
        decision=sampling_decision, attributes=sampling_attr, trace_state=sampling_trace_state
    )
    return sampling_result
