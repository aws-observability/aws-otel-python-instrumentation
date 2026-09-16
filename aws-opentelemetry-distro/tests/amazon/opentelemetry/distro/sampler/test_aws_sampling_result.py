# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from unittest import TestCase

from amazon.opentelemetry.distro.sampler._aws_sampling_result import _AwsSamplingResult
from opentelemetry.sdk.trace.sampling import Decision
from opentelemetry.trace import TraceState

_XRSR_KEY = _AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY


class TestAwsSamplingResult(TestCase):
    def test_adds_sampling_rule_hash_to_trace_state(self):
        result = _AwsSamplingResult(
            decision=Decision.RECORD_AND_SAMPLE,
            sampling_rule_hash="somehash",
        )
        self.assertEqual(result.trace_state.get(_XRSR_KEY), "somehash")

    def test_does_not_overwrite_existing_trace_state_key(self):
        result = _AwsSamplingResult(
            decision=Decision.RECORD_AND_SAMPLE,
            trace_state=TraceState([(_XRSR_KEY, "existing")]),
            sampling_rule_hash="somehash",
        )
        self.assertEqual(result.trace_state.get(_XRSR_KEY), "existing")

    def test_none_hash_does_not_add_key(self):
        # Regression test for issue #874: a None sampling_rule_hash must not be added
        # to the trace state (add would be a no-op and would emit a noisy WARNING).
        result = _AwsSamplingResult(
            decision=Decision.RECORD_AND_SAMPLE,
            sampling_rule_hash=None,
        )
        self.assertIsNone(result.trace_state.get(_XRSR_KEY))
        self.assertEqual(len(result.trace_state), 0)

    def test_none_hash_emits_no_warning(self):
        # Regression test for issue #874: no "Invalid key/value pair (xrsr, None) found."
        # warning should be logged when the sampling rule hash is None.
        with self.assertLogs("opentelemetry.trace.span", level=logging.WARNING) as captured:
            _AwsSamplingResult(
                decision=Decision.RECORD_AND_SAMPLE,
                sampling_rule_hash=None,
            )
            # assertLogs fails if nothing is logged, so emit a sentinel to assert against.
            logging.getLogger("opentelemetry.trace.span").warning("sentinel")

        self.assertEqual(len(captured.records), 1)
        self.assertEqual(captured.records[0].getMessage(), "sentinel")
