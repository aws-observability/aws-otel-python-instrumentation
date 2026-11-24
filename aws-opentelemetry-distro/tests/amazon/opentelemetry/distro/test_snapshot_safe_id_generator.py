# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import random
from importlib.metadata import entry_points
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.snapshot_safe_id_generator import SnapshotSafeIdGenerator
from opentelemetry.sdk._configuration import _import_id_generator
from opentelemetry.trace import INVALID_SPAN_ID, INVALID_TRACE_ID

_MODULE = "amazon.opentelemetry.distro.snapshot_safe_id_generator"


class TestSnapshotSafeIdGenerator(TestCase):
    def setUp(self):
        self.generator = SnapshotSafeIdGenerator()

    def test_ids_are_unique_when_random_state_repeats(self):
        # Simulate restoring a snapshot: every "session" starts from the same `random` state.
        snapshot = random.getstate()
        trace_ids, span_ids = set(), set()
        for _ in range(20):
            random.setstate(snapshot)
            trace_ids.add(self.generator.generate_trace_id())
            span_ids.add(self.generator.generate_span_id())
        self.assertEqual(len(trace_ids), 20)
        self.assertEqual(len(span_ids), 20)

    def test_does_not_use_random_module(self):
        with patch("random.getrandbits", side_effect=AssertionError("must not use random")):
            self.generator.generate_trace_id()
            self.generator.generate_span_id()

    def test_trace_id_is_random_128_bits(self):
        trace_id = self.generator.generate_trace_id()
        self.assertNotEqual(trace_id, INVALID_TRACE_ID)
        self.assertLess(trace_id, 2**128)
        self.assertTrue(self.generator.is_trace_id_random())

    def test_span_id_fits_64_bits(self):
        span_id = self.generator.generate_span_id()
        self.assertNotEqual(span_id, INVALID_SPAN_ID)
        self.assertLess(span_id, 2**64)

    def test_retries_invalid_span_id(self):
        with patch(f"{_MODULE}.os.urandom", side_effect=[bytes(8), b"\x00" * 7 + b"\x01"]):
            self.assertEqual(self.generator.generate_span_id(), 1)

    def test_retries_invalid_trace_id(self):
        with patch(f"{_MODULE}.os.urandom", side_effect=[bytes(16), b"\x00" * 15 + b"\x01"]):
            self.assertEqual(self.generator.generate_trace_id(), 1)

    def test_registered_as_snapshot_safe_entry_point(self):
        names = [ep.name for ep in entry_points(group="opentelemetry_id_generator")]
        self.assertIn("snapshot_safe", names)
        self.assertIsInstance(_import_id_generator("snapshot_safe"), SnapshotSafeIdGenerator)
