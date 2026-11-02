# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for SEH (Sparse Exponential Histogram) implementation.

Tests validate:
- Bucket calculation accuracy
- Value recovery precision
- Zero value handling
- Statistical correctness (min, max, sum, count)
- Input validation
- Bucket limit enforcement
- Compression efficiency
"""

import math
import unittest
from unittest import mock

from amazon.opentelemetry.distro.serviceevents.utils.seh_histogram import BUCKET_FACTOR, BUCKET_FOR_ZERO, SEHHistogram


class TestSEHHistogram(unittest.TestCase):
    """Test cases for SEHHistogram class."""

    def test_initialization(self):
        """Test histogram initialization."""
        hist = SEHHistogram(max_buckets=100)
        self.assertEqual(hist.max_buckets, 100)
        self.assertEqual(hist.count, 0.0)
        self.assertEqual(hist.sum, 0.0)
        self.assertIsNone(hist.minimum)
        self.assertIsNone(hist.maximum)
        self.assertEqual(len(hist.buckets), 0)
        self.assertTrue(hist.is_empty())

    def test_record_single_value(self):
        """Test recording a single value."""
        hist = SEHHistogram()
        result = hist.record(100.0)

        self.assertTrue(result)
        self.assertEqual(hist.count, 1.0)
        self.assertEqual(hist.sum, 100.0)
        self.assertEqual(hist.minimum, 100.0)
        self.assertEqual(hist.maximum, 100.0)
        self.assertFalse(hist.is_empty())

    def test_record_multiple_values(self):
        """Test recording multiple values."""
        hist = SEHHistogram()
        values = [10.0, 20.0, 30.0, 40.0, 50.0]

        for value in values:
            hist.record(value)

        self.assertEqual(hist.count, 5.0)
        self.assertEqual(hist.sum, 150.0)
        self.assertEqual(hist.minimum, 10.0)
        self.assertEqual(hist.maximum, 50.0)

    def test_record_with_weights(self):
        """Test recording values with weights."""
        hist = SEHHistogram()
        hist.record(100.0, weight=2.0)
        hist.record(200.0, weight=3.0)

        self.assertEqual(hist.count, 5.0)  # 2 + 3
        self.assertEqual(hist.sum, 800.0)  # 100*2 + 200*3

    def test_zero_value_handling(self):
        """Test that zero values map to special bucket."""
        hist = SEHHistogram()
        hist.record(0.0)

        self.assertEqual(hist.count, 1.0)
        self.assertEqual(hist.sum, 0.0)
        self.assertEqual(hist.minimum, 0.0)
        self.assertEqual(hist.maximum, 0.0)

        # Verify zero uses special bucket
        self.assertIn(BUCKET_FOR_ZERO, hist.buckets)
        self.assertEqual(hist.buckets[BUCKET_FOR_ZERO], 1.0)

        # Verify value recovery returns 0
        values, counts = hist.get_values_and_counts()
        self.assertEqual(values[0], 0.0)
        self.assertEqual(counts[0], 1.0)

    def test_bucket_calculation_consistency(self):
        """Test that similar values map to same bucket."""
        hist = SEHHistogram()

        # Values within ~10% should map to same bucket
        similar_values = [1000.0, 1005.0, 1009.0]

        buckets_used = set()
        for value in similar_values:
            bucket = hist._get_bucket(value)
            buckets_used.add(bucket)
            hist.record(value)

        # All similar values should use same or adjacent buckets
        self.assertLessEqual(len(buckets_used), 2)

    def test_values_spanning_orders_of_magnitude(self):
        """Test compression with values spanning multiple orders of magnitude."""
        hist = SEHHistogram()

        # Values from nanoseconds to seconds
        values = [
            1_000,  # 1 microsecond
            10_000,  # 10 microseconds
            100_000,  # 100 microseconds
            1_000_000,  # 1 millisecond
            10_000_000,  # 10 milliseconds
            100_000_000,  # 100 milliseconds
            1_000_000_000,  # 1 second
        ]

        for value in values:
            hist.record(value)

        # Should maintain all samples (values span too many orders of magnitude)
        self.assertEqual(hist.count, 7.0)
        self.assertEqual(len(hist.buckets), 7)  # Each value gets own bucket due to large range

        # Verify statistics
        self.assertEqual(hist.minimum, 1_000)
        self.assertEqual(hist.maximum, 1_000_000_000)
        self.assertEqual(hist.sum, sum(values))

    def test_get_values_and_counts_format(self):
        """Test that get_values_and_counts returns proper format."""
        hist = SEHHistogram()
        values_input = [100, 200, 300, 400, 500]

        for value in values_input:
            hist.record(value)

        values, counts = hist.get_values_and_counts()

        # Both should be lists
        self.assertIsInstance(values, list)
        self.assertIsInstance(counts, list)

        # Same length
        self.assertEqual(len(values), len(counts))

        # Values should be sorted (ascending bucket order)
        self.assertEqual(values, sorted(values))

        # Counts should sum to total count
        self.assertEqual(sum(counts), hist.count)

    def test_value_recovery_accuracy(self):
        """Test that recovered values are within ~10% of original."""
        hist = SEHHistogram()
        test_values = [1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]

        for value in test_values:
            hist.record(value)

        values, counts = hist.get_values_and_counts()

        # Each recovered value should be within ~10% of an input value
        for recovered_value in values:
            # Find closest input value
            min_error = float("inf")
            for original in test_values:
                relative_error = abs(recovered_value - original) / original
                min_error = min(min_error, relative_error)

            # Should be within ~10% (bucket width)
            self.assertLess(min_error, 0.15)  # Allow some tolerance

    def test_bucket_limit_folds_overflow_into_nearest_bucket(self):
        """At the bucket cap, overflow samples fold into the nearest existing bucket
        rather than being dropped — so count/sum stay consistent with sum(bucket weights)."""
        hist = SEHHistogram(max_buckets=5)

        # Exponentially spaced values guarantee distinct buckets.
        values = [10**i for i in range(10)]  # 1, 10, ... 1e9
        for value in values:
            # Every sample is accepted now (folded when the cap is hit).
            self.assertTrue(hist.record(value))

        # Capped at exactly max_buckets distinct buckets.
        self.assertEqual(len(hist.buckets), 5)
        # No sample dropped: count reflects ALL recorded values.
        self.assertEqual(hist.count, len(values))
        self.assertEqual(hist.sum, sum(values))
        # The emitted buckets account for every sample (Count == sum(Counts)).
        _emitted_values, counts = hist.get_values_and_counts()
        self.assertEqual(sum(counts), hist.count)
        # min/max still track the true observed extremes, not the folded bucket.
        self.assertEqual(hist.minimum, min(values))
        self.assertEqual(hist.maximum, max(values))

    def test_duplicate_values_dont_create_new_buckets(self):
        """Test that duplicate values increment existing bucket count."""
        hist = SEHHistogram(max_buckets=2)

        # Record same value multiple times
        for _ in range(10):
            result = hist.record(100.0)
            self.assertTrue(result)

        # Should only have 1 bucket
        self.assertEqual(len(hist.buckets), 1)
        self.assertEqual(hist.count, 10.0)

        values, counts = hist.get_values_and_counts()
        self.assertEqual(len(values), 1)
        self.assertEqual(counts[0], 10.0)

    def test_input_validation_nan(self):
        """Test that NaN values are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(float("nan"))

        self.assertIn("NaN", str(context.exception))

    def test_input_validation_infinity(self):
        """Test that Infinity values are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError):
            hist.record(float("inf"))

        with self.assertRaises(ValueError):
            hist.record(float("-inf"))

    def test_input_validation_negative_weight(self):
        """Test that negative weights are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(100.0, weight=-1.0)

        self.assertIn("positive", str(context.exception))

    def test_input_validation_zero_weight(self):
        """Test that zero weight is rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError):
            hist.record(100.0, weight=0.0)

    def test_input_validation_nan_weight(self):
        """Test that NaN weights are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(100.0, weight=float("nan"))

        self.assertIn("NaN", str(context.exception))

    def test_input_validation_infinity_weight(self):
        """Test that Infinity weights are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(100.0, weight=float("inf"))

        self.assertIn("Infinity", str(context.exception))

    def test_input_validation_value_below_minimum(self):
        """Test that values below the supported minimum are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(-(2**361))

        self.assertIn("below minimum", str(context.exception))

    def test_input_validation_value_above_maximum(self):
        """Test that values above the supported maximum are rejected."""
        hist = SEHHistogram()

        with self.assertRaises(ValueError) as context:
            hist.record(2**361)

        self.assertIn("exceeds maximum", str(context.exception))

    def test_record_returns_false_when_validation_fails(self):
        """Test that record returns False when validation reports an invalid input."""
        hist = SEHHistogram()

        with mock.patch.object(hist, "_validate_input", return_value=False):
            result = hist.record(100.0)

        self.assertFalse(result)
        self.assertEqual(hist.count, 0.0)
        self.assertEqual(len(hist.buckets), 0)

    def test_bucket_calculation_for_negative_values(self):
        """Test bucket calculation negates the bucket number for negative values."""
        hist = SEHHistogram()

        value = 100.0
        positive_bucket = hist._get_bucket(value)
        negative_bucket = hist._get_bucket(-value)

        self.assertEqual(negative_bucket, -positive_bucket)

    def test_empty_histogram_values_and_counts(self):
        """Test that empty histogram returns empty arrays."""
        hist = SEHHistogram()
        values, counts = hist.get_values_and_counts()

        self.assertEqual(values, [])
        self.assertEqual(counts, [])

    def test_get_statistics(self):
        """Test get_statistics method."""
        hist = SEHHistogram()
        values = [10.0, 20.0, 30.0]

        for value in values:
            hist.record(value)

        stats = hist.get_statistics()

        self.assertEqual(stats["min"], 10.0)
        self.assertEqual(stats["max"], 30.0)
        self.assertEqual(stats["sum"], 60.0)
        self.assertEqual(stats["count"], 3.0)

    def test_empty_histogram_statistics(self):
        """Test statistics on empty histogram."""
        hist = SEHHistogram()
        stats = hist.get_statistics()

        self.assertEqual(stats["min"], 0.0)
        self.assertEqual(stats["max"], 0.0)
        self.assertEqual(stats["sum"], 0.0)
        self.assertEqual(stats["count"], 0.0)

    def test_realistic_duration_values(self):
        """Test with realistic function duration values (nanoseconds)."""
        hist = SEHHistogram()

        # Realistic durations: 10ms to 900ms
        durations_ns = [
            20_000_000,  # 20ms
            50_000_000,  # 50ms
            75_000_000,  # 75ms
            100_000_000,  # 100ms
            150_000_000,  # 150ms
            200_000_000,  # 200ms
            500_000_000,  # 500ms
            900_000_000,  # 900ms
        ]

        for duration in durations_ns:
            hist.record(duration)

        values, counts = hist.get_values_and_counts()

        # Should compress (not all durations need separate buckets)
        self.assertGreater(len(values), 0)
        self.assertLessEqual(len(values), len(durations_ns))

        # Verify statistics are exact
        self.assertEqual(hist.count, 8.0)
        self.assertEqual(hist.sum, sum(durations_ns))
        self.assertEqual(hist.minimum, min(durations_ns))
        self.assertEqual(hist.maximum, max(durations_ns))

    def test_bucket_calculation_for_zero(self):
        """Test bucket calculation returns special value for zero."""
        hist = SEHHistogram()
        bucket = hist._get_bucket(0.0)
        self.assertEqual(bucket, BUCKET_FOR_ZERO)

    def test_bucket_calculation_for_positive_values(self):
        """Test bucket calculation for positive values."""
        hist = SEHHistogram()

        # Test known values
        # bucket = floor(log(value) / log(1.1))
        value = 100.0
        expected_bucket = int(math.floor(math.log(value) / BUCKET_FACTOR))
        actual_bucket = hist._get_bucket(value)

        self.assertEqual(actual_bucket, expected_bucket)

    def test_value_recovery_for_zero_bucket(self):
        """Test value recovery returns 0 for zero bucket."""
        hist = SEHHistogram()
        recovered = hist._recover_value(BUCKET_FOR_ZERO)
        self.assertEqual(recovered, 0.0)

    def test_value_recovery_formula(self):
        """Test value recovery uses correct formula."""
        hist = SEHHistogram()

        bucket_num = 10
        expected_value = math.exp((bucket_num + 0.5) * BUCKET_FACTOR)
        actual_value = hist._recover_value(bucket_num)

        self.assertAlmostEqual(actual_value, expected_value, places=10)

    def test_repr(self):
        """Test string representation."""
        hist = SEHHistogram()
        hist.record(100.0)
        hist.record(200.0)

        repr_str = repr(hist)

        self.assertIn("SEHHistogram", repr_str)
        self.assertIn("count=2", repr_str)
        self.assertIn("min=100", repr_str)
        self.assertIn("max=200", repr_str)

    def test_compression_ratio(self):
        """Test that SEH provides good compression for typical workloads."""
        hist = SEHHistogram()

        # Simulate 1000 samples with realistic variation
        import random

        random.seed(42)

        base_duration = 50_000_000  # 50ms
        for _ in range(1000):
            # Add ±50% variation
            variation = random.uniform(0.5, 1.5)
            duration = int(base_duration * variation)
            hist.record(duration)

        values, counts = hist.get_values_and_counts()

        # Should compress significantly (1000 samples -> much fewer buckets)
        self.assertEqual(hist.count, 1000.0)
        self.assertLess(len(values), 100)  # Should be well under 100 buckets

        print(f"\nCompression: 1000 samples -> {len(values)} buckets")

    def test_emf_compatibility(self):
        """Test that output format is compatible with CloudWatch EMF."""
        hist = SEHHistogram()

        durations = [10_000_000, 20_000_000, 30_000_000, 15_000_000, 25_000_000]
        for duration in durations:
            hist.record(duration)

        values, counts = hist.get_values_and_counts()
        stats = hist.get_statistics()

        # EMF format requirements
        emf_data = {
            "Values": values,
            "Counts": counts,
            "Max": stats["max"],
            "Min": stats["min"],
            "Count": stats["count"],
            "Sum": stats["sum"],
        }

        # Verify structure
        self.assertIsInstance(emf_data["Values"], list)
        self.assertIsInstance(emf_data["Counts"], list)
        self.assertEqual(len(emf_data["Values"]), len(emf_data["Counts"]))
        self.assertLessEqual(len(emf_data["Values"]), 100)  # CloudWatch limit

        # Verify values are sorted
        self.assertEqual(emf_data["Values"], sorted(emf_data["Values"]))

        print(f"\nEMF format sample: Values={len(values)} buckets, Count={stats['count']}")


if __name__ == "__main__":
    unittest.main()
