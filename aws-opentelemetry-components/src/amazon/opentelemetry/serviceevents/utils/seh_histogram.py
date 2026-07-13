# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
CloudWatch SEH (Sparse Exponential Histogram) implementation for Python.

This module implements the SEH1 algorithm used by AWS CloudWatch for efficient
distribution aggregation with ~10% relative error. Based on the Go implementation:
https://github.com/aws/amazon-cloudwatch-agent/blob/main/metric/distribution/seh1/seh1_distribution.go

The SEH algorithm uses exponentially-spaced buckets to compress large numbers of
samples into a compact representation suitable for CloudWatch EMF (Embedded Metric Format).
"""

import math
from typing import Dict, List, Optional, Tuple

# Constants for SEH1 algorithm
# Bucket width factor: log(1.1) gives ~10% relative error per bucket
BUCKET_FACTOR = math.log(1.1)  # ~0.0953101798043

# Special bucket number for exact zero values
BUCKET_FOR_ZERO = -32768  # math.MinInt16 equivalent

# Supported value range: ±2^360 (extremely large, practically unlimited)
MIN_VALUE = -(2**360)
MAX_VALUE = 2**360


class SEHHistogram:
    """
    Sparse Exponential Histogram for distribution aggregation.

    This class maintains a sparse map of exponentially-spaced buckets to efficiently
    aggregate duration samples while preserving statistical properties.

    Attributes:
        max_buckets: Maximum number of distinct buckets allowed (CloudWatch EMF limit: 100)
        buckets: Sparse map of bucket_number → count (only non-zero buckets stored)
        minimum: Minimum value observed
        maximum: Maximum value observed
        sum: Sum of all values × weights
        count: Total weighted sample count
    """

    def __init__(self, max_buckets: int = 100):
        """
        Initialize an empty SEH histogram.

        Args:
            max_buckets: Maximum number of distinct buckets to maintain.
                        CloudWatch EMF supports up to 100 values per metric.
        """
        self.max_buckets = max_buckets
        self.buckets: Dict[int, float] = {}
        self.minimum: Optional[float] = None
        self.maximum: Optional[float] = None
        self.sum: float = 0.0
        self.count: float = 0.0

    def record(self, value: float, weight: float = 1.0) -> bool:
        """
        Record a value into the histogram with optional weight.

        Args:
            value: The value to record (e.g., duration in nanoseconds)
            weight: Weight for this sample (default: 1.0)

        Returns:
            True if the value was recorded, False if rejected (validation failed)

        Raises:
            ValueError: If validation fails (NaN, Infinity, invalid range, or weight <= 0)
        """
        # Validate input
        if not self._validate_input(value, weight):
            return False

        bucket_num = self._get_bucket(value)

        # Bucket-cap handling: when a new distinct value would exceed max_buckets, fold its
        # weight into the nearest existing bucket instead of dropping the sample. Dropping
        # would desync this histogram's count/sum (and the buckets it emits) from the
        # endpoint aggregation's own count/sum_duration, producing an EMF histogram where
        # Count > sum(Counts) and Sum includes durations absent from every bucket. Folding
        # keeps the invariant sum(bucket weights) == count at the cost of ~one extra bucket
        # of relative error for the few overflow samples (only past 100 distinct buckets).
        if bucket_num not in self.buckets and len(self.buckets) >= self.max_buckets:
            bucket_num = min(self.buckets, key=lambda existing: abs(existing - bucket_num))

        # Update statistics. min/max track the true observed value, not the folded bucket.
        self.count += weight
        self.sum += value * weight

        if self.minimum is None or value < self.minimum:
            self.minimum = value

        if self.maximum is None or value > self.maximum:
            self.maximum = value

        # Update bucket count
        if bucket_num in self.buckets:
            self.buckets[bucket_num] += weight
        else:
            self.buckets[bucket_num] = weight

        return True

    def get_values_and_counts(self) -> Tuple[List[float], List[float]]:
        """
        Get the histogram as parallel arrays of values and counts.

        Returns:
            Tuple of (values, counts) where:
            - values: List of representative values (bucket midpoints)
            - counts: List of counts corresponding to each value
            Both lists are sorted by bucket number (ascending).

        Note:
            This format is compatible with CloudWatch EMF histogram structure.
        """
        if not self.buckets:
            return ([], [])

        # Sort buckets by bucket number
        sorted_buckets = sorted(self.buckets.items())

        values = []
        counts = []

        for bucket_num, count in sorted_buckets:
            # Recover representative value from bucket number
            value = self._recover_value(bucket_num)
            values.append(value)
            counts.append(count)

        return (values, counts)

    @staticmethod
    def _validate_input(value: float, weight: float) -> bool:
        """
        Validate input value and weight.

        Args:
            value: Value to validate
            weight: Weight to validate

        Returns:
            True if valid, False otherwise

        Raises:
            ValueError: If validation fails with descriptive message
        """
        # Check for NaN
        if math.isnan(value):
            raise ValueError("Value cannot be NaN")

        if math.isnan(weight):
            raise ValueError("Weight cannot be NaN")

        # Check for Infinity
        if math.isinf(value):
            raise ValueError("Value cannot be Infinity")

        if math.isinf(weight):
            raise ValueError("Weight cannot be Infinity")

        # Check weight > 0
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")

        # Check value range (with 0.1% tolerance like Go implementation)
        # Note: In practice, duration values will be well within this range
        tolerance = 1.001
        if value < MIN_VALUE * tolerance:
            raise ValueError(f"Value {value} is below minimum supported value")

        if value > MAX_VALUE * tolerance:
            raise ValueError(f"Value {value} exceeds maximum supported value")

        return True

    @staticmethod
    def _get_bucket(value: float) -> int:
        """
        Calculate the bucket number for a given value.

        The bucket calculation uses logarithmic spacing:
        bucket_number = floor(log(value) / log(1.1))

        Args:
            value: The value to bucket

        Returns:
            Bucket number as int16 (-32768 to 32767)

        Note:
            Zero values map to a special bucket (BUCKET_FOR_ZERO = -32768)
        """
        if value == 0:
            return BUCKET_FOR_ZERO

        # For negative values, use absolute value for bucket calculation
        # (preserving sign in the bucket space)
        abs_value = abs(value)

        # Calculate bucket: floor(log(abs_value) / BUCKET_FACTOR)
        bucket_num = int(math.floor(math.log(abs_value) / BUCKET_FACTOR))

        # Apply sign
        if value < 0:
            bucket_num = -bucket_num

        return bucket_num

    @staticmethod
    def _recover_value(bucket_num: int) -> float:
        """
        Recover the representative value from a bucket number.

        Uses the geometric midpoint of the exponential bucket range:
        value = exp((bucket_num + 0.5) × log(1.1))

        The 0.5 offset selects the center of the bucket's range.

        Args:
            bucket_num: Bucket number

        Returns:
            Representative value for this bucket
        """
        if bucket_num == BUCKET_FOR_ZERO:
            return 0.0

        # Calculate midpoint value: exp((bucket_num + 0.5) × BUCKET_FACTOR)
        value = math.exp((bucket_num + 0.5) * BUCKET_FACTOR)

        return value

    def is_empty(self) -> bool:
        """Check if the histogram is empty (no samples recorded)."""
        return self.count == 0

    def get_statistics(self) -> Dict[str, float]:
        """
        Get summary statistics for the histogram.

        Returns:
            Dictionary with keys: min, max, sum, count
        """
        return {
            "min": self.minimum if self.minimum is not None else 0.0,
            "max": self.maximum if self.maximum is not None else 0.0,
            "sum": self.sum,
            "count": self.count,
        }

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"SEHHistogram(count={self.count}, buckets={len(self.buckets)}, "
            f"min={self.minimum}, max={self.maximum}, sum={self.sum})"
        )
