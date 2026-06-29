# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data model for function call duration metrics.

Defines the DurationMetrics schema used for the endpoint telemetry duration
histogram body.
"""

from dataclasses import dataclass
from typing import List, Union


@dataclass
class DurationMetrics:
    """
    Duration metrics in EMF histogram format.

    Represents aggregated duration measurements over a collection period.
    Uses CloudWatch EMF histogram format with Values and Counts arrays.

    Note: Values and Counts can be floats due to SEH (Sparse Exponential Histogram)
    aggregation which may produce float bucket midpoints and weighted counts.
    """

    values: List[Union[int, float]]  # Bucket midpoints or duration samples (microseconds)
    counts: List[Union[int, float]]  # Count for each value (can be weighted floats from SEH)
    max: Union[int, float]  # Maximum duration (microseconds)
    min: Union[int, float]  # Minimum duration (microseconds)
    count: Union[int, float]  # Total number of invocations
    sum: Union[int, float]  # Sum of all durations (microseconds)
