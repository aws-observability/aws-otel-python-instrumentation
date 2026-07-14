# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import BoundedAttributes, ReadableSpan

SERVICE_METRIC: str = "Service"
DEPENDENCY_METRIC: str = "Dependency"


class MetricAttributeGenerator:
    """MetricAttributeGenerator is an interface for generating metric attributes from a span.

    Metric attribute generator defines an interface for classes that can generate specific attributes to be used by an
    AwsSpanMetricsProcessor to produce metrics and by AwsMetricAttributesSpanExporter to wrap the original span.
    """

    @staticmethod
    def generate_metric_attributes_dict_from_span(span: ReadableSpan, resource: Resource) -> [str, BoundedAttributes]:
        """Generate metric attributes from a span.

        Given a span and associated resource, produce meaningful metric attributes for metrics produced from the span.
        If no metrics should be generated from this span, return empty attributes.

        Args:
            span - ReadableSpan to be used to generate metric attributes.
            resource - Resource associated with Span to be used to generate metric attributes.
        Returns:
            A dictionary of Attributes objects with values assigned to key "Service" or "Dependency".  It will contain
            either 0, 1, or 2 items.
        """
