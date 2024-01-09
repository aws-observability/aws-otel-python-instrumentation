# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from metric_attribute_generator import MetricAttributeGenerator

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import BoundedAttributes, ReadableSpan


class AwsMetricAttributeGenerator(MetricAttributeGenerator):
    """AwsMetricAttributeGenerator generates specific metric attributes for incoming and outgoing traffic.

    AwsMetricAttributeGenerator generates very specific metric attributes based on low-cardinality span and resource
    attributes. If such attributes are not present, we fallback to default values.

    The goal of these particular metric attributes is to get metrics for incoming and outgoing traffic for a service.
    Namely, SpanKind#SERVER and SpanKind#CONSUMER spans represent "incoming" traffic, SpanKind#CLIENT and
    SpanKind#PRODUCER spans represent "outgoing" traffic, and SpanKind#INTERNAL spans are ignored.
    """

    @staticmethod
    def generate_metric_attributes_dict_from_span(span: ReadableSpan, resource: Resource) -> [str, BoundedAttributes]:
        """This method is used by the AwsSpanMetricsProcessor to generate service and dependency metrics"""
        # TODO
        return {}
