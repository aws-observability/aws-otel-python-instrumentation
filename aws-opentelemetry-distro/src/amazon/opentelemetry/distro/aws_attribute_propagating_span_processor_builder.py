# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List

from amazon.opentelemetry.distro.aws_attribute_keys import AwsAttributeKeys
from amazon.opentelemetry.distro.aws_attribute_propagating_span_processor import AwsAttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.aws_span_processing_util import AwsSpanProcessingUtil
from opentelemetry.sdk.trace import ReadableSpan


class AwsAttributePropagatingSpanProcessorBuilder:
    """
    AwsAttributePropagatingSpanProcessorBuilder is used to construct a {@link
    AwsAttributePropagatingSpanProcessor}. If {@link #set_propagation_data_extractor}, {@link#set_propagation_data_key}
    or {@link #set_attributes_keys_to_propagate} are not invoked, the builder defaults to using specific propagation targets.
    """

    _propagation_data_extractor: str = AwsSpanProcessingUtil.get_ingress_operation
    _propagation_data_key: str = AwsAttributeKeys.AWS_LOCAL_OPERATION
    _attributes_keys_to_propagate: List[str] = [
        AwsAttributeKeys.AWS_REMOTE_SERVICE,
        AwsAttributeKeys.AWS_REMOTE_OPERATION,
    ]

    def __init__(self):
        pass

    def set_propagation_data_extractor(
        self, propagation_data_extractor: Callable[[ReadableSpan], str]
    ) -> "AwsAttributePropagatingSpanProcessorBuilder":
        if propagation_data_extractor is None:
            raise ValueError("propagation_data_extractor must not be None")
        self._propagation_data_extractor = propagation_data_extractor
        return self

    def set_propagation_data_key(self, propagation_data_key: str) -> "AwsAttributePropagatingSpanProcessorBuilder":
        if propagation_data_key is None:
            raise ValueError("propagation_data_key must not be None")
        self._propagation_data_key = propagation_data_key
        return self

    def set_attributes_keys_to_propagate(
        self, attributes_keys_to_propagate: List[str]
    ) -> "AwsAttributePropagatingSpanProcessorBuilder":
        if attributes_keys_to_propagate is None:
            raise ValueError("attributes_keys_to_propagate must not be None")
        self._attributes_keys_to_propagate = attributes_keys_to_propagate
        return self

    def build(self) -> AwsAttributePropagatingSpanProcessor:
        return AwsAttributePropagatingSpanProcessor(
            self._propagation_data_extractor, self._propagation_data_key, self._attributes_keys_to_propagate
        )
