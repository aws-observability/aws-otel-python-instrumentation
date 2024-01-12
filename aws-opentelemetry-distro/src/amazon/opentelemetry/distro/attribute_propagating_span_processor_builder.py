# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Tuple

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_LOCAL_OPERATION,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
)
from amazon.opentelemetry.distro._aws_span_processing_util import get_ingress_operation
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan


class AttributePropagatingSpanProcessorBuilder:
    """
    AttributePropagatingSpanProcessorBuilder is used to construct a AttributePropagatingSpanProcessor.
    If set_propagation_data_extractor, set_propagation_data_key or set_attributes_keys_to_propagate are not invoked,
    the builder defaults to using specific propagation targets.
    """

    _propagation_data_extractor: str = get_ingress_operation
    _propagation_data_key: str = AWS_LOCAL_OPERATION
    _attributes_keys_to_propagate: Tuple[str, ...] = (
        AWS_REMOTE_SERVICE,
        AWS_REMOTE_OPERATION,
    )

    def set_propagation_data_extractor(
        self, propagation_data_extractor: Callable[[ReadableSpan], str]
    ) -> "AttributePropagatingSpanProcessorBuilder":
        if propagation_data_extractor is None:
            raise ValueError("propagation_data_extractor must not be None")
        self._propagation_data_extractor = propagation_data_extractor
        return self

    def set_propagation_data_key(self, propagation_data_key: str) -> "AttributePropagatingSpanProcessorBuilder":
        if propagation_data_key is None:
            raise ValueError("propagation_data_key must not be None")
        self._propagation_data_key = propagation_data_key
        return self

    def set_attributes_keys_to_propagate(
        self, attributes_keys_to_propagate: List[str]
    ) -> "AttributePropagatingSpanProcessorBuilder":
        if attributes_keys_to_propagate is None:
            raise ValueError("attributes_keys_to_propagate must not be None")
        self._attributes_keys_to_propagate = tuple(attributes_keys_to_propagate)
        return self

    def build(self) -> AttributePropagatingSpanProcessor:
        return AttributePropagatingSpanProcessor(
            self._propagation_data_extractor, self._propagation_data_key, self._attributes_keys_to_propagate
        )
