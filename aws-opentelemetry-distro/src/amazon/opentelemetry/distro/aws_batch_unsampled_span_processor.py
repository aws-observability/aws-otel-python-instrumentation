# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Optional

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_TRACE_FLAG_SAMPLED
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor as BaseBatchSpanProcessor

logger = logging.getLogger(__name__)


class BatchUnsampledSpanProcessor(BaseBatchSpanProcessor):

    # pylint: disable=no-self-use
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not span.context.trace_flags.sampled:
            span.set_attribute(AWS_TRACE_FLAG_SAMPLED, False)

    def on_end(self, span: ReadableSpan) -> None:
        if span.context.trace_flags.sampled:
            return

        if self.done:
            logger.warning("Already shutdown, dropping span.")
            return

        if len(self.queue) == self.max_queue_size:
            # pylint: disable=access-member-before-definition
            if not self._spans_dropped:
                logger.warning("Queue is full, likely spans will be dropped.")
                # pylint: disable=attribute-defined-outside-init
                self._spans_dropped = True

        self.queue.appendleft(span)

        if len(self.queue) >= self.max_export_batch_size:
            with self.condition:
                self.condition.notify()
