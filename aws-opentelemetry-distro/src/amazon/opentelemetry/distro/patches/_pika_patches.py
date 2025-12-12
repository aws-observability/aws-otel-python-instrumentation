# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Patches for OpenTelemetry Pika instrumentation to add code correlation support.
"""
# pylint: disable=duplicate-code

import functools
import logging

from amazon.opentelemetry.distro.code_correlation.utils import add_code_attributes_to_span

logger = logging.getLogger(__name__)


def patch_decorate_callback(original_decorate_callback):
    """Patch _decorate_callback to add code attributes to span."""

    @functools.wraps(original_decorate_callback)
    def patched_decorate_callback(callback, tracer, task_name, consume_hook):
        # Create an enhanced consume_hook that adds code attributes
        def enhanced_consume_hook(span, body, properties):
            # First add code attributes for the callback
            if span and span.is_recording():
                try:
                    add_code_attributes_to_span(span, callback)
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

            try:
                consume_hook(span, body, properties)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

        # Call original with our enhanced hook
        return original_decorate_callback(callback, tracer, task_name, enhanced_consume_hook)

    return patched_decorate_callback


def _apply_pika_instrumentation_patches():
    """Apply pika patches if code correlation is enabled."""
    try:
        # Import pika_utils inside function to allow proper testing
        try:
            # pylint: disable=import-outside-toplevel
            from opentelemetry.instrumentation.pika import utils as pika_utils
        except ImportError:
            logger.warning("Failed to apply Pika patches: pika utils not available")
            return

        # Patch _decorate_callback
        # pylint: disable=protected-access
        pika_utils._decorate_callback = patch_decorate_callback(pika_utils._decorate_callback)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Failed to apply Pika patches: %s", exc)
