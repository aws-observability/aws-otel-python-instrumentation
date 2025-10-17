# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Patches for OpenTelemetry Aio-Pika instrumentation to add code correlation support.
"""

import functools
import logging

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import get_code_correlation_enabled_status
from amazon.opentelemetry.distro.code_correlation.utils import add_code_attributes_to_span
from opentelemetry import trace

logger = logging.getLogger(__name__)


def patch_callback_decorator_decorate(original_decorate):
    """Patch CallbackDecorator.decorate to add code attributes to span."""

    @functools.wraps(original_decorate)
    def patched_decorate(self, callback):
        # Decorate the original callback to add code attributes
        async def enhanced_callback(message):
            # Get current active span
            current_span = trace.get_current_span()
            if current_span and current_span.is_recording():
                try:
                    add_code_attributes_to_span(current_span, callback)
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

            # Call original callback
            return await callback(message)

        # Call original decorate method with our enhanced callback
        return original_decorate(self, enhanced_callback)

    return patched_decorate


def _apply_aio_pika_instrumentation_patches():
    """Apply aio-pika patches if code correlation is enabled."""
    try:
        if get_code_correlation_enabled_status() is not True:
            return

        # Import CallbackDecorator inside function to allow proper testing
        try:
            # pylint: disable=import-outside-toplevel
            from opentelemetry.instrumentation.aio_pika.callback_decorator import CallbackDecorator
        except ImportError:
            logger.warning("Failed to apply Aio-Pika patches: CallbackDecorator not available")
            return

        # Patch CallbackDecorator.decorate
        CallbackDecorator.decorate = patch_callback_decorator_decorate(CallbackDecorator.decorate)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Failed to apply Aio-Pika patches: %s", exc)
