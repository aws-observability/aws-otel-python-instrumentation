# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Exports are resolved lazily so environment-variable imports stay lightweight.
# pylint: disable=undefined-all-variable
__all__ = ["GenAINestedClientSpanProcessor", "LLOHandler"]
# pylint: enable=undefined-all-variable


def __getattr__(name: str):
    """Load public helpers when requested."""
    if name == "GenAINestedClientSpanProcessor":
        # pylint: disable-next=import-outside-toplevel
        from amazon.opentelemetry.distro.genai.gen_ai_nested_client_span_processor import GenAINestedClientSpanProcessor

        return GenAINestedClientSpanProcessor
    if name == "LLOHandler":
        from amazon.opentelemetry.distro.genai.llo_handler import LLOHandler  # pylint: disable=import-outside-toplevel

        return LLOHandler
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
