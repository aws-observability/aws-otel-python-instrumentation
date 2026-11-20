# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""``session.id`` on every span of a request, from W3C baggage ``session.id`` (a post step).

AgentCore Evaluations and the CloudWatch Sessions view group spans by the ``session.id`` span attribute, and
upstream GenAI instrumentations set only ``gen_ai.conversation.id``. Evaluation also needs the attribute on every
span of a session (mixed presence splits it in two), so it is set at span start from the request context, which
the SERVER span and everything under it share. AgentCore Runtime sends the baggage with every request; elsewhere
the app puts it in baggage.

Not used: the bedrock-agentcore SDK's ``BedrockAgentCoreContext`` session id. It is set inside the app's handler,
so the SERVER span (started before the handler runs) would lack the attribute.
"""

from typing import Optional

from plugins.opentelemetry.cloudwatch.genai._pipeline import (
    NOT_SDK,
    genai_instrumentation_active,
    has_span_processor,
    sdk_tracer_provider,
)

from opentelemetry import baggage
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Span, SpanProcessor

SESSION_ID = "session.id"
ON = "on"


class SessionIdSpanProcessor(SpanProcessor):
    """Sets ``session.id`` at span start from the parent context's baggage, unless the span already has one."""

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:  # pylint: disable=no-self-use
        if SESSION_ID in (span.attributes or {}):
            return
        value = baggage.get_baggage(SESSION_ID, parent_context)  # a None context means the current one
        if value:
            span.set_attribute(SESSION_ID, str(value))


def session_step() -> Optional[str]:
    """The ``session.id`` post step: the summary outcome, or None when no bridged GenAI instrumentation is
    active."""
    if not genai_instrumentation_active():
        return None
    provider = sdk_tracer_provider()
    if provider is None:
        return NOT_SDK
    if not has_span_processor(provider, SessionIdSpanProcessor):
        provider.add_span_processor(SessionIdSpanProcessor())
    return ON
