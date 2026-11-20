# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The agent's identity as resource attributes, with no configuration (a post step).

- ``aws.service.type=gen_ai_agent``: the CloudWatch GenAI console's agent list and online evaluation's session
  discovery filter on it, so without it the agent is missing outside AgentCore Runtime.
- On AgentCore Runtime, ``cloud.resource_id`` (the runtime endpoint ARN) and ``cloud.platform``: the console
  groups an agent's tokens, cost and spans by ``cloud.resource_id``, so without it they land in a separate
  ``service.name`` row and the runtime's own row shows none.

The configurator has already built the (immutable) Resource when the post hook runs, so the attributes are merged
into the tracer and logger providers' resources: spans and logs carry them. A key already on the resource is never
overridden; that includes every key in ``OTEL_RESOURCE_ATTRIBUTES``, which the configurator folds into the
resource. The meter provider is not updated, so span-metric series do not carry these attributes.
"""

import os
import re
from typing import Dict, Optional
from urllib.parse import unquote, urlsplit

from plugins.opentelemetry.cloudwatch.genai._pipeline import NOT_SDK, genai_instrumentation_active, sdk_tracer_provider
from plugins.opentelemetry.cloudwatch.genai._settings import debug_once

from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.resources import Resource

AWS_SERVICE_TYPE = "aws.service.type"
CLOUD_RESOURCE_ID = "cloud.resource_id"
CLOUD_PLATFORM = "cloud.platform"
AGENTCORE_RUNTIME_URL = "AGENTCORE_RUNTIME_URL"
# `https://bedrock-agentcore.<region>.amazonaws.com/runtimes/<url-encoded runtime ARN>/invocations`
_RUNTIME_ARN = re.compile(r"arn:aws(-[a-z]+)*:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:runtime/[A-Za-z0-9_-]+")

ALREADY_SET = "already set"
NO_SDK_RESOURCE = "skipped (no SDK Resource)"


# CW-PLUGIN-WORKAROUND(expires: 2027-03-31; retire-when: the CloudWatch GenAI console and online evaluation discover
# agents from invoke_agent spans)
SERVICE_TYPE = {AWS_SERVICE_TYPE: "gen_ai_agent"}


# CW-PLUGIN-WORKAROUND(expires: 2027-03-31; retire-when: AgentCore Runtime injects cloud.resource_id/cloud.platform
# for containers that disable ADOT. The endpoint is assumed to be DEFAULT, which is wrong for other endpoints)
def _runtime_identity() -> Dict[str, str]:
    """The AgentCore Runtime identity, from the runtime URL AgentCore Runtime sets, or {} if it is unset or not
    recognized. ``cloud.resource_id`` has the form AgentCore Runtime injects for ADOT,
    ``<runtime ARN>/runtime-endpoint/DEFAULT:DEFAULT``. The URL carries the runtime ARN but not the endpoint name,
    so the endpoint is assumed to be DEFAULT: an agent invoked through another endpoint gets the wrong id."""
    url = os.environ.get(AGENTCORE_RUNTIME_URL, "").strip()
    if not url:
        return {}
    try:
        parts = urlsplit(url).path.strip("/").split("/")
    except ValueError:  # for example an unbalanced IPv6 bracket
        parts = []
    arn = unquote("/".join(parts[1:-1])) if parts[:1] == ["runtimes"] and parts[-1:] == ["invocations"] else ""
    if not _RUNTIME_ARN.fullmatch(arn):
        debug_once("runtime url", "%s=%r has no AgentCore runtime ARN; no runtime identity", AGENTCORE_RUNTIME_URL, url)
        return {}
    return {CLOUD_RESOURCE_ID: arn + "/runtime-endpoint/DEFAULT:DEFAULT", CLOUD_PLATFORM: "aws_bedrock_agentcore"}


def _missing(provider, wanted: Dict[str, str]) -> Dict[str, str]:
    """The ``wanted`` attributes the provider's resource does not have."""
    return {key: value for key, value in wanted.items() if key not in provider.resource.attributes}


def _merge(provider, attributes: Dict[str, str]) -> None:
    """Merges ``attributes`` into an SDK tracer or logger provider's resource, including the tracers or loggers it
    already handed out (they keep their own reference). Private SDK internals:

    - SDK >= 1.44: ``_update_resource`` does exactly this.
    - SDK 1.42 and 1.43: ``_resource`` on the provider, and the tracers in ``_tracers`` (each reads ``resource`` when
      it starts a span) or the loggers in ``_active_loggers`` (each reads ``_resource`` when it emits).
    """
    extra = Resource(attributes)
    update = getattr(provider, "_update_resource", None)
    if callable(update):
        update(extra)
        return
    # pylint: disable=protected-access
    provider._resource = provider._resource.merge(extra)
    for tracer in list(getattr(provider, "_tracers", {}).values()):
        tracer.resource = provider._resource
    for logger in list(getattr(provider, "_active_loggers", ())):
        logger._resource = provider._resource


def agent_identity_step() -> Optional[str]:
    """The ``agent identity`` post step: what it added, or None when no bridged GenAI instrumentation is active."""
    if not genai_instrumentation_active():
        return None
    provider = sdk_tracer_provider()
    if provider is None:
        return NOT_SDK
    if not isinstance(getattr(provider, "resource", None), Resource):
        return NO_SDK_RESOURCE
    added = _missing(provider, {**SERVICE_TYPE, **_runtime_identity()})
    if not added:
        return ALREADY_SET
    _merge(provider, added)
    logger_provider = _logs.get_logger_provider()
    logger_added = _missing(logger_provider, added) if isinstance(logger_provider, LoggerProvider) else None
    if logger_added:
        _merge(logger_provider, logger_added)
    labels = ["service.type"] if AWS_SERVICE_TYPE in added else []
    if CLOUD_RESOURCE_ID in added or CLOUD_PLATFORM in added:
        labels.append("runtime")
    return "+".join(labels)
