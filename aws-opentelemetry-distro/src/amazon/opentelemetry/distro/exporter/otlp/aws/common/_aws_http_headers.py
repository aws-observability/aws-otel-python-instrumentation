# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled
from amazon.opentelemetry.distro.version import __version__


def build_user_agent() -> str:
    user_agent = f"ADOT-Python-{__version__}"

    if is_agent_observability_enabled():
        user_agent = f"ADOT-Python-GenAI-{__version__}"

    return user_agent


_OTLP_AWS_HTTP_HEADERS = {
    "User-Agent": build_user_agent(),
}
