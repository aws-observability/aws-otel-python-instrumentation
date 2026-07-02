# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared route helpers for the framework instrumentations (Flask, FastAPI, Django).

Keeping these in one module makes cross-framework behavior structural rather than a
promise enforced by comments — a single edit here changes every framework at once.
"""

from typing import Optional

from amazon.opentelemetry.application_signals.internal.aws_span_processing_util import extract_api_path_value


def unmatched_route_label(raw_path: Optional[str]) -> str:
    """Route label for a request that matched no URL rule (unmatched 404s, scanner/bot
    traffic to nonexistent URLs like /wp-admin or /.env).

    Recording the raw path for these would make every probed URL its own metric series —
    a cardinality explosion. Instead, collapse to the first path segment, e.g.
    "/wp-admin/setup.php" -> "/wp-admin". This is exactly what Application Signals does
    for a span whose name can't be resolved to a route
    (_aws_span_processing_util.extract_api_path_value), so ServiceEvents and Application
    Signals produce the same operation label for unmatched requests. The first-segment
    bound keeps cardinality from a single nonexistent prefix in check while still
    distinguishing the common scanner targets.

    Note: this value also flows through ServiceEventsConfig.should_track_endpoint(route,
    ...). With the default (empty) endpoint filters every request is tracked, so unmatched
    requests are still recorded (under their first-segment label). A customer who sets
    endpoint_include_patterns scopes tracking to matching routes only, so unmatched
    requests are excluded unless a pattern matches the first-segment label.
    """
    # extract_api_path_value treats None/"" as "/" and always returns a leading-slash,
    # first-segment value, identically across frameworks.
    return extract_api_path_value(raw_path or "")
