# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared constants for the framework instrumentations (Flask, FastAPI, Django).

Keeping these in one module makes cross-framework behavior structural rather than a
promise enforced by comments — a single edit here changes every framework at once.
"""

# Route label for a request that matched no URL rule (unmatched 404s, scanner/bot traffic
# to nonexistent URLs like /wp-admin or /.env). Recording the raw path for these would make
# every probed URL its own metric series — a cardinality explosion. Collapsing to one
# sentinel keeps the 404-rate signal without the cardinality, identically across frameworks.
#
# Note: this value also flows through ServiceEventsConfig.should_track_endpoint(route, ...).
# With the default (empty) endpoint filters every request is tracked, so unmatched requests
# are still recorded under this label. A customer who sets endpoint_include_patterns scopes
# tracking to matching routes only, so unmatched requests are intentionally excluded then.
UNMATCHED_ROUTE = "<unmatched>"
