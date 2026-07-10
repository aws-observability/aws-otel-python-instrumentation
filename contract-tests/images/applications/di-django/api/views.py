# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: skip-file
"""Django views + DI target functions for DI contract tests.

The target functions live in this module so URLPattern.callback in the
sibling `di_django_server.urls` module holds direct references to them at
import time — the exact cross-module case the instrumentation engine must
handle by mutating each function's `__code__` in place (so Django's stored
callback reference sees the rewritten code with no framework patching).
"""

import inspect
import time

from django.http import HttpResponse, JsonResponse

# ---------------------------------------------------------------------------
# Target functions (defined before configs so we can inspect line numbers)
# ---------------------------------------------------------------------------


def process_data(value):
    """Target function for BREAKPOINT instrumentation (function-level)."""
    result = value * 2
    time.sleep(0.01)
    return result


def compute_total(items):
    """Target function for PROBE instrumentation (method-level only)."""
    total = 0
    for item in items:
        total += item
    return total


def calculate_sum(a, b):
    result = a + b  # line-level breakpoint target
    return result


# Resolve the actual line number for the line-level breakpoint dynamically.
_CALCULATE_SUM_LINE = None
_src_lines, _start = inspect.getsourcelines(calculate_sum)
for _i, _line in enumerate(_src_lines):
    if "result = a + b" in _line:
        _CALCULATE_SUM_LINE = _start + _i
        break
assert _CALCULATE_SUM_LINE is not None, "Could not find 'result = a + b' in calculate_sum"


def limited_function(x):
    """Target function with hit limit (MaxHits=3)."""
    return x * 10


def shared_function(data):
    """Target function with BOTH PROBE and BREAKPOINT instrumentation."""
    processed = data.upper() if isinstance(data, str) else str(data)
    return processed


def process_long_string(long_string):
    """BREAKPOINT target for string truncation limit validation."""
    return len(long_string)


def process_large_collection(large_list):
    """BREAKPOINT target for collection width limit validation."""
    return len(large_list)


# ---------------------------------------------------------------------------
# HTTP views
# ---------------------------------------------------------------------------


def health(_request):
    return HttpResponse("Ready")


def success(_request):
    result = process_data(42)
    return JsonResponse({"status": "ok", "result": result})


def probe_endpoint(_request):
    """Endpoint that triggers PROBE-instrumented function."""
    total = compute_total([10, 20, 30])
    return JsonResponse({"status": "ok", "total": total})


def line_level_endpoint(_request):
    """Endpoint that triggers line-level BREAKPOINT instrumented function."""
    result = calculate_sum(5, 7)
    return JsonResponse({"status": "ok", "sum": result})


def limited_endpoint(_request):
    """Endpoint that triggers hit-limited BREAKPOINT instrumented function."""
    result = limited_function(3)
    return JsonResponse({"status": "ok", "result": result})


def shared_endpoint(_request):
    """Endpoint that triggers function with both PROBE and BREAKPOINT."""
    result = shared_function("hello")
    return JsonResponse({"status": "ok", "result": result})


def limits_string_endpoint(_request):
    """Endpoint that triggers string truncation limit validation."""
    long_string = "A" * 500
    result = process_long_string(long_string)
    return JsonResponse({"status": "ok", "length": result})


def limits_collection_endpoint(_request):
    """Endpoint that triggers collection width limit validation."""
    large_list = list(range(1, 51))
    result = process_large_collection(large_list)
    return JsonResponse({"status": "ok", "size": result})


def error_endpoint(_request):
    return JsonResponse({"error": "bad request"}, status=400)


def fault_endpoint(_request):
    process_data(-1)
    raise RuntimeError("Intentional fault")
