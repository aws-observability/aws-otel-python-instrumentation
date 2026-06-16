# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Flask app for DI contract tests.

Starts a mock DI API on port 3030, then runs the Flask app on port 8080.
The DI poller will fetch breakpoint/probe configs from the mock API.
"""

import inspect
import logging
import time

from flask import Flask, jsonify

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
# This avoids hardcoding a line number that breaks when the file is edited.
_CALCULATE_SUM_LINE = None
_src_lines, _start = inspect.getsourcelines(calculate_sum)
for _i, _line in enumerate(_src_lines):
    if "result = a + b" in _line:
        _CALCULATE_SUM_LINE = _start + _i
        break
assert _CALCULATE_SUM_LINE is not None, "Could not find 'result = a + b' in calculate_sum"


def limited_function(x):
    """Target function with hit limit (MaxHits=3).

    MaxHits=3 means: allow 2 snapshots, disable at 3rd hit.
    (The check is hit_count >= max_hits, so hit 1&2 pass, hit 3 is blocked.)
    """
    return x * 10


def shared_function(data):
    """Target function with BOTH PROBE and BREAKPOINT instrumentation.

    Tests that both instrumentation types can coexist on the same function.
    """
    processed = data.upper() if isinstance(data, str) else str(data)
    return processed


def process_long_string(long_string):
    """BREAKPOINT target for string truncation limit validation.

    Config requests MaxStringLength=9999 which gets clamped to 255.
    The input string is 500 chars, so the captured value should be truncated at 255.
    """
    return len(long_string)


def process_large_collection(large_list):
    """BREAKPOINT target for collection width limit validation.

    Config requests MaxCollectionWidth=9999 which gets clamped to 20.
    The input list has 50 elements, so only the first 20 should be captured.
    """
    return len(large_list)


def process_small_limit_string(small_limit_string):
    """BREAKPOINT target for small (below-max) string limit validation.

    Config requests MaxStringLength=10 (well within range). The input string is
    100 chars, so a function-level capture that honors the config should truncate
    to exactly 10 -- proving the function path respects user-supplied limits rather
    than always using the maximum.
    """
    return len(small_limit_string)


# ---------------------------------------------------------------------------
# Mock DI API configuration
# ---------------------------------------------------------------------------

from mock_di_api import set_breakpoint_configs, set_probe_configs, start_mock_api  # noqa: E402

BREAKPOINT_CONFIGS = [
    # Function-level breakpoint on process_data
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "process_data",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000001",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["value"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Line-level breakpoint on calculate_sum (captures locals at specific line)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "calculate_sum",
                "FilePath": "di_flask_server.py",
                "LineNumber": _CALCULATE_SUM_LINE,
            }
        },
        "LocationHash": "aabb000000000003",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureLocals": True,
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Breakpoint with low hit limit on limited_function
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "limited_function",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000004",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["x"],
                "CaptureLimits": {"MaxStringLength": 255, "MaxHits": 3},
            }
        },
    },
    # Breakpoint on shared_function (coexists with PROBE)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "shared_function",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000005",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["data"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Breakpoint for string truncation limit validation (MaxStringLength=9999 -> clamped to 255)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "process_long_string",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000007",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["long_string"],
                "CaptureLimits": {"MaxStringLength": 9999},
            }
        },
    },
    # Breakpoint for collection width limit validation (MaxCollectionWidth=9999 -> clamped to 20)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "process_large_collection",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000008",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["large_list"],
                "CaptureLimits": {"MaxCollectionWidth": 9999},
            }
        },
    },
    # Breakpoint for small (below-max) string limit validation (MaxStringLength=10).
    # Verifies the function-level path honors a user-supplied limit instead of the maximum.
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "process_small_limit_string",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000009",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["small_limit_string"],
                "CaptureLimits": {"MaxStringLength": 10},
            }
        },
    },
]

PROBE_CONFIGS = [
    # PROBE on compute_total
    {
        "InstrumentationType": "PROBE",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "compute_total",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000002",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["items"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # PROBE on shared_function (coexists with BREAKPOINT)
    {
        "InstrumentationType": "PROBE",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_flask_server",
                "MethodName": "shared_function",
                "FilePath": "di_flask_server.py",
            }
        },
        "LocationHash": "aabb000000000006",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["data"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
]

set_breakpoint_configs(BREAKPOINT_CONFIGS)
set_probe_configs(PROBE_CONFIGS)
start_mock_api(port=3030)
logger.info("Mock DI API started on port 3030 (line-level target line: %d)", _CALCULATE_SUM_LINE)


# ---------------------------------------------------------------------------
# Flask application
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/health")
def health():
    return "Ready"


@app.route("/success")
def success():
    result = process_data(42)
    return jsonify({"status": "ok", "result": result})


@app.route("/probe")
def probe_endpoint():
    """Endpoint that triggers PROBE-instrumented function."""
    total = compute_total([10, 20, 30])
    return jsonify({"status": "ok", "total": total})


@app.route("/line-level")
def line_level_endpoint():
    """Endpoint that triggers line-level BREAKPOINT instrumented function."""
    result = calculate_sum(5, 7)
    return jsonify({"status": "ok", "sum": result})


@app.route("/limited")
def limited_endpoint():
    """Endpoint that triggers hit-limited BREAKPOINT instrumented function."""
    result = limited_function(3)
    return jsonify({"status": "ok", "result": result})


@app.route("/shared")
def shared_endpoint():
    """Endpoint that triggers function with both PROBE and BREAKPOINT."""
    result = shared_function("hello")
    return jsonify({"status": "ok", "result": result})


@app.route("/limits-string")
def limits_string_endpoint():
    """Endpoint that triggers string truncation limit validation."""
    long_string = "A" * 500
    result = process_long_string(long_string)
    return jsonify({"status": "ok", "length": result})


@app.route("/limits-collection")
def limits_collection_endpoint():
    """Endpoint that triggers collection width limit validation."""
    large_list = list(range(1, 51))
    result = process_large_collection(large_list)
    return jsonify({"status": "ok", "size": result})


@app.route("/limits-small-string")
def limits_small_string_endpoint():
    """Endpoint that triggers small (below-max) string limit validation."""
    small_limit_string = "B" * 100
    result = process_small_limit_string(small_limit_string)
    return jsonify({"status": "ok", "length": result})


@app.route("/error")
def error():
    return jsonify({"error": "bad request"}), 400


@app.route("/fault")
def fault():
    process_data(-1)
    raise RuntimeError("Intentional fault")


if __name__ == "__main__":
    print("Ready", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=False)
