# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""FastAPI app for DI contract tests.

Starts a mock DI API on port 3030, then runs the FastAPI app on port 8080.
The DI poller will fetch breakpoint/probe configs from the mock API.

Mirrors di_flask_server.py so the same DI assertions apply, and additionally
exercises the async instrumentation path with `async def` target functions
(process_data_async, compute_total_async) -- something a synchronous Flask app
cannot cover.
"""

import functools
import inspect
import logging
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

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

    MaxHits=3 means: allow 3 snapshots, disable on the 4th hit.
    (The check is hit_count > max_hits, so hits 1-3 pass, hit 4 is blocked.)
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


def _partial_base(prefix, value):
    """Underlying function for the functools.partial target below."""
    result = prefix + str(value)
    return result


# functools.partial target: a module-level name bound to a functools.partial. A partial has
# no __qualname__/__name__, so the wrapper's old runtime-name key was "<module>.<anonymous>"
# and missed the breakpoint set registered under "<module>.partial_target" -> silently
# never fired. With the fix the wrapper keys off the configured name, so it fires.
partial_target = functools.partial(_partial_base, "hello:")


async def process_data_async(value):
    """Async target for function-level BREAKPOINT instrumentation.

    Verifies DI correctly wraps an `async def` coroutine (awaits it and
    returns the awaited result) rather than capturing an unawaited coroutine.
    """
    result = value * 2
    return result


async def compute_total_async(items):
    """Async target for PROBE instrumentation (method-level only)."""
    total = 0
    for item in items:
        total += item
    return total


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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "process_data",
                "FilePath": "di_fastapi_server.py",
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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "calculate_sum",
                "FilePath": "di_fastapi_server.py",
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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "limited_function",
                "FilePath": "di_fastapi_server.py",
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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "shared_function",
                "FilePath": "di_fastapi_server.py",
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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "process_long_string",
                "FilePath": "di_fastapi_server.py",
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
                "CodeUnit": "di_fastapi_server",
                "MethodName": "process_large_collection",
                "FilePath": "di_fastapi_server.py",
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
    # Function-level breakpoint on the async target process_data_async
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "process_data_async",
                "FilePath": "di_fastapi_server.py",
            }
        },
        "LocationHash": "aabb000000000009",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["value"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Breakpoint targeting a FastAPI ROUTE HANDLER directly.
    # FastAPI captures a direct reference to the handler in its route table
    # (APIRoute.endpoint / APIRoute.dependant.call) at decoration time, so
    # replacing only the module-level name would not reach it. DI patches the
    # FastAPI route table (mirroring its Flask app.view_functions patch), so a
    # request routed by FastAPI now dispatches through the wrapper and a snapshot
    # IS produced for this handler.
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "route_handler_target",
                "FilePath": "di_fastapi_server.py",
            }
        },
        "LocationHash": "aabb00000000000b",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["multiplier"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Breakpoint targeting a SYNC FastAPI route handler (isolation: sync vs async dispatch).
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "route_handler_target_sync",
                "FilePath": "di_fastapi_server.py",
            }
        },
        "LocationHash": "aabb00000000000c",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["multiplier"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Function-level breakpoint on a functools.partial target. The configured
    # MethodName "partial_target" must match the manager's registration key; the wrapper
    # now keys its lookup off that configured name (a partial has no __qualname__/__name__),
    # so the wrapper fires and a snapshot IS produced.
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "partial_target",
                "FilePath": "di_fastapi_server.py",
            }
        },
        "LocationHash": "aabb00000000000d",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["value"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
]

PROBE_CONFIGS = [
    # PROBE on compute_total
    {
        "InstrumentationType": "PROBE",
        "InstrumentationName": "compute-total-probe",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "compute_total",
                "FilePath": "di_fastapi_server.py",
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
        "InstrumentationName": "shared-function-probe",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "shared_function",
                "FilePath": "di_fastapi_server.py",
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
    # PROBE on the async target compute_total_async
    {
        "InstrumentationType": "PROBE",
        "InstrumentationName": "compute-total-async-probe",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_fastapi_server",
                "MethodName": "compute_total_async",
                "FilePath": "di_fastapi_server.py",
            }
        },
        "LocationHash": "aabb00000000000a",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["items"],
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
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI()


@app.get("/health")
async def health():
    return PlainTextResponse("Ready")


@app.get("/success")
async def success():
    result = process_data(42)
    return {"status": "ok", "result": result}


@app.get("/probe")
async def probe_endpoint():
    """Endpoint that triggers PROBE-instrumented function."""
    total = compute_total([10, 20, 30])
    return {"status": "ok", "total": total}


@app.get("/line-level")
async def line_level_endpoint():
    """Endpoint that triggers line-level BREAKPOINT instrumented function."""
    result = calculate_sum(5, 7)
    return {"status": "ok", "sum": result}


@app.get("/limited")
async def limited_endpoint():
    """Endpoint that triggers hit-limited BREAKPOINT instrumented function."""
    result = limited_function(3)
    return {"status": "ok", "result": result}


@app.get("/shared")
async def shared_endpoint():
    """Endpoint that triggers function with both PROBE and BREAKPOINT."""
    result = shared_function("hello")
    return {"status": "ok", "result": result}


@app.get("/limits-string")
async def limits_string_endpoint():
    """Endpoint that triggers string truncation limit validation."""
    long_string = "A" * 500
    result = process_long_string(long_string)
    return {"status": "ok", "length": result}


@app.get("/limits-collection")
async def limits_collection_endpoint():
    """Endpoint that triggers collection width limit validation."""
    large_list = list(range(1, 51))
    result = process_large_collection(large_list)
    return {"status": "ok", "size": result}


@app.get("/success-async")
async def success_async_endpoint():
    """Endpoint that triggers an async BREAKPOINT-instrumented function."""
    result = await process_data_async(42)
    return {"status": "ok", "result": result}


@app.get("/probe-async")
async def probe_async_endpoint():
    """Endpoint that triggers an async PROBE-instrumented function."""
    total = await compute_total_async([10, 20, 30])
    return {"status": "ok", "total": total}


@app.get("/route-handler-target")
async def route_handler_target(multiplier: int = 2):
    """Async route handler that is ITSELF a DI BREAKPOINT target.

    DI is configured (LocationHash aabb00000000000b) to instrument this very
    handler. FastAPI holds its own reference to the handler in the route table
    (APIRoute.endpoint / APIRoute.dependant.call) captured at decoration time;
    DI patches that route table, so the wrapper is invoked when FastAPI routes
    the request and a snapshot IS produced. The body is fully self-contained
    (it does not call any other instrumented function) so a snapshot attributed
    to "route_handler_target" unambiguously came from instrumenting the handler.
    """
    result = multiplier * 21
    return {"status": "ok", "result": result}


@app.get("/route-handler-target-sync")
def route_handler_target_sync(multiplier: int = 2):
    """SYNC route handler that is itself a DI BREAKPOINT target.

    FastAPI runs sync handlers in a threadpool (vs async handlers on the event
    loop), so this exercises a different dispatch path than the async route
    handler above while using the same DI route-table patch. A snapshot IS
    produced for this handler.
    """
    result = multiplier * 21
    return {"status": "ok", "result": result}


@app.get("/partial")
async def partial_endpoint():
    """Endpoint that triggers the functools.partial BREAKPOINT target."""
    result = partial_target(9)
    return {"status": "ok", "result": result}


@app.get("/error")
async def error():
    return JSONResponse(status_code=400, content={"error": "bad request"})


@app.get("/fault")
async def fault():
    process_data(-1)
    raise RuntimeError("Intentional fault")


if __name__ == "__main__":
    print("Ready", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
