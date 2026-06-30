# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pure-Starlette app for DI contract tests.

Starts a mock DI API on port 3030, then runs a pure Starlette app (NOT FastAPI)
on port 8080. The DI poller fetches breakpoint configs from the mock API.

The key case this app exercises: a function-level breakpoint on a *Starlette route
handler*. Starlette builds ``route.app = request_response(endpoint)`` at import time,
capturing the original handler in a closure; the per-request path invokes ``route.app``,
never ``route.endpoint``. So replacing the module-level name (or rebinding endpoint)
does not reach the live handler and the breakpoint silently never fires unless DI
rebuilds ``route.app``. A plain (non-handler) function is also instrumented as a control
to prove DI is active in this process.
"""

import asyncio
import logging

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Target functions
# ---------------------------------------------------------------------------


def process_data(value):
    """Control target: a plain module-level function (not a route handler).

    Proves DI is active in this Starlette process independently of the route-handler
    patching path.
    """
    result = value * 2
    return result


async def starlette_handler(request):
    """A Starlette route handler that is ITSELF a DI BREAKPOINT target.

    Body is self-contained so a snapshot attributed to "starlette_handler" unambiguously
    came from instrumenting the handler (not some inner function).
    """
    multiplier = int(request.query_params.get("multiplier", "2"))
    result = multiplier * 21
    return JSONResponse({"status": "ok", "result": result})


async def number_stream(count):
    for i in range(count):
        await asyncio.sleep(0)
        yield f"chunk-{i}\n".encode()


async def sse_event_stream(request, count):
    for i in range(count):
        if await request.is_disconnected():
            break
        await asyncio.sleep(0)
        yield f"data: {i}\n\n"


async def fetch_remote_value(key):
    await asyncio.sleep(0.01)
    value = len(key) * 7
    await asyncio.sleep(0.01)
    return value


async def lookup_or_404(item_id):
    await asyncio.sleep(0.01)
    if item_id < 0:
        raise HTTPException(status_code=404, detail=f"item {item_id} not found")
    return {"item_id": item_id}


# ---------------------------------------------------------------------------
# Mock DI API configuration
# ---------------------------------------------------------------------------

from mock_di_api import set_breakpoint_configs, set_probe_configs, start_mock_api  # noqa: E402

BREAKPOINT_CONFIGS = [
    # Control: plain module-level function.
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "process_data",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000001",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["value"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # The Starlette route handler itself.
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "starlette_handler",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000002",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["request"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "number_stream",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000003",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["count"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "sse_event_stream",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000004",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["count"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "fetch_remote_value",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000005",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["key"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": "di_starlette_server",
                "MethodName": "lookup_or_404",
                "FilePath": "di_starlette_server.py",
            }
        },
        "LocationHash": "ccdd000000000006",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["item_id"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
]

set_breakpoint_configs(BREAKPOINT_CONFIGS)
set_probe_configs([])
start_mock_api(port=3030)
logger.info("Mock DI API started on port 3030")


# ---------------------------------------------------------------------------
# Starlette application
# ---------------------------------------------------------------------------


async def health(request):
    return PlainTextResponse("Ready")


async def success(request):
    """Triggers the plain-function control target."""
    result = process_data(42)
    return JSONResponse({"status": "ok", "result": result})


async def stream_handler(request):
    return StreamingResponse(number_stream(5), media_type="text/plain")


async def sse_handler(request):
    return StreamingResponse(sse_event_stream(request, 4), media_type="text/event-stream")


async def await_io_handler(request):
    value = await fetch_remote_value("contract")
    return JSONResponse({"status": "ok", "value": value})


async def lookup_handler(request):
    item_id = int(request.query_params.get("item_id", "1"))
    result = await lookup_or_404(item_id)
    return JSONResponse({"status": "ok", **result})


app = Starlette(
    routes=[
        Route("/health", health),
        Route("/success", success),
        Route("/handler", starlette_handler),
        Route("/stream", stream_handler),
        Route("/sse", sse_handler),
        Route("/await-io", await_io_handler),
        Route("/lookup", lookup_handler),
    ]
)


if __name__ == "__main__":
    print("Ready", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
