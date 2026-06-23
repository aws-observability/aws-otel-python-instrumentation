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

import logging

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
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


app = Starlette(
    routes=[
        Route("/health", health),
        Route("/success", success),
        Route("/handler", starlette_handler),
    ]
)


if __name__ == "__main__":
    print("Ready", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
