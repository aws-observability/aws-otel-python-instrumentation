# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

logging.basicConfig(level=logging.INFO)

app = FastAPI()


@app.get("/health")
async def health():
    return PlainTextResponse("Ready")


@app.get("/success")
async def success():
    # Deferred import: helpers must be imported after the AST hooks are active
    # so its functions get instrumented (see helpers.py module docstring).
    # pylint: disable-next=import-outside-toplevel
    from helpers import BusinessLogic, compute_result

    bl = BusinessLogic()
    result = bl.process("test_data")
    compute_result(42)
    return {"status": "ok", "result": result}


@app.get("/error")
async def error():
    return JSONResponse(status_code=400, content={"error": "bad request"})


@app.get("/fault")
async def fault():
    raise RuntimeError("Intentional server fault")


@app.get("/exception")
async def exception_endpoint():
    # Deferred import (see /success): keep helpers import inside the view.
    # pylint: disable-next=import-outside-toplevel
    from helpers import validate_input

    validate_input(None)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": str(exc)})


if __name__ == "__main__":
    print("Ready")
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
