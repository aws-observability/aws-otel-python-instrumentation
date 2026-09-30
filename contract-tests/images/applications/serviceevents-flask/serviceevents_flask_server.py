# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import os

from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


@app.route("/health")
def health():
    return "Ready"


@app.route("/success")
def success():
    # Deferred import: helpers must be imported after the AST hooks are active
    # so its functions get instrumented (see helpers.py module docstring).
    # pylint: disable-next=import-outside-toplevel
    from helpers import BusinessLogic, compute_result

    bl = BusinessLogic()
    result = bl.process("test_data")
    compute_result(42)
    return jsonify({"status": "ok", "result": result})


@app.route("/error")
def error():
    return jsonify({"error": "bad request"}), 400


@app.route("/fault")
def fault():
    raise RuntimeError("Intentional server fault")


@app.route("/exception")
def exception_endpoint():
    # Deferred import (see /success): keep helpers import inside the view.
    # pylint: disable-next=import-outside-toplevel
    from helpers import validate_input

    validate_input(None)


if __name__ == "__main__":
    threaded = os.environ.get("FLASK_THREADED", "true").lower() == "true"
    print("Ready", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=threaded)
