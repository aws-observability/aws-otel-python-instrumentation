# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Child process for test_genai_non_genai.py; not a test module.

Usage: ``python -I child_non_genai.py``. A Flask app under the real loader with this package's hooks and the
Flask instrumentor; prints which GenAI modules ended up imported.
"""
import json
import logging
import sys

from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro

GENAI_MODULES = (
    "plugins.opentelemetry.cloudwatch",
    "plugins.opentelemetry.cloudwatch.genai.content_capture",
    "plugins.opentelemetry.cloudwatch.genai.langchain",
    "opentelemetry.util.genai",
)


def main():
    real_entry_points = _load.entry_points

    def restricted(**params):
        eps = real_entry_points(**params)
        if params.get("group") == "opentelemetry_instrumentor":
            return [ep for ep in eps if ep.name == "flask"]
        return [ep for ep in eps if ep.dist is not None and ep.dist.name == "cloudwatch-plugin-otel"]

    _load.entry_points = restricted
    _load._load_instrumentors(DefaultDistro())

    import flask  # pylint: disable=import-outside-toplevel

    flask.Flask(__name__).test_client().get("/")
    loggers = {
        name: logging.getLogger(name)
        for name in ("plugins.opentelemetry.cloudwatch", "plugins.opentelemetry.cloudwatch.genai")
    }
    report = {
        "imported": [m for m in GENAI_MODULES if m in sys.modules],
        "loggers": {name: [logger.level, len(logger.handlers)] for name, logger in loggers.items()},
    }
    print(json.dumps(report))


if __name__ == "__main__":
    main()
