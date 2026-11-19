# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Child process for test_genai_isolation.py; not a test module.

Usage: ``python -I child_isolation.py <scenario> <workdir>``. ``workdir`` holds the sentinel instrumentor's
module and distribution metadata, and for the ``shadow`` scenario an application's own ``plugins/`` package.
Runs the real ``_load_instrumentors`` with only this package's hook entry points and the sentinel, then prints a
JSON report on stdout.
"""
import importlib.abc
import json
import logging
import sys
import traceback

PLUGIN_DIST = "cloudwatch-plugin-otel"
SENTINEL_DIST = "cw-plugin-sentinel"


class _HideOpenTelemetrySdk(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "opentelemetry.sdk" or fullname.startswith("opentelemetry.sdk."):
            raise ModuleNotFoundError(f"No module named {fullname!r} (hidden by the test)", name=fullname)
        return None


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        exc = "".join(traceback.format_exception(*record.exc_info)) if record.exc_info else ""
        self.records.append({"name": record.name, "level": record.levelname, "msg": record.getMessage(), "exc": exc})


def main():
    scenario, workdir = sys.argv[1], sys.argv[2]
    sys.path.insert(0, workdir)
    collector = _Collect()
    if scenario != "shadow-no-logging":
        root = logging.getLogger()
        root.addHandler(collector)
        root.setLevel(logging.DEBUG)

    if scenario == "no_sdk":
        sys.meta_path.insert(0, _HideOpenTelemetrySdk())
    elif scenario == "raising_step":
        # pylint: disable=import-outside-toplevel
        from plugins.opentelemetry.cloudwatch import _auto_instrumentation

        def raising_step():
            raise RuntimeError("injected step failure")

        _auto_instrumentation._PRE_STEPS.insert(0, ("test.raising_step", raising_step))

    # pylint: disable=import-outside-toplevel
    from opentelemetry.instrumentation.auto_instrumentation import _load
    from opentelemetry.instrumentation.distro import DefaultDistro

    real_entry_points = _load.entry_points
    seen = {}

    def restricted(**params):
        group = params.get("group")
        wanted = SENTINEL_DIST if group == "opentelemetry_instrumentor" else PLUGIN_DIST
        eps = [ep for ep in real_entry_points(**params) if ep.dist is not None and ep.dist.name == wanted]
        seen[group] = [ep.value for ep in eps]
        return eps

    _load.entry_points = restricted
    _load._load_instrumentors(DefaultDistro())

    import cw_plugin_sentinel  # pylint: disable=import-outside-toplevel,import-error

    plugins = sys.modules.get("plugins")
    report = {
        "loader_returned": True,
        "sentinel_ran": cw_plugin_sentinel.RAN,
        "entry_points": seen,
        "plugins_file": getattr(plugins, "__file__", None),
        "sdk_imported": "opentelemetry.sdk" in sys.modules,
        "records": collector.records,
    }
    print(json.dumps(report))


if __name__ == "__main__":
    main()
