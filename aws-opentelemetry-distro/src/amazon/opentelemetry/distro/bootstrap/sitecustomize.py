# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Bootstrap for injected ADOT Python auto-instrumentation.

The root Dockerfile copies this file over upstream's
``opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py`` in the
auto-instrumentation image. The operator puts that directory and the payload
root on ``PYTHONPATH``, so every Python process in an instrumented container
imports this module at startup, whatever its Python version.

The payload is resolved under a supported Python and can break an older
interpreter: it shadows the application's own packages, such as ``urllib3``.
This module therefore checks the running Python before anything from the
payload is imported, and on an unsupported Python removes the payload from
``sys.path`` so that the application starts without telemetry.

Constraints:
- Standard library only, and syntax that parses on Python 3.6: no f-strings,
  no type hints, no walrus operator.
- Never raise: every path ends with the application continuing.
"""
# pylint: disable=consider-using-f-string,broad-exception-caught

import json
import logging
import os
import sys
import time

# Must match ``requires-python`` in aws-opentelemetry-distro/pyproject.toml.
MINIMUM_PYTHON = (3, 10)

STATUS_FILE_NAME = "adot-status.json"
LOG_PREFIX = "[ADOT]"

# What upstream opentelemetry-instrumentation logs when its initialization fails.
UPSTREAM_LOGGER_NAME = "opentelemetry.instrumentation.auto_instrumentation"
UPSTREAM_INITIALIZATION_FAILURE_MESSAGE = "Failed to auto initialize OpenTelemetry"

STATUS_ACTIVATED = "activated"
STATUS_UNSUPPORTED_RUNTIME = "skipped-unsupported-runtime"
STATUS_INITIALIZATION_ERROR = "skipped-initialization-error"

_DISTRIBUTION_PREFIX = "aws_opentelemetry_distro-"
_DISTRIBUTION_SUFFIX = ".dist-info"


def _python_version():
    return "%d.%d.%d" % tuple(sys.version_info[:3])


def _minimum_version():
    return "%d.%d" % MINIMUM_PYTHON


def _bootstrap_directory():
    return os.path.dirname(os.path.abspath(__file__))


def _payload_root(bootstrap_directory):
    # <payload root>/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py
    return os.path.dirname(os.path.dirname(os.path.dirname(bootstrap_directory)))


def _same_path(left, right):
    return os.path.normcase(os.path.realpath(left)) == os.path.normcase(os.path.realpath(right))


def _remove_from_sys_path(directories):
    """Remove every ``sys.path`` entry that resolves to one of ``directories``.

    Only the current process changes. ``PYTHONPATH`` is left alone so that each
    child Python process runs this bootstrap and makes its own decision.
    """
    sys.path[:] = [entry for entry in sys.path if not any(_same_path(entry or ".", d) for d in directories)]


def _adot_version(payload_root):
    """Read the ADOT version from the payload's metadata without importing it."""
    try:
        for name in os.listdir(payload_root):
            if name.startswith(_DISTRIBUTION_PREFIX) and name.endswith(_DISTRIBUTION_SUFFIX):
                return name[len(_DISTRIBUTION_PREFIX) : -len(_DISTRIBUTION_SUFFIX)]
    except Exception:
        pass
    return None


def _log(message):
    try:
        sys.stderr.write("%s %s\n" % (LOG_PREFIX, message))
        sys.stderr.flush()
    except Exception:
        pass


def _write_status(payload_root, instrumented, status, reason):
    """Record this process's decision, unless another process already has.

    The first writer is usually the container's entrypoint, the application itself. Later processes, such as
    the application's children, ``kubectl exec`` sessions and exec probes, must not overwrite its record.
    Skip silently if the volume is not writable.
    """
    path = os.path.join(payload_root, STATUS_FILE_NAME)
    if os.path.exists(path):
        return
    command = None
    argv = getattr(sys, "argv", None)
    if argv:
        command = argv[0]
    document = {
        "instrumented": instrumented,
        "status": status,
        "reason": reason,
        "python_version": _python_version(),
        "adot_version": _adot_version(payload_root),
        "pid": os.getpid(),
        "command": command,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    temporary_path = "%s.%d.tmp" % (path, os.getpid())
    try:
        with open(temporary_path, "w", encoding="utf-8") as status_file:
            json.dump(document, status_file, sort_keys=True)
            status_file.write("\n")
        # Publishes the complete file atomically, and fails if another process created it first.
        os.link(temporary_path, path)
    except Exception:
        pass
    finally:
        try:
            os.remove(temporary_path)
        except Exception:
            pass


class _InitializationFailureHandler(logging.Handler):
    """Records the exception that upstream logs, and swallows, when initialization fails."""

    def __init__(self):
        logging.Handler.__init__(self, logging.ERROR)
        self.error = None

    def emit(self, record):
        if record.getMessage() == UPSTREAM_INITIALIZATION_FAILURE_MESSAGE and record.exc_info:
            self.error = record.exc_info[1]


def _initialize():
    # Imported here so that nothing from the payload loads on an unsupported Python.
    from opentelemetry.instrumentation.auto_instrumentation import initialize  # pylint: disable=import-outside-toplevel

    # Call upstream exactly as its own sitecustomize.py does. It logs and swallows initialization errors, and
    # treats some of them as non-fatal (for example a missing gevent). Watch its log to report a failure.
    handler = _InitializationFailureHandler()
    logger = logging.getLogger(UPSTREAM_LOGGER_NAME)
    logger.addHandler(handler)
    try:
        initialize()
    finally:
        logger.removeHandler(handler)
    if handler.error is not None:
        raise handler.error


def bootstrap(initializer=_initialize, bootstrap_directory=None):
    """Activate auto-instrumentation, or skip it without affecting the application.

    Returns the status that was reported.
    """
    if bootstrap_directory is None:
        bootstrap_directory = _bootstrap_directory()
    payload_root = _payload_root(bootstrap_directory)

    if tuple(sys.version_info[:2]) < MINIMUM_PYTHON:
        _remove_from_sys_path([bootstrap_directory, payload_root])
        reason = "Python %s is not supported (requires %s+)" % (_python_version(), _minimum_version())
        _log("auto-instrumentation skipped: %s; application continues without telemetry" % reason)
        _write_status(payload_root, False, STATUS_UNSUPPORTED_RUNTIME, reason)
        return STATUS_UNSUPPORTED_RUNTIME

    try:
        initializer()
    except Exception as error:
        reason = "initialization failed (%s: %s)" % (type(error).__name__, error)
        _log("auto-instrumentation skipped: %s; application continues without telemetry" % reason)
        _write_status(payload_root, False, STATUS_INITIALIZATION_ERROR, reason)
        return STATUS_INITIALIZATION_ERROR

    _write_status(payload_root, True, STATUS_ACTIVATED, "Python %s is supported" % _python_version())
    return STATUS_ACTIVATED


# Python imports this file as the top-level ``sitecustomize`` module at startup.
# Importing it from the distro package, as the unit tests do, has no side effects.
if __name__ == "sitecustomize":
    try:
        bootstrap()
    except Exception:
        pass
