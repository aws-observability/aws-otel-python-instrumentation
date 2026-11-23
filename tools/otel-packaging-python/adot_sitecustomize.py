# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Entry point of ADOT Python auto-instrumentation injected by the CloudWatch agent operator.
#
# build.sh installs this file as <bundle>/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py, in place
# of upstream's, which only calls initialize(). The operator puts that directory first on PYTHONPATH and the bundle
# root last, so every Python process in the container imports this file at startup, whatever its Python version. It
# runs the OpenTelemetry Packaging bootstrap (<bundle>/_otel_packaging_sitecustomize.py, see README.md), which checks
# whether auto-instrumentation can start in this process and, if it cannot, removes the bundle root from sys.path and
# PYTHONPATH. This file then removes its own directory too, which only the operator adds, so that the application and
# its child processes run as if auto-instrumentation were not injected.
#
# IMPORTANT: this file must stay valid Python 2.7 so that older interpreters can run it, use only the standard library,
# and never raise.

# pylint: disable=consider-using-f-string  # f-strings are a SyntaxError on Python 2.7

import os
import sys

BUNDLE_BOOTSTRAP_NAME = "_otel_packaging_sitecustomize.py"
INJECTOR_PREFIX_VARIABLE = "PYTHON_AUTO_INSTRUMENTATION_AGENT_PATH_PREFIX"


def _same_entry(entry, directory):
    # Compared the way the bootstrap compares entries, so that both agree on whether the bundle is on sys.path.
    return os.path.normpath(entry) == os.path.normpath(directory)


def _inside(entry, directory):
    entry = os.path.normpath(entry)
    directory = os.path.normpath(directory)
    return entry == directory or entry.startswith(directory + os.sep)


def _remove_bundle_entries(bundle_root):
    # Removes the bundle root and every directory inside it: this file's directory, which only the operator adds, and
    # the interpreter-specific cp3<minor> directory, which the bootstrap inserts but does not remove on deactivation.
    sys.path[:] = [entry for entry in sys.path if not _inside(entry, bundle_root)]
    pythonpath = os.environ.get("PYTHONPATH")
    if pythonpath is not None:
        entries = [entry for entry in pythonpath.split(os.pathsep) if not _inside(entry, bundle_root)]
        os.environ["PYTHONPATH"] = os.pathsep.join(entries)


def _deactivate(bundle_root, reason):
    _remove_bundle_entries(bundle_root)
    os.environ[INJECTOR_PREFIX_VARIABLE] = ""
    try:
        sys.stderr.write(
            "[opentelemetry-python-autoinstrumentation] WARNING: cannot auto-instrument Python process: %s\n" % reason
        )
        sys.stderr.flush()
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def run():
    own_directory = os.path.dirname(os.path.abspath(__file__))
    # <bundle>/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py
    bundle_root = os.path.dirname(os.path.dirname(os.path.dirname(own_directory)))
    bootstrap = os.path.join(bundle_root, BUNDLE_BOOTSTRAP_NAME)
    try:
        # Read as bytes and compiled, rather than imported, so that the bootstrap runs from this exact path under its
        # own name, and so that its source encoding is decoded the same way on every interpreter.
        with open(bootstrap, "rb") as bootstrap_file:
            code = compile(bootstrap_file.read(), bootstrap, "exec")
        namespace = {"__name__": "_otel_packaging_sitecustomize", "__file__": bootstrap}
        exec(code, namespace)  # pylint: disable=exec-used
    except Exception as error:  # pylint: disable=broad-exception-caught
        _deactivate(bundle_root, "cannot run %s: %s: %s" % (bootstrap, type(error).__name__, error))
        return
    if not any(_same_entry(entry, bundle_root) for entry in sys.path):
        # The bootstrap deactivated auto-instrumentation and already logged why.
        _remove_bundle_entries(bundle_root)


try:
    run()
except Exception:  # pylint: disable=broad-exception-caught
    pass
