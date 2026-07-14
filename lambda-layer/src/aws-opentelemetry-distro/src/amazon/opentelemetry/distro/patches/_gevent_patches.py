# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from importlib.metadata import PackageNotFoundError, version
from logging import Logger, getLogger

from packaging.requirements import Requirement

_logger: Logger = getLogger(__name__)

# Env variable to control Gevent monkey patching behavior in ADOT.
# Read more about the Gevent monkey patching: https://www.gevent.org/intro.html#monkey-patching
# Possible values are 'all', 'none', and
# comma separated list 'os, thread, time, sys, socket, select, ssl, subprocess, builtins, signal, queue, contextvars'.
# When set to 'none', gevent's monkey patching is skipped.
# When set to 'all' (default behavior), gevent patch is executed for all modules as per
# https://www.gevent.org/api/gevent.monkey.html#gevent.monkey.patch_all.
# When set to a comma separated list of modules, only those are processed for gevent's patch.
AWS_GEVENT_PATCH_MODULES = "AWS_GEVENT_PATCH_MODULES"


def _is_gevent_installed() -> bool:
    """Is the gevent package installed?"""
    req = Requirement("gevent")
    try:
        dist_version = version(req.name)
        _logger.debug("Gevent is installed: %s", dist_version)
    except PackageNotFoundError as exc:
        _logger.debug("Gevent is not installed. %s", exc)
        return False
    return True


def apply_gevent_monkey_patch():
    # This patch differs from other instrumentation patches in this directory as it addresses
    # application compatibility rather than telemetry functionality. It prevents breaking user
    # applications that run on Gevent and use libraries like boto3, requests, or urllib3 when
    # instrumented with ADOT.
    #
    # Without this patch, users encounter "RecursionError: maximum recursion depth exceeded"
    # because by the time Gevent monkey-patches modules (such as ssl), those modules have already
    # been imported by ADOT. Specifically, aws_xray_remote_sampler imports requests, which
    # transitively imports ssl, leaving these modules in an inconsistent state for Gevent.
    #
    # Gevent recommends monkey-patching as early as possible:
    # https://www.gevent.org/intro.html#monkey-patching
    #
    # Since ADOT initialization occurs before user application code, we perform the monkey-patch
    # here to ensure proper module state for Gevent-based applications.

    # Only apply the gevent monkey patch if gevent is installed is user application space.
    if _is_gevent_installed():
        try:
            gevent_patch_module = os.environ.get(AWS_GEVENT_PATCH_MODULES, "all")

            if gevent_patch_module != "none":
                # pylint: disable=import-outside-toplevel
                # Delay import to only occur if monkey patch is needed (e.g. gevent is used to run application).
                from gevent import monkey

                if gevent_patch_module == "all":
                    monkey.patch_all()
                else:
                    module_list = [module.strip() for module in gevent_patch_module.split(",")]

                    monkey.patch_all(
                        socket="socket" in module_list,
                        time="time" in module_list,
                        select="select" in module_list,
                        thread="thread" in module_list,
                        os="os" in module_list,
                        ssl="ssl" in module_list,
                        subprocess="subprocess" in module_list,
                        sys="sys" in module_list,
                        builtins="builtins" in module_list,
                        signal="signal" in module_list,
                        queue="queue" in module_list,
                        contextvars="contextvars" in module_list,
                    )
        except Exception as exc:  # pylint: disable=broad-except
            _logger.error("Failed to monkey patch gevent, exception: %s", exc)
