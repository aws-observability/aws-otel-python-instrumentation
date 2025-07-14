# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import os
from logging import Logger, getLogger

from amazon.opentelemetry.distro._utils import is_installed
from amazon.opentelemetry.distro.patches._resource_detector_patches import _apply_resource_detector_patches

# Env variable for determining whether we want to monkey patch gevent modules. Possible values are 'all', 'none', and
# comma separated list 'os, thread, time, sys, socket, select, ssl, subprocess, builtins, signal, queue, contextvars'
AWS_GEVENT_PATCH_MODULES = "AWS_GEVENT_PATCH_MODULES"

_logger: Logger = getLogger(__name__)


def apply_instrumentation_patches() -> None:
    """Apply patches to upstream instrumentation libraries.

    This method is invoked to apply changes to upstream instrumentation libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """
    if is_installed("gevent"):
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
            _logger.info("Failed to monkey patch gevent, exception: %s", exc)

    if is_installed("botocore ~= 1.0"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._botocore_patches import _apply_botocore_instrumentation_patches

        _apply_botocore_instrumentation_patches()

    if is_installed("starlette"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches

        # Starlette auto-instrumentation v0.54b includes a strict dependency version check
        # This restriction was removed in v1.34.0/0.55b0. Applying temporary patch for Genesis launch
        # TODO: Remove patch after syncing with upstream v1.34.0 or later
        _apply_starlette_instrumentation_patches()

    # No need to check if library is installed as this patches opentelemetry.sdk,
    # which must be installed for the distro to work at all.
    _apply_resource_detector_patches()
