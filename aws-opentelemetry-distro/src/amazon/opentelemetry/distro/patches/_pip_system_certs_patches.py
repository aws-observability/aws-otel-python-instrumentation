# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from importlib.metadata import PackageNotFoundError, version
from logging import Logger, getLogger

_logger: Logger = getLogger(__name__)

# Module-level guard so the patch is applied at most once per process.
_patch_applied = False


def _is_pip_system_certs_installed() -> bool:
    """Is the pip_system_certs package installed?"""
    try:
        dist_version = version("pip_system_certs")
        _logger.debug("pip_system_certs is installed: %s", dist_version)
        return True
    except PackageNotFoundError as exc:
        _logger.debug("pip_system_certs is not installed. %s", exc)
        return False


def apply_pip_system_certs_compatibility_patch() -> None:
    """Re-bind stale ``ssl.SSLContext`` references in botocore/urllib3.

    When ``pip_system_certs`` is installed, it injects ``truststore.SSLContext`` as the
    process-wide ``ssl.SSLContext`` via a ``.pth`` file. The injection runs in the
    ``finally`` block of a ``site.execsitecustomize`` wrapper, i.e. *after*
    ``sitecustomize.py`` returns.

    OpenTelemetry's auto-instrumentation entry point (``opentelemetry-instrument``)
    runs from ``sitecustomize.py``, which loads the ADOT distro and transitively imports
    ``requests`` (via the upstream OTLP HTTP exporters) and ``botocore``. Both of those
    modules capture a reference to ``ssl.SSLContext`` at import time. Because the import
    happens before ``pip_system_certs``'s injection runs, the captured reference is the
    original C-level ``ssl.SSLContext``, not the truststore-wrapped class.

    On Python 3.12, ``ssl.SSLContext.options.__set__`` is implemented as
    ``super(SSLContext, SSLContext).options.__set__(self, value)`` where ``SSLContext``
    is resolved from ``ssl``'s module globals at call time. After ``pip_system_certs``
    runs, that name resolves to ``truststore.SSLContext``, and the ``super()`` chain
    bounces between the original and truststore classes until the recursion limit
    (~978 frames) is exceeded.

    This patch re-binds ``botocore.httpsession.SSLContext`` and
    ``urllib3.util.ssl_.SSLContext`` to the *current* ``ssl.SSLContext``
    (i.e., truststore's wrapper). truststore's own ``SSLContext.options`` setter does
    not use the recursive ``super()`` pattern, so subsequent SSL context creations
    succeed.

    The patch is idempotent: a module-level guard ensures it only runs once per
    process. It is a no-op when ``pip_system_certs`` is not installed or when the
    references already match ``ssl.SSLContext``.
    """
    global _patch_applied  # pylint: disable=global-statement
    if _patch_applied:
        return

    # Only apply the patch when pip_system_certs is installed in user application space.
    if not _is_pip_system_certs_installed():
        _patch_applied = True
        return

    # pylint: disable=import-outside-toplevel
    import ssl

    try:
        # pylint: disable=import-outside-toplevel
        import botocore.httpsession

        if botocore.httpsession.SSLContext is not ssl.SSLContext:
            _logger.debug(
                "Rebinding botocore.httpsession.SSLContext to current ssl.SSLContext (pip_system_certs detected)."
            )
            botocore.httpsession.SSLContext = ssl.SSLContext
    except ImportError:
        # botocore not installed; nothing to rebind on the botocore side.
        pass
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to rebind botocore.httpsession.SSLContext: %s", exc)

    try:
        # pylint: disable=import-outside-toplevel
        import urllib3.util.ssl_

        if urllib3.util.ssl_.SSLContext is not ssl.SSLContext:
            _logger.debug(
                "Rebinding urllib3.util.ssl_.SSLContext to current ssl.SSLContext (pip_system_certs detected)."
            )
            urllib3.util.ssl_.SSLContext = ssl.SSLContext
    except ImportError:
        # urllib3 not installed; nothing to rebind.
        pass
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to rebind urllib3.util.ssl_.SSLContext: %s", exc)

    _patch_applied = True
