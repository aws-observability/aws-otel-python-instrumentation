# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from importlib.metadata import PackageNotFoundError
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.patches import _pip_system_certs_patches
from amazon.opentelemetry.distro.patches._pip_system_certs_patches import apply_pip_system_certs_compatibility_patch


class TestPipSystemCertsPatches(TestCase):
    def setUp(self) -> None:
        # Reset the module-level guard before every test so each test exercises the
        # full code path.
        _pip_system_certs_patches._patch_attempted = False

    def tearDown(self) -> None:
        # Leave the guard in a clean state for tests that follow.
        _pip_system_certs_patches._patch_attempted = False

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_no_op_when_pip_system_certs_not_installed(self, mock_version):
        """When pip_system_certs is not installed, the patch is a no-op and does not
        touch botocore/urllib3 module globals."""
        mock_version.side_effect = PackageNotFoundError("pip_system_certs")

        # pylint: disable=import-outside-toplevel
        import botocore.httpsession
        import urllib3.util.ssl_

        sentinel_class = type("SentinelSSLContext", (), {})

        with patch.object(botocore.httpsession, "SSLContext", sentinel_class):
            with patch.object(urllib3.util.ssl_, "SSLContext", sentinel_class):
                apply_pip_system_certs_compatibility_patch()

                # References must remain untouched when pip_system_certs is not present.
                self.assertIs(botocore.httpsession.SSLContext, sentinel_class)
                self.assertIs(urllib3.util.ssl_.SSLContext, sentinel_class)

        self.assertTrue(_pip_system_certs_patches._patch_attempted)

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_rebinds_stale_references_when_installed(self, mock_version):
        """When pip_system_certs is installed and botocore/urllib3 hold stale
        ``ssl.SSLContext`` references, the patch rebinds them to the current
        ``ssl.SSLContext``."""
        mock_version.return_value = "5.3"

        # pylint: disable=import-outside-toplevel
        import ssl

        import botocore.httpsession
        import urllib3.util.ssl_

        # Simulate the post-injection state: ssl.SSLContext has been replaced with
        # truststore's wrapper, but botocore/urllib3 still hold the original.
        original_ssl_context = ssl.SSLContext
        truststore_like = type("TruststoreSSLContext", (), {})

        with patch.object(botocore.httpsession, "SSLContext", original_ssl_context):
            with patch.object(urllib3.util.ssl_, "SSLContext", original_ssl_context):
                with patch.object(ssl, "SSLContext", truststore_like):
                    apply_pip_system_certs_compatibility_patch()

                    self.assertIs(botocore.httpsession.SSLContext, truststore_like)
                    self.assertIs(urllib3.util.ssl_.SSLContext, truststore_like)

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_no_op_when_references_already_match(self, mock_version):
        """When references already match the current ``ssl.SSLContext``, the patch
        leaves them untouched (idempotent)."""
        mock_version.return_value = "5.3"

        # pylint: disable=import-outside-toplevel
        import ssl

        import botocore.httpsession
        import urllib3.util.ssl_

        current = ssl.SSLContext

        with patch.object(botocore.httpsession, "SSLContext", current):
            with patch.object(urllib3.util.ssl_, "SSLContext", current):
                apply_pip_system_certs_compatibility_patch()

                self.assertIs(botocore.httpsession.SSLContext, current)
                self.assertIs(urllib3.util.ssl_.SSLContext, current)

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_runs_only_once(self, mock_version):
        """The patch is guarded so the package detection only runs on the first call."""
        mock_version.side_effect = PackageNotFoundError("pip_system_certs")

        apply_pip_system_certs_compatibility_patch()
        apply_pip_system_certs_compatibility_patch()
        apply_pip_system_certs_compatibility_patch()

        self.assertEqual(mock_version.call_count, 1)

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_botocore_import_failure_does_not_crash(self, mock_version):
        """If botocore.httpsession is absent the patch silently skips it and
        still processes urllib3."""
        mock_version.return_value = "5.3"

        # pylint: disable=import-outside-toplevel
        import sys

        import urllib3.util.ssl_

        saved = sys.modules.get("botocore.httpsession")
        # Setting to None forces Python to raise ImportError on `import botocore.httpsession`.
        sys.modules["botocore.httpsession"] = None
        try:
            apply_pip_system_certs_compatibility_patch()
        finally:
            if saved is None:
                sys.modules.pop("botocore.httpsession", None)
            else:
                sys.modules["botocore.httpsession"] = saved

        # Patch should still mark itself as attempted even when one library is missing.
        self.assertTrue(_pip_system_certs_patches._patch_attempted)
        # urllib3 path should still have been considered.
        self.assertTrue(hasattr(urllib3.util.ssl_, "SSLContext"))

    @patch("amazon.opentelemetry.distro.patches._pip_system_certs_patches.version")
    def test_urllib3_import_failure_does_not_crash(self, mock_version):
        """If urllib3.util.ssl_ is absent the patch silently skips it."""
        mock_version.return_value = "5.3"

        # pylint: disable=import-outside-toplevel
        import sys

        saved = sys.modules.get("urllib3.util.ssl_")
        sys.modules["urllib3.util.ssl_"] = None
        try:
            apply_pip_system_certs_compatibility_patch()
        finally:
            if saved is None:
                sys.modules.pop("urllib3.util.ssl_", None)
            else:
                sys.modules["urllib3.util.ssl_"] = saved

        self.assertTrue(_pip_system_certs_patches._patch_attempted)
