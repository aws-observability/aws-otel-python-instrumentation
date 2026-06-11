# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extra branch-coverage tests for debugger.py.

These cover the remaining unit-testable branches that test_debugger_config.py and
test_debugger_fork.py do not reach: the optional opentelemetry.trace import
fallback, the manager-None / client-None early returns and the outer exception
handler in initialize_debugger, the missing-os.register_at_fork branch, and the
cleanup_debugger outer exception handler.

No real fork, threads, or network are used: os.register_at_fork, the client, and
the manager are all mocked. Module state mutated for the import-fallback test is
restored via addCleanup.
"""

import importlib
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from amazon.opentelemetry.distro.debugger import debugger as debugger_module
from amazon.opentelemetry.distro.debugger import instrumentation_manager


class TestTracerProviderImportFallback(unittest.TestCase):
    """Covers the opentelemetry.trace ImportError fallback at module import (lines 17-19)."""

    def test_import_failure_sets_tracer_provider_to_none(self):
        original_module = debugger_module
        self.addCleanup(
            lambda: importlib.reload(importlib.import_module("amazon.opentelemetry.distro.debugger.debugger"))
        )

        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "opentelemetry.trace":
                raise ImportError("no trace module")
            return real_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            reloaded = importlib.reload(original_module)

        # The except branch ran: both names fell back to None.
        self.assertIsNone(reloaded.TracerProvider)
        self.assertIsNone(reloaded.get_tracer_provider)


class TestInitializeDebuggerEarlyReturns(unittest.TestCase):
    """Covers manager-None (138-139), client-None (146-147), and outer except (165-167)."""

    def setUp(self):
        self._orig_client = debugger_module._global_debugger_client
        self._orig_pid = debugger_module._initialized_pid
        self._orig_fork = debugger_module._fork_handler_registered
        self._orig_manager = instrumentation_manager._global_manager_instance

    def tearDown(self):
        debugger_module._global_debugger_client = self._orig_client
        debugger_module._initialized_pid = self._orig_pid
        debugger_module._fork_handler_registered = self._orig_fork
        instrumentation_manager._global_manager_instance = self._orig_manager

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager", return_value=None)
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_manager_none_returns_false(self, _mock_enabled, _mock_init_mgr):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
            result = debugger_module.initialize_debugger()
        self.assertFalse(result)

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.start_debugger_client", return_value=None)
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_client_none_returns_false(self, _mock_enabled, mock_init_mgr, _mock_start):
        mock_init_mgr.return_value = mock.MagicMock()
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
            result = debugger_module.initialize_debugger()
        self.assertFalse(result)

    @mock.patch(
        "amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager", side_effect=RuntimeError("boom")
    )
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_unexpected_exception_returns_false(self, _mock_enabled, _mock_init_mgr):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
            result = debugger_module.initialize_debugger()
        self.assertFalse(result)


class TestRegisterForkHandlerNoAttr(unittest.TestCase):
    """Covers the missing os.register_at_fork branch (lines 229-230)."""

    def setUp(self):
        self._orig_registered = debugger_module._fork_handler_registered

    def tearDown(self):
        debugger_module._fork_handler_registered = self._orig_registered

    def test_no_register_at_fork_attribute_is_noop(self):
        debugger_module._fork_handler_registered = False
        # Simulate a platform whose os module lacks register_at_fork (Python <3.7 / some OSes).
        fake_os = SimpleNamespace()  # deliberately has no register_at_fork attribute
        with mock.patch.object(debugger_module, "os", fake_os):
            debugger_module._register_fork_handler()
        # The early return means the flag was NOT set.
        self.assertFalse(debugger_module._fork_handler_registered)


class TestCleanupDebuggerOuterException(unittest.TestCase):
    """Covers the cleanup_debugger outer except branch (lines 352-353)."""

    def setUp(self):
        self._orig_client = debugger_module._global_debugger_client
        debugger_module._global_debugger_client = None

    def tearDown(self):
        debugger_module._global_debugger_client = self._orig_client

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.stop_debugger_client", side_effect=RuntimeError("boom"))
    def test_outer_exception_is_swallowed(self, _mock_stop):
        # stop_debugger_client raising drives the outermost try/except; must not propagate.
        debugger_module.cleanup_debugger()


if __name__ == "__main__":
    unittest.main()
