# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for fork handling in debugger.py.

Validates that _reset_debugger_state clears all global singletons,
and _register_fork_handler registers the os.register_at_fork callback.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.debugger import _function_wrapper as fw_module
from amazon.opentelemetry.distro.debugger import debugger, instrumentation_manager
from amazon.opentelemetry.distro.debugger.debugger import (
    _register_fork_handler,
    _reset_debugger_state,
    initialize_debugger,
)


class TestResetDebuggerState(unittest.TestCase):
    """Tests for _reset_debugger_state."""

    def setUp(self):
        self._orig_client = debugger._global_debugger_client
        self._orig_pid = debugger._initialized_pid
        self._orig_manager = instrumentation_manager._global_manager_instance
        self._orig_writer = fw_module._snapshot_emitter

    def tearDown(self):
        debugger._global_debugger_client = self._orig_client
        debugger._initialized_pid = self._orig_pid
        instrumentation_manager._global_manager_instance = self._orig_manager
        fw_module._snapshot_emitter = self._orig_writer

    def test_reset_clears_global_client(self):
        """_reset_debugger_state sets _global_debugger_client to None."""
        debugger._global_debugger_client = MagicMock()
        _reset_debugger_state()
        self.assertIsNone(debugger._global_debugger_client)

    def test_reset_clears_global_manager(self):
        """_reset_debugger_state sets _global_manager_instance to None."""
        instrumentation_manager._global_manager_instance = MagicMock()
        _reset_debugger_state()
        self.assertIsNone(instrumentation_manager._global_manager_instance)

    def test_reset_clears_snapshot_emitter(self):
        """_reset_debugger_state sets _snapshot_emitter to None."""
        fw_module._snapshot_emitter = MagicMock()
        _reset_debugger_state()
        self.assertIsNone(fw_module._snapshot_emitter)

    def test_reset_clears_initialized_pid(self):
        """_reset_debugger_state sets _initialized_pid to None."""
        debugger._initialized_pid = 12345
        _reset_debugger_state()
        self.assertIsNone(debugger._initialized_pid)


class TestRegisterForkHandler(unittest.TestCase):
    """Tests for _register_fork_handler."""

    def setUp(self):
        self._orig_registered = debugger._fork_handler_registered

    def tearDown(self):
        debugger._fork_handler_registered = self._orig_registered

    @patch("os.register_at_fork")
    def test_registers_after_in_child_callback(self, mock_register):
        """_register_fork_handler calls os.register_at_fork with after_in_child."""
        debugger._fork_handler_registered = False
        _register_fork_handler()

        mock_register.assert_called_once()
        _, kwargs = mock_register.call_args
        self.assertIn("after_in_child", kwargs)
        self.assertTrue(callable(kwargs["after_in_child"]))

    @patch("os.register_at_fork")
    def test_idempotent_registration(self, mock_register):
        """_register_fork_handler only registers once."""
        debugger._fork_handler_registered = False
        _register_fork_handler()
        _register_fork_handler()
        _register_fork_handler()
        mock_register.assert_called_once()

    @patch("os.register_at_fork")
    def test_sets_flag_after_registration(self, mock_register):
        """_register_fork_handler sets _fork_handler_registered to True."""
        debugger._fork_handler_registered = False
        _register_fork_handler()
        self.assertTrue(debugger._fork_handler_registered)


class TestInitializeDebuggerForkIntegration(unittest.TestCase):
    """Tests for initialize_debugger's fork-related behavior."""

    def setUp(self):
        self._orig_client = debugger._global_debugger_client
        self._orig_pid = debugger._initialized_pid
        self._orig_fork_registered = debugger._fork_handler_registered
        self._orig_manager = instrumentation_manager._global_manager_instance

    def tearDown(self):
        debugger._global_debugger_client = self._orig_client
        debugger._initialized_pid = self._orig_pid
        debugger._fork_handler_registered = self._orig_fork_registered
        instrumentation_manager._global_manager_instance = self._orig_manager

    @patch("amazon.opentelemetry.distro.debugger.debugger._register_fork_handler")
    @patch("amazon.opentelemetry.distro.debugger.debugger.StatusReporter")
    @patch("amazon.opentelemetry.distro.debugger.debugger.start_debugger_client")
    @patch("amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager")
    @patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_initialize_registers_fork_handler(
        self, mock_enabled, mock_init_mgr, mock_start_client, mock_reporter, mock_register_fork
    ):
        """initialize_debugger calls _register_fork_handler on success."""
        mock_init_mgr.return_value = MagicMock()
        mock_start_client.return_value = MagicMock()

        result = initialize_debugger()

        self.assertTrue(result)
        mock_register_fork.assert_called_once()

    @patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=False)
    def test_initialize_disabled_does_not_register_fork(self, mock_enabled):
        """When debugger is disabled, fork handler is not registered."""
        result = initialize_debugger()
        self.assertFalse(result)

    @patch("amazon.opentelemetry.distro.debugger.debugger._register_fork_handler")
    @patch("amazon.opentelemetry.distro.debugger.debugger.StatusReporter")
    @patch("amazon.opentelemetry.distro.debugger.debugger.start_debugger_client")
    @patch("amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager")
    @patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_initialize_sets_pid(
        self, mock_enabled, mock_init_mgr, mock_start_client, mock_reporter, mock_register_fork
    ):
        """initialize_debugger records the current PID."""
        mock_init_mgr.return_value = MagicMock()
        mock_start_client.return_value = MagicMock()
        debugger._initialized_pid = None

        initialize_debugger()

        self.assertEqual(debugger._initialized_pid, os.getpid())


class TestLambdaAutoDetection(unittest.TestCase):
    """Tests for Lambda environment auto-detection."""

    @patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    @patch.dict(os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "my-function"})
    def test_initialize_skipped_on_lambda(self, mock_enabled):
        """initialize_debugger returns False when running on Lambda."""
        result = initialize_debugger()
        self.assertFalse(result)

    @patch("amazon.opentelemetry.distro.debugger.debugger._register_fork_handler")
    @patch("amazon.opentelemetry.distro.debugger.debugger.StatusReporter")
    @patch("amazon.opentelemetry.distro.debugger.debugger.start_debugger_client")
    @patch("amazon.opentelemetry.distro.debugger.debugger.initialize_global_manager")
    @patch("amazon.opentelemetry.distro.debugger.debugger.is_debugger_enabled", return_value=True)
    def test_initialize_proceeds_without_lambda_env(
        self, mock_enabled, mock_init_mgr, mock_start_client, mock_reporter, mock_register_fork
    ):
        """initialize_debugger proceeds when AWS_LAMBDA_FUNCTION_NAME is not set."""
        mock_init_mgr.return_value = MagicMock()
        mock_start_client.return_value = MagicMock()
        # Ensure AWS_LAMBDA_FUNCTION_NAME is not set
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
            result = initialize_debugger()
        self.assertTrue(result)


class TestAfterForkCallback(unittest.TestCase):
    """Tests for the actual after_in_child callback behavior."""

    def setUp(self):
        self._orig_client = debugger._global_debugger_client
        self._orig_pid = debugger._initialized_pid
        self._orig_fork_registered = debugger._fork_handler_registered
        self._orig_manager = instrumentation_manager._global_manager_instance
        self._orig_writer = fw_module._snapshot_emitter

    def tearDown(self):
        debugger._global_debugger_client = self._orig_client
        debugger._initialized_pid = self._orig_pid
        debugger._fork_handler_registered = self._orig_fork_registered
        instrumentation_manager._global_manager_instance = self._orig_manager
        fw_module._snapshot_emitter = self._orig_writer

    @patch("amazon.opentelemetry.distro.debugger.debugger.initialize_debugger")
    def test_after_fork_callback_resets_and_reinitializes(self, mock_init):
        """The after_in_child callback resets state and calls initialize_debugger."""
        debugger._fork_handler_registered = False
        captured_callback = None

        def capture_register(after_in_child=None):
            nonlocal captured_callback
            captured_callback = after_in_child

        with patch("os.register_at_fork", side_effect=capture_register):
            _register_fork_handler()

        self.assertIsNotNone(captured_callback)

        # Set state as if we're in the master
        debugger._global_debugger_client = MagicMock()
        debugger._initialized_pid = 999
        instrumentation_manager._global_manager_instance = MagicMock()
        fw_module._snapshot_emitter = MagicMock()

        # Call the callback (simulating post-fork in child)
        captured_callback()

        # Verify reset happened
        self.assertIsNone(debugger._global_debugger_client)
        self.assertIsNone(instrumentation_manager._global_manager_instance)
        self.assertIsNone(fw_module._snapshot_emitter)
        # Verify re-initialization was attempted
        mock_init.assert_called_once()

    @patch("amazon.opentelemetry.distro.debugger.debugger.initialize_debugger", side_effect=RuntimeError("boom"))
    def test_after_fork_callback_handles_errors(self, mock_init):
        """The after_in_child callback doesn't propagate exceptions."""
        debugger._fork_handler_registered = False
        captured_callback = None

        def capture_register(after_in_child=None):
            nonlocal captured_callback
            captured_callback = after_in_child

        with patch("os.register_at_fork", side_effect=capture_register):
            _register_fork_handler()

        # Should not raise even though initialize_debugger raises
        captured_callback()


if __name__ == "__main__":
    unittest.main()
