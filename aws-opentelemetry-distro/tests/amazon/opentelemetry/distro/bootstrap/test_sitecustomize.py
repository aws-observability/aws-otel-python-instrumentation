# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.bootstrap import sitecustomize
from amazon.opentelemetry.distro.bootstrap.sitecustomize import (
    LOG_PREFIX,
    MINIMUM_PYTHON,
    STATUS_ACTIVATED,
    STATUS_FILE_NAME,
    STATUS_INITIALIZATION_ERROR,
    STATUS_UNSUPPORTED_RUNTIME,
    UPSTREAM_LOGGER_NAME,
    bootstrap,
)

_UNSUPPORTED_VERSION_INFO = (3, 9, 15, "final", 0)
_ADOT_VERSION = "0.22.0"


class TestSitecustomize(TestCase):
    """Uses the operator's layout: the payload root contains
    ``opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py``, and ``PYTHONPATH`` is
    ``<auto_instrumentation dir>:<customer entry>:<payload root>``."""

    def setUp(self):
        self.payload_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.payload_root, True)
        self.bootstrap_directory = os.path.join(
            self.payload_root, "opentelemetry", "instrumentation", "auto_instrumentation"
        )
        os.makedirs(self.bootstrap_directory)
        os.makedirs(os.path.join(self.payload_root, f"aws_opentelemetry_distro-{_ADOT_VERSION}.dist-info"))
        self.customer_entry = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.customer_entry, True)

        original_sys_path = list(sys.path)
        self.addCleanup(sys.path.__setitem__, slice(None), original_sys_path)
        sys.path[:] = [self.bootstrap_directory, self.customer_entry, self.payload_root] + original_sys_path
        self.expected_supported_sys_path = list(sys.path)

        self.stderr = io.StringIO()
        stderr_patcher = patch.object(sys, "stderr", self.stderr)
        stderr_patcher.start()
        self.addCleanup(stderr_patcher.stop)

        self.initializer = MagicMock()

    def _bootstrap(self, version_info=None):
        if version_info is None:
            return bootstrap(initializer=self.initializer, bootstrap_directory=self.bootstrap_directory)
        with patch.object(sys, "version_info", version_info):
            return bootstrap(initializer=self.initializer, bootstrap_directory=self.bootstrap_directory)

    def _read_status(self):
        with open(os.path.join(self.payload_root, STATUS_FILE_NAME), encoding="utf-8") as status_file:
            return json.load(status_file)

    def _leftover_temporary_files(self):
        return [name for name in os.listdir(self.payload_root) if name.endswith(".tmp")]

    def test_minimum_python_matches_requires_python(self):
        pyproject = Path(__file__).resolve().parents[5] / "pyproject.toml"
        match = re.search(r'^requires-python\s*=\s*">=(\d+)\.(\d+)"', pyproject.read_text(), re.MULTILINE)
        self.assertIsNotNone(match)
        self.assertEqual(MINIMUM_PYTHON, (int(match.group(1)), int(match.group(2))))

    def test_unsupported_python_removes_payload_from_sys_path_and_skips_initialization(self):
        # The payload resolved under a supported Python shadows the app's own packages (for example
        # urllib3 2.x on Python 3.9), so both ADOT entries must leave sys.path before the app imports.
        pythonpath = os.pathsep.join([self.bootstrap_directory, self.customer_entry, self.payload_root])
        with patch.dict(os.environ, {"PYTHONPATH": pythonpath}):
            status = self._bootstrap(_UNSUPPORTED_VERSION_INFO)

            # Each child Python process makes its own decision, so PYTHONPATH is left alone.
            self.assertEqual(os.environ["PYTHONPATH"], pythonpath)

        self.assertEqual(status, STATUS_UNSUPPORTED_RUNTIME)
        self.initializer.assert_not_called()
        self.assertNotIn(self.bootstrap_directory, sys.path)
        self.assertNotIn(self.payload_root, sys.path)
        self.assertEqual(sys.path[0], self.customer_entry)

    def test_unsupported_python_removes_equivalent_spellings_of_the_payload_paths(self):
        sys.path[:] = [self.bootstrap_directory + os.sep, self.customer_entry, self.payload_root + os.sep + "."]

        self._bootstrap(_UNSUPPORTED_VERSION_INFO)

        self.assertEqual(sys.path, [self.customer_entry])

    def test_unsupported_python_logs_reason_and_writes_status(self):
        self._bootstrap(_UNSUPPORTED_VERSION_INFO)

        self.assertEqual(
            self.stderr.getvalue(),
            f"{LOG_PREFIX} auto-instrumentation skipped: Python 3.9.15 is not supported (requires 3.10+); "
            "application continues without telemetry\n",
        )
        status = self._read_status()
        self.assertEqual(status["instrumented"], False)
        self.assertEqual(status["status"], STATUS_UNSUPPORTED_RUNTIME)
        self.assertEqual(status["reason"], "Python 3.9.15 is not supported (requires 3.10+)")
        self.assertEqual(status["python_version"], "3.9.15")
        self.assertEqual(status["adot_version"], _ADOT_VERSION)
        self.assertEqual(status["pid"], os.getpid())
        self.assertEqual(status["command"], sys.argv[0])
        self.assertRegex(status["timestamp"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_supported_python_initializes_once_and_keeps_sys_path(self):
        status = self._bootstrap()

        self.assertEqual(status, STATUS_ACTIVATED)
        self.initializer.assert_called_once_with()
        self.assertEqual(sys.path, self.expected_supported_sys_path)
        # Activation is recorded in the status file only, to avoid a log line in every instrumented process.
        self.assertEqual(self.stderr.getvalue(), "")
        status_document = self._read_status()
        self.assertEqual(status_document["instrumented"], True)
        self.assertEqual(status_document["status"], STATUS_ACTIVATED)

    def test_initialization_error_is_reported_and_not_raised(self):
        self.initializer.side_effect = RuntimeError("Requested component 'bogus' not found")

        status = self._bootstrap()

        self.assertEqual(status, STATUS_INITIALIZATION_ERROR)
        self.assertEqual(
            self.stderr.getvalue(),
            f"{LOG_PREFIX} auto-instrumentation skipped: initialization failed (RuntimeError: Requested component "
            "'bogus' not found); application continues without telemetry\n",
        )
        status_document = self._read_status()
        self.assertEqual(status_document["instrumented"], False)
        self.assertEqual(status_document["status"], STATUS_INITIALIZATION_ERROR)

    def test_upstream_initialization_failure_is_detected_from_its_log(self):
        # Upstream initialize() logs and swallows the failure, so the bootstrap reads it from upstream's log.
        # Also guards the logger name and message the bootstrap depends on.
        with self._patched_upstream_initialization() as loaders:
            loaders["_load_distro"].side_effect = RuntimeError("distro failed")

            with self.assertRaisesRegex(RuntimeError, "^distro failed$"):
                sitecustomize._initialize()

    def test_non_fatal_upstream_error_keeps_initializing(self):
        # Upstream logs a missing gevent but still instruments the application. That must stay activated.
        with self._patched_upstream_initialization() as loaders, patch.dict(
            os.environ, {"OTEL_PYTHON_AUTO_INSTRUMENTATION_EXPERIMENTAL_GEVENT_PATCH": "patch_all"}
        ), patch.dict(sys.modules, {"gevent": None}), self.assertLogs(UPSTREAM_LOGGER_NAME, logging.ERROR) as logs:
            sitecustomize._initialize()

        loaders["_load_instrumentors"].assert_called_once()
        self.assertIn("Failed to monkey patch with gevent", logs.output[0])

    def test_initialization_failure_handler_is_removed_after_initialization(self):
        logger = logging.getLogger(UPSTREAM_LOGGER_NAME)
        handlers_before = list(logger.handlers)

        with self._patched_upstream_initialization() as loaders:
            loaders["_load_distro"].side_effect = RuntimeError("distro failed")
            with self.assertRaises(RuntimeError):
                sitecustomize._initialize()

        self.assertEqual(logger.handlers, handlers_before)

    @staticmethod
    @contextmanager
    def _patched_upstream_initialization():
        """Replaces what upstream initialize() loads, so that no real SDK is configured in the test process."""
        names = ("_load_distro", "_load_configurators", "_load_instrumentors")
        with patch.dict(os.environ, {}), ExitStack() as stack:
            os.environ.pop("PYTHONPATH", None)
            yield {
                name: stack.enter_context(patch("opentelemetry.instrumentation.auto_instrumentation." + name))
                for name in names
            }

    def test_first_writer_keeps_the_status_file(self):
        # The first process is usually the application. A later process, for example a child running another
        # Python version or a `kubectl exec` session, must not replace its record.
        self._bootstrap(_UNSUPPORTED_VERSION_INFO)
        self._bootstrap()

        self.assertEqual(self._read_status()["status"], STATUS_UNSUPPORTED_RUNTIME)
        self.assertEqual(self._leftover_temporary_files(), [])

    def test_process_that_loses_the_race_to_create_the_status_file_continues(self):
        with patch.object(sitecustomize.os, "link", side_effect=FileExistsError("created by another process")):
            status = self._bootstrap()

        self.assertEqual(status, STATUS_ACTIVATED)
        self.assertEqual(self._leftover_temporary_files(), [])

    def test_unwritable_status_volume_is_skipped_silently(self):
        with patch.object(sitecustomize.os, "link", side_effect=PermissionError("read-only volume")):
            status = self._bootstrap(_UNSUPPORTED_VERSION_INFO)

        self.assertEqual(status, STATUS_UNSUPPORTED_RUNTIME)
        self.assertFalse(os.path.exists(os.path.join(self.payload_root, STATUS_FILE_NAME)))
        self.assertEqual(self._leftover_temporary_files(), [])

    def test_missing_payload_metadata_reports_unknown_adot_version(self):
        shutil.rmtree(os.path.join(self.payload_root, f"aws_opentelemetry_distro-{_ADOT_VERSION}.dist-info"))

        self._bootstrap()

        self.assertIsNone(self._read_status()["adot_version"])

    def test_python_runs_bootstrap_when_it_imports_sitecustomize_at_startup(self):
        # Runs a real interpreter with the operator's PYTHONPATH layout, against a fake payload whose
        # initialize() records the call. Also serves as a control: upstream's sitecustomize.py also
        # calls initialize() here, but writes no status file.
        shutil.copy(sitecustomize.__file__, os.path.join(self.bootstrap_directory, "sitecustomize.py"))
        Path(self.bootstrap_directory, "__init__.py").write_text(
            "import os\n\ndef initialize(swallow_exceptions=True):\n    print('initialized', os.getpid())\n"
        )
        for package in ("opentelemetry", os.path.join("opentelemetry", "instrumentation")):
            Path(self.payload_root, package, "__init__.py").touch()
        pythonpath = os.pathsep.join([self.bootstrap_directory, self.customer_entry, self.payload_root])

        result = self._run_python("pass", pythonpath)

        self.assertEqual(result.stdout.count("initialized"), 1)
        self.assertEqual(result.stderr, "")
        self.assertEqual(self._read_status()["status"], STATUS_ACTIVATED)

    def test_importing_from_the_distro_package_does_not_run_the_bootstrap(self):
        # The unit tests import the module by its package name. Only the top-level `sitecustomize` import at
        # interpreter startup may run the bootstrap, otherwise importing it would initialize OpenTelemetry.
        result = self._run_python(
            "import sys; import amazon.opentelemetry.distro.bootstrap.sitecustomize; "
            "print('opentelemetry.instrumentation.auto_instrumentation' in sys.modules)",
            pythonpath=None,
        )

        self.assertEqual(result.stdout.strip(), "False")

    @staticmethod
    def _run_python(code, pythonpath):
        environment = {key: value for key, value in os.environ.items() if not key.startswith("COVERAGE_")}
        environment.pop("PYTHONPATH", None)
        if pythonpath is not None:
            environment["PYTHONPATH"] = pythonpath
        return subprocess.run([sys.executable, "-c", code], env=environment, capture_output=True, text=True, check=True)
