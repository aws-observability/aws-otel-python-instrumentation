# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from amazon.opentelemetry.distro.debugger._status_reporter import ConfigurationStatus, StatusReporter


class TestStatusReporterSignalType(unittest.TestCase):
    """Tests that StatusReporter uses SNAPSHOT signal type instead of SPAN."""

    def test_build_status_entry_uses_snapshot(self):
        entry = StatusReporter._build_status_entry(
            instrumentation_type="BREAKPOINT",
            signal_type="SNAPSHOT",
            location_hash="hash123",
            status=ConfigurationStatus.ACTIVE,
        )
        self.assertEqual(entry["SignalType"], "SNAPSHOT")
        self.assertEqual(entry["InstrumentationType"], "BREAKPOINT")
        self.assertEqual(entry["LocationHash"], "hash123")
        self.assertEqual(entry["Status"], "ACTIVE")

    def test_build_status_entry_ready(self):
        entry = StatusReporter._build_status_entry(
            instrumentation_type="PROBE",
            signal_type="SNAPSHOT",
            location_hash="hash456",
            status=ConfigurationStatus.READY,
        )
        self.assertEqual(entry["SignalType"], "SNAPSHOT")
        self.assertEqual(entry["Status"], "READY")

    def test_config_key_uses_snapshot(self):
        key = StatusReporter._get_config_key("SNAPSHOT", "hash123", ConfigurationStatus.READY)
        self.assertEqual(key, "SNAPSHOT:hash123:READY")

    def test_config_key_active_no_status_suffix(self):
        key = StatusReporter._get_config_key("SNAPSHOT", "hash123", ConfigurationStatus.ACTIVE)
        self.assertEqual(key, "SNAPSHOT:hash123")
