# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Branch-coverage tests for _data_models.py parse/validation paths.

These complement test_data_models.py by feeding from_api_config the malformed
and PROBE-specific inputs that exercise the remaining validation branches
(bool line numbers, non-dict CodeLocation/CodeCapture, invalid InstrumentationType,
PROBE rules around InstrumentationName / ExpiresAt / MaxHits, and CreatedAt
parsing), plus the simple property accessors and the arg_mappings=None guard.
"""

import unittest

from amazon.opentelemetry.distro.debugger._data_models import DEFAULT_MAX_HITS, BreakpointConfiguration, CaptureConfig


def _location(line_number=10, code_unit="myapp", method_name="func", language="python"):
    code_location = {"Language": language, "CodeUnit": code_unit, "MethodName": method_name}
    if line_number is not None:
        code_location["LineNumber"] = line_number
    return {"CodeLocation": code_location}


class TestCaptureConfigArgMappingsNoneGuard(unittest.TestCase):
    """Covers the arg_mappings-is-None guard in __post_init__ (line 88)."""

    def test_explicit_none_arg_mappings_becomes_empty_dict(self):
        config = CaptureConfig(arg_mappings=None)
        self.assertEqual(config.arg_mappings, {})


class TestBreakpointConfigurationProperties(unittest.TestCase):
    """Covers the is_permanent / is_temporary property accessors (lines 222, 227)."""

    def _config(self, instrumentation_type):
        return BreakpointConfiguration(
            module="m",
            function_name="f",
            line_number=0,
            capture_config=CaptureConfig(),
            config_id="id",
            instrumentation_type=instrumentation_type,
        )

    def test_probe_is_permanent_not_temporary(self):
        config = self._config("PROBE")
        self.assertTrue(config.is_permanent)
        self.assertFalse(config.is_temporary)

    def test_breakpoint_is_temporary_not_permanent(self):
        config = self._config("BREAKPOINT")
        self.assertTrue(config.is_temporary)
        self.assertFalse(config.is_permanent)


class TestFromApiConfigBoolLineNumber(unittest.TestCase):
    """Covers safe_int's bool-guard (line 281): a bool LineNumber falls back to default 0."""

    def test_bool_line_number_falls_back_to_zero(self):
        api_config = {
            "Location": _location(line_number=True),
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        # bool is rejected by safe_int -> default 0 (function-level).
        self.assertEqual(config.line_number, 0)


class TestFromApiConfigSafeBoolException(unittest.TestCase):
    """Covers safe_bool's except branch (lines 294-295) via a value whose __bool__ raises."""

    def test_capture_return_bool_raises_falls_back_to_default(self):
        class ExplodingBool:
            def __bool__(self):
                raise ValueError("cannot coerce")

        api_config = {
            "Location": _location(),
            "CaptureConfiguration": {"CodeCapture": {"CaptureReturn": ExplodingBool()}},
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        # bool() raised -> safe_bool returned the default (False).
        self.assertFalse(config.capture_config.capture_return)


class TestFromApiConfigLocationVariants(unittest.TestCase):
    """Covers non-dict CodeLocation (308-309) and the legacy flat fallback (311-312)."""

    def test_non_dict_code_location_returns_none(self):
        api_config = {"Location": {"CodeLocation": "not-a-dict"}, "LocationHash": "h"}
        self.assertIsNone(BreakpointConfiguration.from_api_config(api_config))

    def test_legacy_flat_location_is_parsed(self):
        # No CodeLocation key -> the raw_location itself is used (legacy format).
        api_config = {
            "Location": {"Language": "python", "CodeUnit": "legacymod", "MethodName": "legacyfunc", "LineNumber": 7},
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertEqual(config.module, "legacymod")
        self.assertEqual(config.function_name, "legacyfunc")
        self.assertEqual(config.line_number, 7)


class TestFromApiConfigInvalidInstrumentationType(unittest.TestCase):
    """Covers the invalid-InstrumentationType warning + reset to BREAKPOINT (lines 342-349)."""

    def test_unknown_type_defaults_to_breakpoint(self):
        api_config = {
            "Location": _location(),
            "InstrumentationType": "TRACEPOINT",  # not PROBE/BREAKPOINT
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertEqual(config.instrumentation_type, "BREAKPOINT")


class TestFromApiConfigProbeRules(unittest.TestCase):
    """Covers PROBE-specific branches: optional name, ExpiresAt ignored, MaxHits ignored."""

    def test_probe_missing_instrumentation_name_defaults_to_empty(self):
        api_config = {
            "Location": _location(line_number=0),
            "InstrumentationType": "PROBE",
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertEqual(config.instrumentation_name, "")

    def test_probe_ignores_expires_at_and_max_hits(self):
        api_config = {
            "Location": _location(line_number=0),
            "InstrumentationType": "PROBE",
            "InstrumentationName": "my-probe",
            "ExpiresAt": 1700000000,  # ignored for PROBE (line 456)
            "CaptureConfiguration": {"CodeCapture": {"CaptureLimits": {"MaxHits": 5}}},  # ignored for PROBE (line 483)
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNone(config.expires_at)
        # MaxHits is ignored for PROBE; the default is retained.
        self.assertEqual(config.max_hits, DEFAULT_MAX_HITS)


class TestFromApiConfigNonDictCodeCapture(unittest.TestCase):
    """Covers the non-dict CodeCapture branch (lines 390-391)."""

    def test_non_dict_code_capture_uses_defaults(self):
        api_config = {
            "Location": _location(),
            "CaptureConfiguration": {"CodeCapture": "not-a-dict"},
            "LocationHash": "h",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        # CodeCapture was invalid -> capture config falls back to defaults.
        self.assertFalse(config.capture_config.capture_return)
        self.assertIsNone(config.capture_config.capture_arguments)


class TestFromApiConfigCreatedAt(unittest.TestCase):
    """Covers CreatedAt parsing branches (lines 462-468)."""

    def test_created_at_unix_timestamp(self):
        api_config = {"Location": _location(), "CreatedAt": 1700000000, "LocationHash": "h"}
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNotNone(config.created_at)

    def test_created_at_iso_string(self):
        api_config = {"Location": _location(), "CreatedAt": "2025-10-27T19:34:00Z", "LocationHash": "h"}
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNotNone(config.created_at)

    def test_created_at_invalid_is_none(self):
        api_config = {"Location": _location(), "CreatedAt": "not-a-date", "LocationHash": "h"}
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNone(config.created_at)


if __name__ == "__main__":
    unittest.main()
