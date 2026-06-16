# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""DI contract tests for Flask application.

Verifies that DI instruments functions, emits snapshots as OTLP LogRecords
to the mock collector, and that attributes/body/trace context are populated.

All test classes follow the same OTLP-based pattern as the trace/metrics tests:
- Snapshots are OTLP LogRecords queried from the mock collector via gRPC
- Flat attributes (aws.di.*) are used for filtering and queryable assertions
- Structured body (captures, stack) is used for data content assertions
"""

import time

from typing_extensions import override

from amazon.di.di_contract_test_base import DITestInfrastructure

_APP_IMAGE = "aws-application-signals-tests-di-flask-app"


# =============================================================================
# Function-level BREAKPOINT tests
# =============================================================================


class DIFlaskFunctionLevelTest(DITestInfrastructure):
    """Function-level breakpoint (line=0) produces a method-level snapshot."""

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_function_level_snapshot_generated(self) -> None:
        """Function-level breakpoint generates snapshot with captures.entry/return in body."""
        response = self.send_request("GET", "success")
        self.assertEqual(200, response.status_code)

        logs = self.wait_for_snapshots(min_count=1)
        method_logs = self.logs_for_method(logs, "process_data")
        self.assertGreater(len(method_logs), 0, "Expected OTLP snapshot for process_data")

        log = method_logs[0]
        self.assert_snapshot_attr(log, "aws.di.instrumentation_level", "method")
        self.assert_body_has_entry_or_return(log)

    def test_snapshot_has_trace_context(self) -> None:
        """LogRecord should carry trace_id/span_id from the active OTel span."""
        self.send_request("GET", "success")
        logs = self.wait_for_snapshots(min_count=1)
        self.assert_has_trace_context(logs[0])

    def test_snapshot_has_instrumentation_location(self) -> None:
        """Snapshot attributes should carry location info."""
        self.send_request("GET", "success")
        logs = self.wait_for_snapshots(min_count=1)
        self.assert_snapshot_has_attr(logs[0], "aws.di.method_name")
        self.assert_snapshot_has_attr(logs[0], "aws.di.code_unit")
        self.assert_snapshot_has_attr(logs[0], "aws.di.snapshot_id")

    def test_snapshot_has_stack_frames(self) -> None:
        """Snapshot body should carry stack frames."""
        self.send_request("GET", "success")
        logs = self.wait_for_snapshots(min_count=1)
        self.assert_body_has_stack(logs[0])

    def test_snapshot_event_name(self) -> None:
        """event.name attribute must be 'aws.dynamic_instrumentation.snapshot'."""
        self.send_request("GET", "success")
        logs = self.wait_for_snapshots(min_count=1)
        self.assert_snapshot_attr(logs[0], "event.name", "aws.dynamic_instrumentation.snapshot")

    def test_multiple_requests_generate_multiple_snapshots(self) -> None:
        """Multiple requests should generate multiple snapshots (up to rate limit)."""
        for _ in range(3):
            self.send_request("GET", "success")

        logs = self.wait_for_snapshots(min_count=3)
        method_logs = self.logs_for_method(logs, "process_data")
        self.assertGreaterEqual(len(method_logs), 3)


# =============================================================================
# PROBE instrumentation tests
# =============================================================================


class DIFlaskProbeTest(DITestInfrastructure):
    """Test PROBE instrumentation (permanent, method-level only, no hit limit)."""

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_probe_creates_snapshot(self) -> None:
        """PROBE instrumentation creates a method-level snapshot with entry/return captures."""
        response = self.send_request("GET", "probe")
        self.assertEqual(200, response.status_code)

        logs = self.wait_for_snapshots(min_count=1)
        probe_logs = self.logs_for_method(logs, "compute_total")
        self.assertGreater(len(probe_logs), 0, "Expected snapshot for compute_total (PROBE)")

        log = probe_logs[0]
        self.assert_snapshot_attr(log, "aws.di.instrumentation_level", "method")
        self.assert_body_has_entry_or_return(log)

    def test_probe_snapshot_has_location(self) -> None:
        """PROBE snapshot has correct instrumentation location attributes."""
        self.send_request("GET", "probe")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "compute_total")[0]

        self.assert_snapshot_attr(log, "aws.di.code_unit", "di_flask_server")
        self.assert_snapshot_attr(log, "aws.di.method_name", "compute_total")

    def test_probe_snapshot_has_trace_context(self) -> None:
        """PROBE snapshot includes trace context."""
        self.send_request("GET", "probe")
        logs = self.wait_for_snapshots(min_count=1)
        self.assert_has_trace_context(self.logs_for_method(logs, "compute_total")[0])

    def test_probe_captures_arguments(self) -> None:
        """PROBE snapshot captures function arguments."""
        self.send_request("GET", "probe")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "compute_total")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        entry = captures.get("entry", {})
        arguments = entry.get("arguments", {})
        self.assertIn("items", arguments, "Expected 'items' argument to be captured")

    def test_probe_captures_return_value(self) -> None:
        """PROBE snapshot captures return value."""
        self.send_request("GET", "probe")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "compute_total")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        return_ctx = captures.get("return", {})
        return_value = return_ctx.get("return_value", {})
        self.assertEqual(return_value.get("type"), "int")
        self.assertEqual(return_value.get("value"), "60")

    def test_probe_and_breakpoint_coexist_on_different_functions(self) -> None:
        """PROBE and BREAKPOINT generate snapshots for different functions."""
        self.send_request("GET", "success")  # triggers BREAKPOINT on process_data
        self.send_request("GET", "probe")  # triggers PROBE on compute_total

        logs = self.wait_for_snapshots(min_count=2)
        breakpoint_logs = self.logs_for_method(logs, "process_data")
        probe_logs = self.logs_for_method(logs, "compute_total")

        self.assertGreater(len(breakpoint_logs), 0, "Expected BREAKPOINT snapshot for process_data")
        self.assertGreater(len(probe_logs), 0, "Expected PROBE snapshot for compute_total")

    def test_probe_no_hit_limit(self) -> None:
        """PROBE generates snapshots on multiple invocations (no hit limit)."""
        for _ in range(5):
            self.send_request("GET", "probe")
            time.sleep(0.2)

        logs = self.wait_for_snapshots(min_count=5)
        probe_logs = self.logs_for_method(logs, "compute_total")
        self.assertGreaterEqual(len(probe_logs), 5, "PROBE should have no hit limit")


# =============================================================================
# Line-level BREAKPOINT tests
# =============================================================================


class DIFlaskLineLevelTest(DITestInfrastructure):
    """Test line-level BREAKPOINT instrumentation (lineNumber > 0).

    Line-level breakpoints capture local variables at a specific line,
    rather than entry/return captures for function-level breakpoints.
    """

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_line_level_snapshot_generated(self) -> None:
        """Line-level breakpoint generates snapshot with instrumentation_level=line."""
        response = self.send_request("GET", "line-level")
        self.assertEqual(200, response.status_code)

        logs = self.wait_for_snapshots(min_count=1)
        line_logs = self.logs_for_method(logs, "calculate_sum")
        self.assertGreater(len(line_logs), 0, "Expected snapshot for calculate_sum")

        log = line_logs[0]
        self.assert_snapshot_attr(log, "aws.di.instrumentation_level", "line")
        self.assert_snapshot_has_attr(log, "aws.di.line_number")

    def test_line_level_snapshot_has_captures_lines(self) -> None:
        """Line-level snapshot has captures.lines with local variables."""
        self.send_request("GET", "line-level")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "calculate_sum")[0]
        self.assert_body_has_lines_capture(log)

    def test_line_level_captures_locals(self) -> None:
        """Line-level snapshot captures local variables at the breakpoint line."""
        self.send_request("GET", "line-level")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "calculate_sum")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        lines = captures.get("lines", {})
        # Get the first (and only) line capture
        line_key = list(lines.keys())[0]
        line_capture = lines[line_key]
        locals_captured = line_capture.get("locals", {})
        # At minimum, function arguments should be available as locals
        self.assertTrue(
            "a" in locals_captured or "b" in locals_captured or "result" in locals_captured,
            f"Expected local variables (a, b, or result), got: {list(locals_captured.keys())}",
        )

    def test_line_level_differs_from_function_level(self) -> None:
        """Line-level and function-level snapshots have different capture structures."""
        self.send_request("GET", "success")  # function-level
        self.send_request("GET", "line-level")  # line-level

        logs = self.wait_for_snapshots(min_count=2)
        func_logs = self.logs_for_method(logs, "process_data")
        line_logs = self.logs_for_method(logs, "calculate_sum")

        self.assertGreater(len(func_logs), 0)
        self.assertGreater(len(line_logs), 0)

        self.assert_snapshot_attr(func_logs[0], "aws.di.instrumentation_level", "method")
        self.assert_snapshot_attr(line_logs[0], "aws.di.instrumentation_level", "line")


# =============================================================================
# Hit limit tests
# =============================================================================


class DIFlaskHitLimitTest(DITestInfrastructure):
    """Test BREAKPOINT hit limit behavior.

    BREAKPOINTs have a max_hits limit. With MaxHits=3, the check is
    hit_count > max_hits, so hits 1, 2, and 3 generate snapshots and the
    breakpoint is disabled on hit 4.
    """

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_breakpoint_generates_snapshots_up_to_limit(self) -> None:
        """BREAKPOINT generates a snapshot for each hit up to and including max_hits."""
        # limited_function has MaxHits=3, so 3 snapshots should be generated
        self.send_request("GET", "limited")
        self.send_request("GET", "limited")
        self.send_request("GET", "limited")

        logs = self.wait_for_snapshots(min_count=3)
        limited_logs = self.logs_for_method(logs, "limited_function")
        self.assertEqual(len(limited_logs), 3, "Expected exactly 3 snapshots (MaxHits=3 allows 3)")

    def test_breakpoint_disabled_after_hit_limit(self) -> None:
        """BREAKPOINT stops generating snapshots after hit limit is reached."""
        # First 3 calls generate snapshots (hits 1, 2, and 3)
        self.send_request("GET", "limited")
        self.send_request("GET", "limited")
        self.send_request("GET", "limited")

        logs = self.wait_for_snapshots(min_count=3)
        initial_count = len(self.logs_for_method(logs, "limited_function"))
        self.assertEqual(initial_count, 3)

        # 4th call hits the limit -- no new snapshot
        self.send_request("GET", "limited")
        time.sleep(5)

        final_logs = self._peek_snapshots()
        final_count = len(self.logs_for_method(final_logs, "limited_function"))
        self.assertEqual(
            final_count,
            3,
            f"Expected 3 snapshots (MaxHits=3), but got {final_count}. "
            "BREAKPOINT should be disabled after hitting limit.",
        )


# =============================================================================
# PROBE + BREAKPOINT coexistence on same function
# =============================================================================


class DIFlaskCoexistenceTest(DITestInfrastructure):
    """Test PROBE and BREAKPOINT coexistence on the same function.

    Current DI merges PROBE+BREAKPOINT on the same function into a single
    wrapper that generates one snapshot per invocation. This tests that both
    config types are accepted and the function works correctly.
    """

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_shared_function_is_instrumented(self) -> None:
        """Function with both PROBE and BREAKPOINT configs generates snapshots."""
        response = self.send_request("GET", "shared")
        self.assertEqual(200, response.status_code)

        logs = self.wait_for_snapshots(min_count=1)
        shared_logs = self.logs_for_method(logs, "shared_function")
        self.assertGreaterEqual(len(shared_logs), 1, "Expected snapshot for shared_function")

    def test_shared_function_has_location_hash(self) -> None:
        """Snapshot from shared function has a valid locationHash."""
        self.send_request("GET", "shared")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "shared_function")[0]

        self.assert_snapshot_has_attr(log, "aws.di.location_hash")
        location_hash = self.attrs(log).get("aws.di.location_hash")
        self.assertIn(
            location_hash,
            ["aabb000000000005", "aabb000000000006"],
            f"locationHash should be from PROBE or BREAKPOINT config, got: {location_hash}",
        )

    def test_shared_function_multiple_invocations(self) -> None:
        """Shared function generates snapshots on multiple invocations."""
        for _ in range(3):
            self.send_request("GET", "shared")
            time.sleep(0.2)

        logs = self.wait_for_snapshots(min_count=3)
        shared_logs = self.logs_for_method(logs, "shared_function")
        self.assertGreaterEqual(len(shared_logs), 3, f"Expected 3+ snapshots, got {len(shared_logs)}")


# =============================================================================
# Capture limit tests
# =============================================================================


class DIFlaskCaptureLimitsTest(DITestInfrastructure):
    """Tests that DI capture limits are enforced correctly.

    The breakpoint configs intentionally request limits above the allowed maximum
    (e.g., MaxStringLength=9999, MaxCollectionWidth=9999). The agent must clamp these
    to the enforced maximums.

    Current enforced maximums (from _data_models.py):
        MAX_MAX_STRING_LENGTH = 255
        MAX_MAX_COLLECTION_WIDTH = 20
    """

    __test__ = True

    # Enforced maximums -- update these if CaptureConfig limits change
    ENFORCED_MAX_STRING_LENGTH = 255
    ENFORCED_MAX_COLLECTION_WIDTH = 20

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_string_value_truncated_at_enforced_maximum(self) -> None:
        """String argument should be truncated at ENFORCED_MAX_STRING_LENGTH (255)."""
        self.send_request("GET", "limits-string")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "process_long_string")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        entry = captures.get("entry", {})
        arguments = entry.get("arguments", {})
        long_string_arg = arguments.get("long_string", {})

        self.assertIsNotNone(long_string_arg, "Expected 'long_string' argument to be captured")

        captured_value = long_string_arg.get("value")
        self.assertIsNotNone(captured_value, "Captured string value should not be None")
        self.assertEqual(
            len(captured_value),
            self.ENFORCED_MAX_STRING_LENGTH,
            f"String should be truncated at enforced max {self.ENFORCED_MAX_STRING_LENGTH}, "
            f"but was {len(captured_value)}.",
        )
        self.assertTrue(long_string_arg.get("truncated", False), "Captured string should be marked as truncated")

    def test_collection_elements_capped_at_enforced_maximum(self) -> None:
        """Collection argument should be capped at ENFORCED_MAX_COLLECTION_WIDTH (20) elements."""
        self.send_request("GET", "limits-collection")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "process_large_collection")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        entry = captures.get("entry", {})
        arguments = entry.get("arguments", {})
        large_list_arg = arguments.get("large_list", {})

        self.assertIsNotNone(large_list_arg, "Expected 'large_list' argument to be captured")

        elements = large_list_arg.get("elements", [])
        self.assertIsNotNone(elements, "Captured collection should have 'elements'")
        self.assertEqual(
            len(elements),
            self.ENFORCED_MAX_COLLECTION_WIDTH,
            f"Collection should be capped at enforced max {self.ENFORCED_MAX_COLLECTION_WIDTH} elements, "
            f"but had {len(elements)}.",
        )

        size = large_list_arg.get("size")
        self.assertIsNotNone(size, "Captured collection should report original size")
        self.assertEqual(size, 50, "Original collection size should be 50")

    def test_string_value_truncated_at_user_supplied_limit_below_maximum(self) -> None:
        """Function-level capture must honor a user-supplied limit below the maximum.

        The config requests MaxStringLength=10 (well within range) and the input is
        100 chars. The captured value must be truncated to exactly 10 -- not the
        maximum (255) -- proving the function path uses the per-config limit rather
        than a fixed serializer.
        """
        self.send_request("GET", "limits-small-string")
        logs = self.wait_for_snapshots(min_count=1)
        log = self.logs_for_method(logs, "process_small_limit_string")[0]

        body = self.body(log)
        captures = body.get("captures", {})
        entry = captures.get("entry", {})
        arguments = entry.get("arguments", {})
        small_string_arg = arguments.get("small_limit_string", {})

        self.assertIsNotNone(small_string_arg, "Expected 'small_limit_string' argument to be captured")

        captured_value = small_string_arg.get("value")
        self.assertIsNotNone(captured_value, "Captured string value should not be None")
        self.assertEqual(
            len(captured_value),
            10,
            f"String should be truncated at the user-supplied limit of 10, but was {len(captured_value)}.",
        )
        self.assertTrue(small_string_arg.get("truncated", False), "Captured string should be marked as truncated")
        self.assertEqual(small_string_arg.get("size"), 100, "Captured string should report original size of 100")
