# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""DI contract tests for a pure-Starlette application.

Verifies that DI instruments a Starlette route handler. Starlette is NOT FastAPI
(FastAPI subclasses Starlette and is patched by its own path); a pure-Starlette app
builds ``route.app = request_response(endpoint)`` at import time, so DI must rebuild
``route.app`` to instrument the handler. Without the fix the handler breakpoint
silently never fires.

Follows the same OTLP/mock-collector pattern as flask_test.py / fastapi_test.py.
"""

from typing_extensions import override

from amazon.di.di_contract_test_base import DITestInfrastructure

_APP_IMAGE = "aws-application-signals-tests-di-starlette-app"
_CODE_UNIT = "di_starlette_server"


class DIStarletteFunctionLevelTest(DITestInfrastructure):
    """A plain (non-handler) function is instrumented — control proving DI is active."""

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_function_level_snapshot_generated(self) -> None:
        response = self.send_request("GET", "success")
        self.assertEqual(200, response.status_code)

        logs = self.wait_for_snapshots(min_count=1)
        method_logs = self.logs_for_method(logs, "process_data")
        self.assertGreater(len(method_logs), 0, "Expected OTLP snapshot for process_data")

        log = method_logs[0]
        self.assert_snapshot_attr(log, "aws.di.instrumentation_level", "method")
        self.assert_snapshot_attr(log, "aws.di.code_unit", _CODE_UNIT)
        self.assert_body_has_entry_or_return(log)


class DIStarletteRouteHandlerTest(DITestInfrastructure):
    """A Starlette route handler configured as a DI target produces a snapshot.

    Starlette captures the handler in a ``request_response`` closure on ``route.app`` at
    import time and invokes that per request (never ``route.endpoint``). DI must rebuild
    ``route.app`` from the wrapper, so the handler fires and a snapshot is produced.
    """

    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Ready"

    def test_route_handler_produces_snapshot(self) -> None:
        # Control: the plain instrumented function works, proving DI is active in this
        # Starlette process and the mock collector is receiving snapshots.
        control = self.send_request("GET", "success")
        self.assertEqual(200, control.status_code)
        self.wait_for_snapshots(min_count=1)

        # Hit the Starlette route handler that is itself a DI target.
        response = self.send_request("GET", "handler", params={"multiplier": 2})
        self.assertEqual(200, response.status_code)
        # The handler still runs normally (DI must never break the application).
        self.assertEqual(response.json().get("result"), 42)

        handler_logs = self.wait_for_method_snapshots("starlette_handler", min_count=1)
        self.assertGreater(
            len(handler_logs),
            0,
            "Expected a snapshot for the Starlette route handler 'starlette_handler' "
            "(DI now rebuilds Starlette's route.app).",
        )

        log = handler_logs[0]
        self.assert_snapshot_attr(log, "aws.di.instrumentation_level", "method")
        self.assert_snapshot_attr(log, "aws.di.code_unit", _CODE_UNIT)
        self.assert_body_has_entry_or_return(log)
