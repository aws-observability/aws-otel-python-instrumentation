# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Helper functions for serviceevents Flask contract test.

These are in a separate module so that the AST instrumentation hooks
(installed before user-code imports) can transform them properly.
The __main__ module is loaded before hooks are active.
"""


def compute_result(x):
    return x * 2


def validate_input(value):
    if not value:
        raise ValueError("Invalid input")
    return True


class BusinessLogic:
    # process() is deliberately an instance method (not static) so the AST
    # instrumentation hooks exercise method-level function telemetry.
    def process(self, data):  # pylint: disable=no-self-use
        return compute_result(len(data))
