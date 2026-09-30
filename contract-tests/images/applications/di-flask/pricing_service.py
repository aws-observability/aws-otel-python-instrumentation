# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Service module for the DI Flask contract app.

This module exists specifically to exercise the ``from module import func``
aliasing case for method-level (function-level) breakpoints. ``di_flask_server``
imports ``apply_discount`` via ``from pricing_service import apply_discount`` and
calls it by its bare name, so the executing call site holds a reference in the
*importing* module's namespace -- not in this defining module. A method-level
breakpoint must redirect that alias for instrumentation to fire.
"""


def apply_discount(price_cents, discount_percent):
    """Method-level BREAKPOINT target reached via a `from x import` alias."""
    discount = price_cents * discount_percent // 100
    final_cents = price_cents - discount
    return final_cents
