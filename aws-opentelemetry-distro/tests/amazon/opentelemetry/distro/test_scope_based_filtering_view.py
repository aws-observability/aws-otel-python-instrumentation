# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import unittest
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.scope_based_filtering_view import ScopeBasedRetainingView
from opentelemetry.metrics import Instrument


class TestScopeBasedRetainingView(unittest.TestCase):
    def test_retained(self):
        instrument: Instrument = MagicMock()
        instrument.instrumentation_scope.name = "not_matched"
        view = ScopeBasedRetainingView(meter_name="test_meter")
        self.assertTrue(view._match(instrument))

    def test_dropped(self):
        instrument: Instrument = MagicMock()
        instrument.instrumentation_scope.name = "test_meter"
        view = ScopeBasedRetainingView(meter_name="test_meter")
        self.assertFalse(view._match(instrument))
