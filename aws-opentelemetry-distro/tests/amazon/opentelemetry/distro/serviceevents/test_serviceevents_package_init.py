# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

import amazon.opentelemetry.distro.serviceevents as se


class TestServiceEventsPackageInit(TestCase):
    """Cover the lazy __getattr__ exports on the serviceevents package."""

    def test_lazy_import_service_events_config(self):
        """Accessing ServiceEventsConfig resolves the lazy import."""
        self.assertIsNotNone(se.ServiceEventsConfig)

    def test_lazy_import_service_events_instrumentation(self):
        """Accessing ServiceEventsInstrumentation resolves the lazy import."""
        self.assertIsNotNone(se.ServiceEventsInstrumentation)

    def test_lazy_import_get_serviceevents_instrumentation(self):
        """Accessing get_serviceevents_instrumentation resolves the lazy import."""
        self.assertIsNotNone(se.get_serviceevents_instrumentation)

    def test_all_lists_lazy_exported_names(self):
        """__all__ advertises exactly the lazily-exported public names."""
        self.assertEqual(
            set(se.__all__),
            {
                "ServiceEventsConfig",
                "ServiceEventsInstrumentation",
                "get_serviceevents_instrumentation",
            },
        )

    def test_unknown_attribute_raises_attribute_error(self):
        """An unknown attribute falls through to the AttributeError branch."""
        with self.assertRaises(AttributeError):
            _ = se.does_not_exist
