# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from server import SpanMetricsApplication


class AutoInstrumentedApplication(SpanMetricsApplication):
    def configure_instrumentation(self, app):
        return None


if __name__ == "__main__":
    AutoInstrumentedApplication().run()
