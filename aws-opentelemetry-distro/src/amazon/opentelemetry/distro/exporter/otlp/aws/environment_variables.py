# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

AWS_EXPORTER_SERVICE = "AWS_EXPORTER_SERVICE"
"""
.. envvar:: AWS_EXPORTER_SERVICE

The :envvar:`AWS_EXPORTER_SERVICE` sets the AWS signing service for
the OTLP SigV4 exporters. Signal-specific service variables take higher precedence.
This variable is optional. When unset, the exporters default to ``xray`` for spans,
``monitoring`` for metrics, and ``logs`` for logs.
"""

AWS_TRACES_EXPORTER_SERVICE = "AWS_TRACES_EXPORTER_SERVICE"
"""
.. envvar:: AWS_TRACES_EXPORTER_SERVICE

Same as :envvar:`AWS_EXPORTER_SERVICE` but only for the span
exporter. If both are present, this takes higher precedence.
Default: xray
"""

AWS_METRICS_EXPORTER_SERVICE = "AWS_METRICS_EXPORTER_SERVICE"
"""
.. envvar:: AWS_METRICS_EXPORTER_SERVICE

Same as :envvar:`AWS_EXPORTER_SERVICE` but only for the metrics
exporter. If both are present, this takes higher precedence.
Default: monitoring
"""

AWS_LOGS_EXPORTER_SERVICE = "AWS_LOGS_EXPORTER_SERVICE"
"""
.. envvar:: AWS_LOGS_EXPORTER_SERVICE

Same as :envvar:`AWS_EXPORTER_SERVICE` but only for the log
exporter. If both are present, this takes higher precedence.
Default: logs
"""
