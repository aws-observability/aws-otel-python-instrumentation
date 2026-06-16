# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Main entry point for ServiceEvents instrumentation.

This module manages the lifecycle of AST hooks, Python monitor, and collectors
for deep observability of Python applications.
"""

import atexit
import logging
import os
from typing import List, Optional

from amazon.opentelemetry.distro.serviceevents.ast_transformation import install_ast_hooks
from amazon.opentelemetry.distro.serviceevents.collectors.deployment_event_collector import DeploymentEventCollector
from amazon.opentelemetry.distro.serviceevents.collectors.endpoint_collector import EndpointMetricCollector
from amazon.opentelemetry.distro.serviceevents.collectors.incident_snapshot_collector import IncidentSnapshotCollector
from amazon.opentelemetry.distro.serviceevents.config import ServiceEventsConfig
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState

logger = logging.getLogger(__name__)

# Module-level singleton instance
_serviceevents_instance: Optional["ServiceEventsInstrumentation"] = None


def _build_log_otlp_exporter(logs_endpoint: str, headers: dict, compression):
    """Build an OTLP log exporter for the configured logs endpoint.

    When the endpoint matches the CloudWatch Logs OTLP pattern
    (``https://logs.{region}.amazonaws.com/v1/logs``), wrap the upstream
    ``OTLPLogExporter`` with ADOT's ``OTLPAwsLogRecordExporter`` so
    requests are SigV4-signed. The ``x-aws-log-group`` / ``x-aws-log-stream``
    headers travel with every batch (already populated in ``headers``).
    Otherwise return a plain upstream ``OTLPLogExporter`` pointing at the
    collector-proxied endpoint.

    Mirrors the Java SDK's behavior in ``ServiceEventsInstrumentation.java:557``.
    Imports are deferred so this module stays importable without OTel SDK
    and botocore at import time.

    The SigV4 path requires ``botocore`` (an optional distro dependency). When
    it is unavailable we fall back to a plain, unsigned ``OTLPLogExporter``
    against the same endpoint rather than raising — telemetry must never crash
    the host app, and a usable exporter keeps ``initialize()`` from tripping its
    broad-except and silently disabling all of ServiceEvents.
    """
    # Module-local imports to preserve the file's lazy-import posture (defer OTel SDK / botocore).
    # pylint: disable=import-outside-toplevel
    import re

    from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AWS_LOGS_OTLP_ENDPOINT_PATTERN
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

    is_cw_endpoint = bool(re.match(AWS_LOGS_OTLP_ENDPOINT_PATTERN, (logs_endpoint or "").lower()))
    if not is_cw_endpoint:
        return OTLPLogExporter(endpoint=logs_endpoint, headers=headers, compression=compression)

    # Direct-to-CloudWatch (SigV4) path. Use the shared botocore-session helper
    # the rest of the distro relies on (see aws_opentelemetry_configurator
    # ._create_aws_otlp_exporter). It returns a ``botocore.session.Session`` —
    # the type OTLPAwsLogRecordExporter is annotated for — or ``None`` when
    # botocore is not installed.
    # pylint: disable=import-outside-toplevel
    from amazon.opentelemetry.distro._utils import get_aws_session

    session = get_aws_session()
    if not session:
        # botocore unavailable: cannot SigV4-sign. Degrade to a plain OTLP
        # exporter against the same endpoint instead of returning None (the
        # caller does not handle None) or raising into the host app.
        logger.warning(
            "ServiceEvents direct-to-CloudWatch SigV4 export requires botocore, which is not installed; "
            "falling back to an unsigned OTLP log exporter for %s",
            logs_endpoint,
        )
        return OTLPLogExporter(endpoint=logs_endpoint, headers=headers, compression=compression)

    from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_log_record_exporter import OTLPAwsLogRecordExporter

    region = (logs_endpoint or "").lower().split(".")[1]
    # OTLPAwsLogRecordExporter hardcodes compression=Gzip (matches ADOT design —
    # CloudWatch OTLP ingestion assumes gzip for LLO batching). The `compression`
    # argument is honored only on the collector-proxied branch above.
    _ = compression  # suppress unused-kwarg lint on this branch
    return OTLPAwsLogRecordExporter(
        aws_region=region,
        session=session,
        endpoint=logs_endpoint,
        headers=headers,
    )


def get_serviceevents_instrumentation(
    config: Optional["ServiceEventsConfig"] = None,
) -> Optional["ServiceEventsInstrumentation"]:
    """
    Get or create the singleton ServiceEventsInstrumentation instance.

    This ensures only one instance is ever created, preventing duplicate
    initialization (e.g., from both manual init and auto-configurator).

    Args:
        config: ServiceEventsConfig instance. Required on first call, optional afterwards.
                If provided after first call, it is ignored (first config wins).

    Returns:
        The singleton ServiceEventsInstrumentation instance, or None if no config provided
        on first call.
    """
    # Singleton instrumentation state — module-level by design.
    global _serviceevents_instance  # pylint: disable=global-statement

    if _serviceevents_instance is not None:
        if config is not None:
            logger.debug(
                "ServiceEventsInstrumentation singleton already exists "
                "(service=%s), ignoring new config (service=%s)",
                _serviceevents_instance.config.service_name,
                config.service_name,
            )
        return _serviceevents_instance

    if config is None:
        logger.debug("No ServiceEventsInstrumentation instance exists and no config provided")
        return None

    _serviceevents_instance = ServiceEventsInstrumentation(config)
    return _serviceevents_instance


class ServiceEventsInstrumentation:
    """
    Main entry point for ServiceEvents instrumentation.
    Manages lifecycle of AST hooks, Python monitor, and framework instrumentations.
    """

    def __init__(self, config: ServiceEventsConfig):
        """
        Initialize ServiceEvents instrumentation with configuration.

        Args:
            config: ServiceEventsConfig instance with all settings
        """
        self.config = config
        self.monitor_state = None
        self.collectors: List = []  # Will be populated in Phase 2-4
        self._initialized = False
        # Guards against registering more than one atexit hook across re-init cycles.
        self._atexit_registered = False
        # OTLP components
        self._otlp_emitter = None
        self._otlp_logger_provider = None
        self._otlp_meter_provider = None

    # Sequential, tested startup routine — kept whole rather than fragmented.
    def initialize(self) -> None:  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """
        Initialize ServiceEvents instrumentation.

        Steps:
        1. Configure logging level from config
        2. Initialize Python monitor (singleton for aggregation)
        3. Install AST import hooks (if enabled)
        4. Initialize framework instrumentations (Phase 3)
        5. Start periodic collectors (Phase 2-4)
        """
        if self._initialized:
            logger.warning("ServiceEvents instrumentation already initialized, skipping")
            return

        # Configure ServiceEvents logging — reuse OTEL_PYTHON_LOG_LEVEL (default: INFO)
        otel_log_level = os.getenv("OTEL_PYTHON_LOG_LEVEL", "info").upper()
        log_level = getattr(logging, otel_log_level, logging.INFO)
        serviceevents_logger = logging.getLogger("amazon.opentelemetry.distro.serviceevents")
        serviceevents_logger.setLevel(log_level)

        if not serviceevents_logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(log_level)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            serviceevents_logger.addHandler(handler)

        if not self.config.enabled:
            logger.info("ServiceEvents instrumentation disabled via configuration")
            return

        try:
            logger.info("Initializing ServiceEvents instrumentation (Python implementation)")

            # Initialize Python monitor singleton for aggregation
            self.monitor_state = _ServiceEventsMonitorState.get_instance()
            logger.debug("ServiceEvents monitor state initialized")

            # Configure sampling mode from config. Applied unconditionally: the module-level
            # default is "always", so "auto" only takes effect if we actually call the setter
            # (a prior `!= "auto"` guard left auto-mode inert). An unrecognized value — e.g. the
            # removed "adaptive" left in a stale env var — is logged and left at the current
            # default instead of aborting ServiceEvents init (mirrors the Java bridge, which
            # silently retains the default on an invalid mode).
            # Lazy import to avoid import-time coupling with python_monitor.
            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.serviceevents.python_monitor import get_sampling_mode, set_sampling_mode

            try:
                set_sampling_mode(self.config.sampling_mode)
            except ValueError:
                logger.warning(
                    "ServiceEvents: invalid sampling mode '%s'; falling back to '%s'",
                    self.config.sampling_mode,
                    get_sampling_mode(),
                )
            logger.info("ServiceEvents sampling mode set to: %s", get_sampling_mode())

            # Configure sampling thresholds from config
            # Lazy import to avoid import-time coupling with python_monitor.
            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.serviceevents.python_monitor import set_sampling_thresholds

            set_sampling_thresholds(
                tier1_threshold=self.config.sample_tier1_threshold,
                tier2_threshold=self.config.sample_tier2_threshold,
                tier2_rate=self.config.sample_tier2_rate,
                tier3_rate=self.config.sample_tier3_rate,
            )

            # Initialize OTLP emitter when either `output_file` (local-testing
            # file exporter) OR `logs_endpoint` (OTLP network exporter) is set.
            # If both are set, `output_file` wins — it replaces the network
            # exporter, with LOGS_ENDPOINT and METRICS_ENDPOINT ignored.
            otlp_emitter = None
            if self.config.output_file:
                logger.info(
                    "ServiceEvents OUTPUT_FILE mode: %s (LOGS_ENDPOINT and METRICS_ENDPOINT ignored)",
                    self.config.output_file,
                )
                otlp_emitter = self._create_otlp_emitter()
            elif self.config.logs_endpoint:
                otlp_emitter = self._create_otlp_emitter()
            else:
                logger.info(
                    "No ServiceEvents OTLP endpoint or output file configured — telemetry will be written to console"
                )

            # AST hooks for FunctionCall telemetry. Gated on config.function_instrument_enabled:
            # when enabled, bytecode-style AST instrumentation owns FunctionCall plus
            # synchronous incident call_path capture.
            if self.config.function_instrument_enabled:
                # Wire the OTel histogram directly into monitor state for real-time recording.
                # PythonServiceEventsMonitor.__exit__ records each call's duration straight into
                # service.function.duration (OTel ExponentialHistogram) — the single source of
                # truth for FunctionCall. Wired in BOTH network and output_file mode: the file
                # metric exporter emits canonical OTLP metrics JSON (incl. ExponentialHistogram),
                # so the histogram is exported identically in either mode.
                if otlp_emitter:
                    meter = self._otlp_meter_provider.get_meter("serviceevents", "1.0")
                    function_call_histogram = meter.create_histogram(
                        "service.function.duration",
                        unit="Microseconds",
                        description="Function call duration",
                    )
                    # `Telemetry.Source` is the only signal-level base attribute
                    # left on the per-call dimension set. service.name,
                    # environment, deployment.environment.name, the SDK version,
                    # deployment id, and VCS attributes all live on the OTel
                    # Resource (set in _create_otlp_emitter), so they ride
                    # along on every metric data point automatically without
                    # bloating the cardinality budget.
                    base_attrs = {
                        "Telemetry.Source": "ServiceEvents",
                    }
                    self.monitor_state.set_metric_base_attrs(base_attrs)
                    self.monitor_state.set_function_duration_histogram(function_call_histogram)
                    logger.info("Wired OTel histogram into monitor state for direct recording")

                # When packages_include is empty the hooks install but instrument nothing
                # (there is no implicit default scope). That state is visible in the
                # "AST hooks installed. Packages include: ..." log below; we intentionally
                # do not warn, since function instrumentation is on by default and an empty
                # allowlist is a normal, quiet no-op rather than a misconfiguration.
                logger.info("Installing ServiceEvents AST transformation hooks")
                install_ast_hooks(
                    packages_include=set(self.config.packages_include),
                    packages_exclude=self.config.packages_exclude,
                )
                logger.info(
                    "AST hooks installed. Packages include: %s, Packages exclude: %s",
                    self.config.packages_include,
                    self.config.packages_exclude,
                )
            else:
                logger.info("AST disabled: skipping AST hooks")

            # DeploymentEventCollector: emits once at startup + every flush interval, regardless of mode.
            if otlp_emitter:
                deployment_event_collector = DeploymentEventCollector(
                    flush_interval_ms=self.config.deployment_event_flush_interval,
                    environment=self.config.environment,
                    service_name=self.config.service_name,
                    sdk_version=self.config.sdk_version,
                    resource_attributes=self.config.resource_attributes,
                    otlp_emitter=otlp_emitter,
                )
                self.collectors.append(deployment_event_collector)
                deployment_event_collector.start()
                logger.info(
                    "Started DeploymentEventCollector (interval: %sms)",
                    self.config.deployment_event_flush_interval,
                )

            # EndpointMetricCollector: always enabled (framework hooks provide data).
            # suppress_endpoint_summary: when Application Signals is on, skip emitting
            # EndpointSummary LogRecords (App Signals carries equivalent data). The
            # collector still runs so latency histograms feed IncidentSnapshot triggers.
            endpoint_collector = EndpointMetricCollector(
                flush_interval_ms=self.config.endpoint_flush_interval,
                environment=self.config.environment,
                service_name=self.config.service_name,
                sdk_version=self.config.sdk_version,
                resource_attributes=self.config.resource_attributes,
                otlp_emitter=otlp_emitter,
                suppress_endpoint_summary=self.config.application_signals_enabled,
            )
            self.collectors.append(endpoint_collector)
            endpoint_collector.start()
            logger.info("Started EndpointMetricCollector (interval: %sms)", self.config.endpoint_flush_interval)

            # IncidentSnapshotCollector: always runs (AST owns incidents).
            incident_snapshot_collector = IncidentSnapshotCollector(
                flush_interval_ms=self.config.incident_snapshot_flush_interval,
                duration_threshold_ms=self.config.incident_snapshot_duration_threshold_ms,
                max_per_period=self.config.incident_snapshot_max_per_minute,
                environment=self.config.environment,
                service_name=self.config.service_name,
                sdk_version=self.config.sdk_version,
                capture_request_body=self.config.incident_snapshot_capture_request_body,
                max_same_error=self.config.incident_snapshot_max_same_error,
                resource_attributes=self.config.resource_attributes,
                otlp_emitter=otlp_emitter,
            )

            latency_patterns = self.config.get_latency_threshold_patterns()
            if latency_patterns:
                incident_snapshot_collector.set_latency_threshold_patterns(latency_patterns)
                logger.info("Applied %d latency threshold patterns", len(latency_patterns))

            self.collectors.append(incident_snapshot_collector)
            incident_snapshot_collector.start()
            logger.info(
                "Started IncidentSnapshotCollector (interval: %sms)",
                self.config.incident_snapshot_flush_interval,
            )

            # Phase 3: Initialize framework hooks (auto-detect via ImportError)
            try:
                # Optional framework dep — auto-detected via ImportError.
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation import (
                    install_flask_hooks,
                )

                install_flask_hooks(
                    endpoint_collector=endpoint_collector,
                    incident_snapshot_collector=incident_snapshot_collector,
                    config=self.config,
                )
                logger.info("Flask instrumentation hooks installed")
            except ImportError:
                logger.debug("Flask not installed, skipping Flask instrumentation")
            except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
                logger.error("Error installing Flask hooks: %s", exc, exc_info=True)

            try:
                # Optional framework dep — auto-detected via ImportError.
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation import (
                    install_fastapi_hooks,
                )

                install_fastapi_hooks(
                    endpoint_collector=endpoint_collector,
                    incident_snapshot_collector=incident_snapshot_collector,
                    config=self.config,
                )
                logger.info("FastAPI instrumentation hooks installed")
            except ImportError:
                logger.debug("FastAPI not installed, skipping FastAPI instrumentation")
            except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
                logger.error("Error installing FastAPI hooks: %s", exc, exc_info=True)

            try:
                # Optional framework dep — auto-detected via ImportError.
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation import (
                    install_django_hooks,
                )

                install_django_hooks(
                    endpoint_collector=endpoint_collector,
                    incident_snapshot_collector=incident_snapshot_collector,
                    config=self.config,
                )
                logger.info("Django instrumentation hooks installed")
            except ImportError:
                logger.debug("Django not installed, skipping Django instrumentation")
            except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
                logger.error("Error installing Django hooks: %s", exc, exc_info=True)

            # Register fork handler for multi-process servers (e.g., gunicorn)
            try:
                os.register_at_fork(after_in_child=self._reinitialize_after_fork)
            except AttributeError:
                pass  # os.register_at_fork not available (Windows)

            # Flush on interpreter exit. Collectors run on daemon threads, which are killed
            # abruptly at exit without running their final-collection path, and the OTLP
            # providers are never force-flushed — so the last interval's snapshots/metrics
            # would be lost on a clean shutdown. shutdown() is idempotent (guards on
            # _initialized), so a redundant explicit call is harmless. Registered once.
            if not self._atexit_registered:
                atexit.register(self.shutdown)
                self._atexit_registered = True

            self._initialized = True
            logger.info("ServiceEvents instrumentation initialized successfully (service=%s)", self.config.service_name)

        except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
            logger.error("Failed to initialize ServiceEvents instrumentation: %s", exc, exc_info=True)
            # Don't crash application - graceful degradation
            self._initialized = False

    # Tested provider/exporter wiring — kept whole rather than fragmented.
    def _create_otlp_emitter(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Create dedicated OTLP LoggerProvider + MeterProvider + ServiceEventsOtlpEmitter."""
        # Defer OTel SDK / exporter imports so this module imports without those deps.
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.serviceevents.exporter.cloudwatch_file_exporter import (
            ServiceEventsCloudWatchLogFileExporter,
            ServiceEventsCloudWatchMetricFileExporter,
        )
        from amazon.opentelemetry.distro.serviceevents.exporter.otlp_emitter import ServiceEventsOtlpEmitter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
        from opentelemetry.sdk.metrics.export import AggregationTemporality, PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource

        # Build Resource from application context.
        # aws.local.service duplicates service.name for backend compatibility —
        # the backend currently queries aws.local.service and will migrate to
        # service.name in a future release, at which point this duplicate is removed.
        #
        # SDK version, deployment id, and VCS attributes are folded into the
        # Resource so they flow with every signal (logs + metrics) without
        # being repeated on every per-call attribute set. This keeps
        # `service.function.duration` data points lean.
        resource_attrs = {
            "service.name": self.config.service_name,
            "aws.local.service": self.config.service_name,
        }
        # Only set the deployment.environment.name resource attribute when environment is
        # known — omit it entirely (no sentinel) when unset.
        if self.config.environment:
            resource_attrs["deployment.environment.name"] = self.config.environment
        version = self.config.sdk_version
        if version:
            resource_attrs["aws.service_events.version"] = version
        deployment_id = os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID", "")
        if deployment_id:
            resource_attrs["aws.service_events.deployment.id"] = deployment_id
        git_commit_sha = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA", "")
        if git_commit_sha:
            resource_attrs["vcs.ref.head.revision"] = git_commit_sha
        git_repo_url = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL", "")
        if git_repo_url:
            resource_attrs["vcs.repository.url.full"] = git_repo_url
        # Add platform attributes from OTel Resource detectors
        if self.config.resource_attributes:
            ra = self.config.resource_attributes
            for attr_name, attr_key in [
                ("cloud_provider", "cloud.provider"),
                ("cloud_platform", "cloud.platform"),
                ("cloud_region", "cloud.region"),
                ("cloud_account_id", "cloud.account.id"),
                ("cloud_availability_zone", "cloud.availability_zone"),
                ("host_id", "host.id"),
                ("host_type", "host.type"),
                ("container_id", "container.id"),
            ]:
                val = getattr(ra, attr_name, None)
                if val:
                    resource_attrs[attr_key] = val
        resource = Resource.create(resource_attrs)

        # Dedicated LoggerProvider for ServiceEvents OTLP signals.
        # When `output_file` is set, use the CloudWatch-faithful file exporter
        # in place of the OTLP HTTP exporter — logs and metrics all land in the same file.
        output_file = self.config.output_file
        logs_endpoint = self.config.logs_endpoint
        log_headers = {}
        if self.config.log_group:
            log_headers["x-aws-log-group"] = self.config.log_group
        log_stream = self.config.log_stream or self.config.service_name
        if log_stream:
            log_headers["x-aws-log-stream"] = log_stream

        if output_file:
            log_exporter = ServiceEventsCloudWatchLogFileExporter(output_file)
        else:
            log_exporter = _build_log_otlp_exporter(logs_endpoint, log_headers, Compression.NoCompression)
        self._otlp_logger_provider = LoggerProvider(resource=resource)
        self._otlp_logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

        # Dedicated MeterProvider for ServiceEvents metrics (count + service.function.duration).
        # File mode: write OTLP metrics JSON to the same file as logs. Network mode: POST OTLP metrics.
        # Both paths use Delta temporality so the file mirror matches the wire.
        # metrics_endpoint is guaranteed non-empty by the endpoint policy enforced
        # before ServiceEventsConfig reaches this code path.
        metrics_endpoint = self.config.metrics_endpoint
        if output_file:
            metric_exporter = ServiceEventsCloudWatchMetricFileExporter(output_file)
        else:
            metric_exporter = OTLPMetricExporter(
                endpoint=metrics_endpoint,
                preferred_temporality={
                    Counter: AggregationTemporality.DELTA,
                    Histogram: AggregationTemporality.DELTA,
                },
            )
        # Honor OTEL_METRIC_EXPORT_INTERVAL when set (defaults to 60s in the
        # OTel SDK). Lets contract tests force a fast flush without code change,
        # and operators tune flush cadence per environment.
        reader = PeriodicExportingMetricReader(metric_exporter)

        # Configure ExponentialBucketHistogramAggregation for function call duration metric
        # pylint: disable=import-outside-toplevel
        from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View

        function_duration_view = View(
            instrument_name="service.function.duration",
            aggregation=ExponentialBucketHistogramAggregation(),
        )
        self._otlp_meter_provider = MeterProvider(
            resource=resource, metric_readers=[reader], views=[function_duration_view]
        )

        # Deployment context from env
        deployment_id = os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID", "")
        git_commit_sha = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA", "")
        git_repo_url = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL", "")

        emitter = ServiceEventsOtlpEmitter(
            logger_provider=self._otlp_logger_provider,
            meter_provider=self._otlp_meter_provider,
            deployment_id=deployment_id,
            git_commit_sha=git_commit_sha,
            git_repo_url=git_repo_url,
        )
        self._otlp_emitter = emitter

        if output_file:
            logger.info("ServiceEvents OTLP emitter initialized (output_file: %s)", output_file)
        else:
            logger.info(
                "ServiceEvents OTLP emitter initialized (logs: %s, metrics: %s)",
                logs_endpoint,
                metrics_endpoint or "disabled",
            )
        return emitter

    def _reinitialize_after_fork(self) -> None:
        """
        Re-initialize ServiceEvents collector threads after fork.

        Called automatically in child processes (e.g., gunicorn workers) via
        os.register_at_fork. Daemon threads do not survive fork(), so collectors
        must be restarted in each worker for data to be collected and flushed.
        """
        try:
            # Lazy import to avoid import-time coupling with python_monitor.
            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.serviceevents.python_monitor import reset_after_fork

            reset_after_fork()

            # Note: reset_after_fork() preserves the monitor-state singleton's
            # identity (clearing only its mutable state), so:
            #   - self.monitor_state stays valid — no need to re-fetch.
            #   - The OTel histogram wiring set in initialize() is preserved,
            #     so post-fork __exit__ calls record into the histogram with no
            #     race against re-wiring.
            #   - Collectors and in-flight monitor objects that cached
            #     _ServiceEventsMonitorState.get_instance() still point at the
            #     correct (now-cleared) singleton.

            # Reset and restart each collector thread
            for collector in self.collectors:
                collector._reset_for_fork()
                collector.start()

            logger.info("ServiceEvents re-initialized after fork in worker process (pid=%d)", os.getpid())
        except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
            logger.error("Failed to re-initialize ServiceEvents after fork: %s", exc, exc_info=True)

    def shutdown(self) -> None:
        """
        Stop all collectors and cleanup resources.

        This should be called during application shutdown to ensure proper cleanup.
        """
        if not self._initialized:
            return

        # An explicit shutdown removes the atexit hook so it doesn't run a second time at
        # interpreter exit (the second run would be a guarded no-op via _initialized, but
        # unregistering also avoids emitting logs after the logging subsystem may have torn
        # down its streams). Safe if the hook was never registered.
        if self._atexit_registered:
            try:
                atexit.unregister(self.shutdown)
            except Exception:  # pylint: disable=broad-exception-caught  # never fail teardown
                pass
            self._atexit_registered = False

        try:
            logger.info("Shutting down ServiceEvents instrumentation")

            # Stop all collectors (this will also close file exporter if present)
            for collector in self.collectors:
                try:
                    collector.stop()
                    logger.debug("Stopped collector: %s", collector.__class__.__name__)
                except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
                    logger.error("Error stopping collector: %s", exc, exc_info=True)

            # Shutdown OTLP emitter and providers
            if self._otlp_emitter:
                try:
                    self._otlp_emitter.shutdown()
                    logger.debug("Shut down OTLP emitter")
                except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
                    logger.error("Error shutting down OTLP emitter: %s", exc, exc_info=True)

            self._initialized = False
            logger.info("ServiceEvents instrumentation shut down successfully")

        except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must not crash app
            logger.error("Error during ServiceEvents shutdown: %s", exc, exc_info=True)
