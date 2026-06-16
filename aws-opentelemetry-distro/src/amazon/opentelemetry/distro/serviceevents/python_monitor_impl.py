# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Pure Python implementation of ServiceEvents function monitoring.

This module contains the pure Python fallback implementation.
Do NOT import from this module directly - use python_monitor.py instead
which handles native/Python implementation selection.
"""

import threading
import time
import traceback
from contextvars import ContextVar
from typing import Dict, Optional

from amazon.opentelemetry.distro.serviceevents.ast_transformation import get_function_info_unlocked

# Hard cap on real call-path frames retained per request. The monitor records a frame for every
# instrumented call (not just sampled ones), so under "auto"/"never" sampling a high-call-volume
# request would otherwise grow this list without bound and could push a serialized IncidentSnapshot
# past the 1 MB CloudWatch OTLP Logs limit. At ~150-250 B/frame, 1024 frames is ~256 KB — well under
# the limit with room for the stack trace, yet far above any legitimate frame count. Java/JS match.
_MAX_CALL_PATH_ENTRIES = 1024
# Marker appended once when the cap is exceeded. duration_ns=0 also trips is_partial (computed from
# timing in the incident collector).
_CALL_PATH_TRUNCATION_SENTINEL = "<call_path_truncated>"

# ============================================================================
# Sampling Configuration
# ============================================================================

_sample_tier1_threshold: int = 100
_sample_tier2_threshold: int = 1000
_sample_tier2_rate: int = 10
_sample_tier3_rate: int = 100

_sampling_mode: str = "always"  # "auto", "always", or "never"
_call_counters: Dict[str, int] = {}
_call_counters_lock = threading.Lock()

# Process-wide count of contexts with an active investigation. The monitor records a call_path
# entry on EVERY instrumented function exit (not just sampled ones), which would otherwise pay a
# ContextVar lookup on the hot path even when no incident investigation is in flight (the common
# case). This counter lets record_call_path_entry skip that lookup when nothing is investigating —
# mirroring the JS distro's _investigationActiveCount and the Java bridge's investigationActiveCount.
# Reads on the hot path are lock-free (a plain int load is atomic under CPython); the infrequent
# begin/clear writes take the lock to avoid lost updates. The counter is global (shared across
# threads/contexts) while the investigation data itself is a per-context ContextVar, exactly like
# JS's global counter + per-context AsyncLocalStorage: when ANY context is investigating, the hot
# path does the ContextVar lookup and finds data only in the contexts that began one.
_investigation_active_count: int = 0
_investigation_active_lock = threading.Lock()


def _increment_investigation_active() -> None:
    """Increment the active-investigation count. Called only when begin_investigation actually
    creates new data (so a double-begin in one context doesn't leak the count high)."""
    global _investigation_active_count  # pylint: disable=global-statement
    with _investigation_active_lock:
        _investigation_active_count += 1


def _decrement_investigation_active() -> None:
    """Decrement the active-investigation count, clamped at zero. Clamping keeps an unbalanced clear
    (e.g. a clear with no prior begin) from driving the count negative — a spuriously-high count
    only costs an extra hot-path ContextVar lookup (degrades to pre-guard behavior), whereas a
    negative count would wrongly disable call_path capture for a genuinely active investigation."""
    global _investigation_active_count  # pylint: disable=global-statement
    with _investigation_active_lock:
        if _investigation_active_count > 0:
            _investigation_active_count -= 1


def set_sampling_mode(mode: str) -> None:
    """Set sampling mode: 'always', 'never', or 'auto'."""
    # Singleton module-level sampling state.
    global _sampling_mode  # pylint: disable=global-statement
    if mode not in ("always", "never", "auto"):
        raise ValueError(f"Invalid sampling mode: '{mode}'")
    _sampling_mode = mode


def get_sampling_mode() -> str:
    """Get current sampling mode."""
    return _sampling_mode


def set_sampling_thresholds(
    tier1_threshold: int = 100,
    tier2_threshold: int = 1000,
    tier2_rate: int = 10,
    tier3_rate: int = 100,
) -> None:
    """Set sampling thresholds for auto mode."""
    # Singleton module-level sampling state.
    global _sample_tier1_threshold, _sample_tier2_threshold  # pylint: disable=global-statement
    global _sample_tier2_rate, _sample_tier3_rate  # pylint: disable=global-statement
    _sample_tier1_threshold = tier1_threshold
    _sample_tier2_threshold = tier2_threshold
    _sample_tier2_rate = tier2_rate
    _sample_tier3_rate = tier3_rate


def _should_sample(total_calls: int) -> bool:  # pylint: disable=too-many-return-statements
    """Determine if a call should be sampled based on current mode and call count."""
    if _sampling_mode == "always":
        return True
    if _sampling_mode == "never":
        return False
    # AUTO: 3-tier sampling. Rates are "1-in-N"; a non-positive N is degenerate
    # (it can only arrive via the internal test-config hook, which doesn't validate) and
    # would otherwise raise ZeroDivisionError on the modulo. Treat N <= 0 as "sample none
    # in this tier" so a misconfigured rate degrades gracefully instead of crashing the
    # guarded monitor __enter__ on every call.
    if total_calls <= _sample_tier1_threshold:
        return True
    if total_calls <= _sample_tier2_threshold:
        return _sample_tier2_rate > 0 and total_calls % _sample_tier2_rate == 0
    return _sample_tier3_rate > 0 and total_calls % _sample_tier3_rate == 0


def _increment_call_counter(function_name: str) -> int:
    """Increment and return the call counter for a function."""
    with _call_counters_lock:
        _call_counters[function_name] = _call_counters.get(function_name, 0) + 1
        return _call_counters[function_name]


# ============================================================================
# Context Variables
# ============================================================================

# Thread-local call stack for tracking caller relationships
_call_stack: ContextVar[list] = ContextVar("serviceevents_call_stack", default=[])

# Thread-local operation for associating functions with HTTP endpoints
_current_operation: ContextVar[Optional[str]] = ContextVar("serviceevents_operation", default=None)


# ============================================================================
# Operation Functions
# ============================================================================


def set_current_operation(operation: str):
    """Set the current operation (e.g., 'POST /users') for the request context."""
    _current_operation.set(operation)


def get_current_operation() -> Optional[str]:
    """Get the current operation from the request context."""
    return _current_operation.get()


def clear_current_operation():
    """Clear the current operation from the request context."""
    _current_operation.set(None)


def get_call_stack() -> list:
    """Get the current call stack as a list of function names."""
    stack = _call_stack.get()
    if stack is None:
        return []
    return list(stack)


def reset_after_fork() -> None:
    """Reset all module state after fork. Important for multiprocessing.

    The singleton's identity is preserved — only mutable state is cleared.
    This avoids two hazards inherent to "null and recreate" semantics:

    - A publication race where a freshly-recreated singleton is briefly visible
      without its OTel instruments wired, causing a few post-fork calls to
      silently no-op and miss the metric.
    - Stale caches: collectors and PythonServiceEventsMonitor instances cache
      `_ServiceEventsMonitorState.get_instance()` at construction time. If we
      replaced the singleton, those caches would point at a discarded object
      and writes/reads would diverge from what `_reinitialize_after_fork`
      re-wires.
    """
    # Singleton module-level sampling state.
    global _sampling_mode, _investigation_active_count  # pylint: disable=global-statement

    # Reset sampling mode to default (always)
    _sampling_mode = "always"

    # The child starts with no in-flight investigations; the singleton's investigation_data is
    # cleared below, so the active count must drop to zero to match (avoids a leaked count
    # forcing the post-fork hot path to keep doing ContextVar lookups for nothing).
    with _investigation_active_lock:
        _investigation_active_count = 0

    # Clear call counters
    with _call_counters_lock:
        _call_counters.clear()

    # Clear thread-local state
    _call_stack.set([])
    _current_operation.set(None)

    # Clear the existing singleton's mutable state in place. The OTel
    # instrument (histogram) and its base attrs are intentionally left intact —
    # the histogram is owned by the parent's MeterProvider, which survives fork,
    # and re-wiring it would only widen the un-wired window for no benefit. If
    # the parent never wired it, the field stays None and recording simply
    # no-ops, as before.
    with _ServiceEventsMonitorState._lock:
        inst = _ServiceEventsMonitorState._instance
        if inst is not None:
            inst._investigation_data.set(None)


# ============================================================================
# ServiceEventsMonitorState Class
# ============================================================================


class _ServiceEventsMonitorState:
    """
    Singleton class that holds global state for all monitored functions.
    This is separate from the context manager to maintain a single metric
    recording path and investigation store.
    """

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        # For investigation capture
        self._investigation_data: ContextVar[Optional[dict]] = ContextVar("investigation_data", default=None)

        # Direct OTel histogram recording for function-call durations.
        self._function_duration_histogram = None
        # Shared attributes for the duration Histogram. Stored as a plain dict
        # (write-once during init, never mutated) so __exit__ can build per-call
        # attrs by copying this dict and adding per-call keys.
        self._metric_base_attrs: Dict = {}

    @classmethod
    def get_instance(cls):
        """Get or create the singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def set_metric_base_attrs(self, base_attrs: Dict) -> None:
        """
        Store the shared attribute set used by the function-duration metric.

        ``service.function.duration`` (Histogram) builds its per-call attribute
        dict on top of these base attrs. Should be called before the instrument
        is wired so the first recorded call sees a fully-populated attribute set.

        Args:
            base_attrs: Signal-level attributes (e.g. ``Telemetry.Source``).
                Service identity, deployment, and VCS metadata live on the OTel
                Resource and ride along with every data point automatically, so
                they are not part of this dict. Snapshotted into a plain dict so
                external mutation can't poison readers; never mutated after.
        """
        self._metric_base_attrs = dict(base_attrs)

    def set_function_duration_histogram(self, histogram) -> None:
        """
        Wire the OTel histogram instrument for direct recording at call time.

        Called once during ServiceEventsInstrumentation initialization after the
        MeterProvider and histogram are created. Enables PythonServiceEventsMonitor
        to record raw durations directly into the OTel Exponential Histogram.

        Args:
            histogram: OTel Histogram instrument (service.function.duration)
        """
        self._function_duration_histogram = histogram

    def record_function_call_metrics(
        self,
        function_name: str,
        duration_ns: int,
        caller: Optional[str] = None,
        exception_name: Optional[str] = None,
        is_sampled: bool = True,
    ) -> None:
        """
        Record a function call into the OTel metrics pipeline, if the histogram
        is wired.

        ``service.function.duration`` (Histogram) is recorded only when
        ``is_sampled=True`` so latency stats (sum, min, percentiles) aren't
        polluted by zero-duration placeholders. The per-call attribute dict is
        built on top of ``_metric_base_attrs``, populated separately via
        ``set_metric_base_attrs``.

        No-ops when the histogram is not wired (no OTLP emitter configured).

        Args:
            function_name: Composite function name (e.g., "myapp/server.my_func")
            duration_ns: Measured duration in nanoseconds (only used when sampled)
            caller: Calling function name (None if entry point)
            exception_name: Exception class name if the call raised, else None
            is_sampled: Whether timing was actually captured for this call
        """
        histogram = self._function_duration_histogram
        if histogram is None:
            return

        # Latency histogram: sampled calls only. Non-sampled calls would record
        # duration_ns=0, polluting sum/min/percentiles, so we skip them entirely.
        if not is_sampled:
            return

        # Copy the write-once base dict and add per-call keys directly.
        attrs = self._metric_base_attrs.copy()
        attrs["function.name"] = function_name
        if caller:
            attrs["aws.service_events.caller"] = caller

        # Lock-free best-effort lookup of function metadata.
        # Safe because the registry is write-once (populated at import time)
        # and dict.get() is protected by CPython's internal dict locking on
        # both GIL and free-threaded (PEP 703) builds.
        func_info = get_function_info_unlocked(function_name)
        if func_info:
            line = func_info.get("line")
            if line is not None:
                attrs["aws.service_events.function_at_line"] = line
            if func_info.get("is_async"):
                attrs["aws.service_events.async"] = True

        if exception_name:
            attrs["status"] = "error"
        else:
            attrs["status"] = "success"

        duration_us = duration_ns / 1000.0
        histogram.record(duration_us, attrs)

    def begin_investigation(self):
        """Start capturing investigation data for current request."""
        # Increment the active count only when we actually create new data, so a re-entrant
        # begin in the same context (the analogue of Java's nested servlet dispatch) doesn't
        # leak the count high. Mirrors the Java bridge's create-only increment.
        if self._investigation_data.get() is None:
            _increment_investigation_active()
        self._investigation_data.set({"call_path": [], "exception": None, "start_time": time.time()})

    def get_investigation_data(self) -> Optional[dict]:
        """Get and clear investigation data."""
        data = self._investigation_data.get()
        self._investigation_data.set(None)
        if data is not None:
            _decrement_investigation_active()
        return data

    def peek_investigation_data(self) -> Optional[dict]:
        """Peek at investigation data WITHOUT clearing it."""
        return self._investigation_data.get()

    def clear_investigation_data(self) -> None:
        """Drop any investigation data captured for the current request.

        begin_investigation() seeds a dict at the start of every request, but only the
        incident path consumes (and clears) it via get_investigation_data(). On the normal
        path process_potential_incident returns early, so without this the dict — including
        a captured traceback string on error paths — would linger in the ContextVar on
        pooled worker threads until the next request on that thread overwrites it. Each
        framework calls this unconditionally in its request-finalize path. Idempotent: a
        no-op when already cleared (e.g. an incident was collected this request).
        """
        # Decrement only when data was actually present, so the unconditional finalize-path call
        # (and the incident path, which already consumed via get_investigation_data) each net the
        # count correctly — every begin pairs with exactly one decrement.
        if self._investigation_data.get() is not None:
            _decrement_investigation_active()
        self._investigation_data.set(None)

    def record_execution_flow(self, caller: Optional[str], callee: str):
        """
        Record a function call edge for investigation.

        DEPRECATED: Use record_call_path_entry() for richer timing data.
        Kept for backward compatibility.
        """
        inv_data = self._investigation_data.get()
        if inv_data is not None:
            inv_data["call_path"].append((caller, callee))

    def record_call_path_entry(self, function_name: str, caller: Optional[str], duration_ns: int):
        """
        Record a function call with timing information for investigation.

        Args:
            function_name: Composite function name (e.g., "module/path.func")
            caller: Function name that called this one (None if entry point)
            duration_ns: Duration in nanoseconds
        """
        # Hot-path gate: skip the ContextVar lookup entirely when no investigation is in flight
        # anywhere (the common case). Mirrors the JS/Java active-count guards. A lock-free int read
        # is enough — a stale 0 only happens in the instant before begin_investigation's locked
        # increment publishes, which is the same context that hasn't recorded any frames yet.
        if _investigation_active_count == 0:
            return
        inv_data = self._investigation_data.get()
        if inv_data is None:
            return
        call_path = inv_data["call_path"]
        size = len(call_path)
        if size > _MAX_CALL_PATH_ENTRIES:
            # Already at [MAX real frames + sentinel] — drop everything after.
            return
        if size == _MAX_CALL_PATH_ENTRIES:
            # This frame overflows the cap: keep the first MAX, then a single truncation sentinel.
            call_path.append(
                {
                    "function_name": _CALL_PATH_TRUNCATION_SENTINEL,
                    "caller_function_name": None,
                    "duration_ns": 0,
                }
            )
            return
        call_path.append(
            {
                "function_name": function_name,
                "caller_function_name": caller,
                "duration_ns": duration_ns,
            }
        )


# ============================================================================
# PythonServiceEventsMonitor Class
# ============================================================================


class PythonServiceEventsMonitor:
    """
    Context manager for monitoring individual function invocations.

    This is instantiated for each function call via AST transformation:
        with PythonServiceEventsMonitor("module/path.my_function"):
            # function body

    Usage:
        def my_function():
            with PythonServiceEventsMonitor("myapp/server.my_function"):
                # original function body
                pass
    """

    def __init__(self, function_name: str):
        """
        Initialize the monitor for a specific function invocation.

        Args:
            function_name: Composite function name (e.g., "myapp/server.my_func")
        """
        self.function_name = function_name
        self.start_time = None
        self.caller = None
        self.is_sampled = False
        # Whether __enter__ actually pushed this frame onto _call_stack. __exit__
        # pops only when this is True, so a failed/partial __enter__ never causes
        # __exit__ to pop a frame it didn't push (which would corrupt caller
        # attribution for sibling/parent calls).
        self._pushed = False
        self._state = _ServiceEventsMonitorState.get_instance()

    def __enter__(self):
        """Called when entering the context manager.

        Crash-safety invariant: this wraps the entire body of every instrumented
        customer function, so it must never raise into customer code. All
        telemetry setup is guarded; on failure we mark the call un-sampled and
        still return self so the customer body runs and __exit__ no-ops cleanly.
        The ``return self`` is outside the try so control is guaranteed.
        """
        try:
            # The per-function call counter only drives AUTO-mode tiered sampling. Only AUTO
            # reads it, so for every other mode (including the default "always") skip the
            # lock acquisition and the unbounded counter-dict growth on this hot path.
            if _sampling_mode == "auto":
                call_count = _increment_call_counter(self.function_name)
            else:
                call_count = 0
            self.is_sampled = _should_sample(call_count)

            # Only record perf_counter for sampled calls (used for duration calculation)
            if self.is_sampled:
                self.start_time = time.perf_counter_ns()

            # Get current call stack from context var
            stack = _call_stack.get()
            if stack is None:
                stack = []

            # Determine caller (last item on stack)
            self.caller = stack[-1] if stack else None

            # Push current function to stack
            new_stack = stack + [self.function_name]  # Create new list to avoid mutation issues
            _call_stack.set(new_stack)
            self._pushed = True  # Set last: __exit__ pops iff the push succeeded.
        except Exception:  # pylint: disable=broad-exception-caught
            # Telemetry must never crash the customer app; swallow all errors.
            # Telemetry setup failed — disable timing/recording for this call so
            # the (also-guarded) __exit__ stays a no-op. Never propagate.
            self.is_sampled = False
            self.start_time = None

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Called when exiting the context manager.

        Crash-safety invariant: telemetry must never alter the customer's control
        flow. We do NOT touch the customer's exception — when the body raises,
        Python passes it in as exc_* args and re-raises it after this returns
        False; it is never inside our try, so it cannot be caught or changed. We
        only swallow exceptions raised by our own telemetry code, and only
        ``Exception`` (not ``BaseException``): KeyboardInterrupt/SystemExit/
        GeneratorExit are not ours to suppress. ``return False`` is outside the
        try so the customer's exception always propagates untouched.
        """
        try:
            # Calculate duration only for sampled calls
            duration_ns = 0
            if self.is_sampled:
                end_time = time.perf_counter_ns()
                duration_ns = end_time - self.start_time

            # Record call path entry for investigations (with timing data)
            # For non-sampled calls, duration_ns will be 0
            self._state.record_call_path_entry(
                function_name=self.function_name, caller=self.caller, duration_ns=duration_ns
            )

            # Record exception if any
            exception_name = None
            if exc_type is not None:
                exception_name = exc_type.__name__

                # Format the traceback to a string eagerly rather than stashing the
                # (exc_type, exc_value, exc_traceback) tuple: holding exc_traceback pins the
                # entire frame chain (every local in every frame) alive in the ContextVar until
                # the next request overwrites the investigation data. On the common non-incident
                # path that window can be arbitrarily long under low request rates. str()/
                # format_exception run the customer's exception __str__ — guarded by the
                # surrounding try so a misbehaving __str__ can't escape here.
                try:
                    stack_trace = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                except Exception:  # pylint: disable=broad-exception-caught
                    stack_trace = f"{exception_name}: {exc_value}"

                # First-writer-wins: __exit__ unwinds innermost-first, so the first frame to
                # observe a propagating exception is the one closest to the raise (the true
                # origin). Outer frames re-observe the same exception as it propagates out; they
                # must NOT overwrite the origin with their own (outer) function_name — otherwise
                # the recorded thrower is always the outermost instrumented frame.
                inv_data = self._state._investigation_data.get()
                if inv_data is not None and inv_data.get("exception") is None:
                    inv_data["exception"] = {
                        "name": exception_name,
                        "message": str(exc_value),
                        "traceback_info": stack_trace,
                        "function_name": self.function_name,  # Capture which function threw the exception
                    }

            # Record the call duration into the OTel histogram (sampled calls only).
            self._state.record_function_call_metrics(
                function_name=self.function_name,
                duration_ns=duration_ns,
                caller=self.caller,
                exception_name=exception_name,
                is_sampled=self.is_sampled,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            # Telemetry must never crash the customer app; swallow all errors.
            # Swallow only exceptions raised by our own telemetry code above.
            # The customer's in-flight exception is not in this try (Python
            # re-raises it after return), so it is unaffected.
            pass
        finally:
            # Always balance the stack: pop iff __enter__ pushed this frame, and
            # do it in finally so a failure in the recording calls above can't
            # skip the pop (which would leak this frame) — keeping caller
            # attribution correct for subsequent calls on this thread/context.
            if self._pushed:
                try:
                    stack = _call_stack.get()
                    if stack:
                        _call_stack.set(stack[:-1])  # Remove last element
                except Exception:  # pylint: disable=broad-exception-caught
                    # Telemetry must never crash the customer app; swallow all errors.
                    pass

        return False

    @classmethod
    def get_instance(cls):
        """Get the global monitor state (for collectors)."""
        return _ServiceEventsMonitorState.get_instance()
