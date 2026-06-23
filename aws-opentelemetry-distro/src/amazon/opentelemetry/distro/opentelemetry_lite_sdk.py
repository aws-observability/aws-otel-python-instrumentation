# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=too-many-public-methods,no-self-use,import-outside-toplevel,broad-exception-caught,too-many-locals
import base64
import logging
import os
import random
import socket
import struct
import traceback
from time import time_ns
from typing import Iterator, List, Optional, Sequence
from urllib.parse import urlparse

from opentelemetry import context as context_api
from opentelemetry import trace as trace_api
from opentelemetry import version as otel_version
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util import types
from opentelemetry.util._decorator import _agnosticcontextmanager
from opentelemetry.util.types import Attributes

logger = logging.getLogger(__name__)

_PROTOCOL_HEADER = '{"format":"json","version":1}\n'
_DEFAULT_ENDPOINT = "127.0.0.1:2000"
_EXCEPTION_TYPE = "exception.type"
_EXCEPTION_MESSAGE = "exception.message"
_EXCEPTION_STACKTRACE = "exception.stacktrace"
_EXCEPTION_ESCAPED = "exception.escaped"


def _build_lambda_resource():
    attrs = {}
    raw = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
    for pair in raw.split(","):
        if "=" in pair:
            key, val = pair.split("=", 1)
            attrs[key.strip()] = val.strip()
    otel_service_name = os.environ.get("OTEL_SERVICE_NAME", "")
    if otel_service_name:
        attrs["service.name"] = otel_service_name

    attrs["telemetry.sdk.language"] = "python"
    attrs["telemetry.sdk.name"] = "opentelemetry"
    attrs["telemetry.sdk.version"] = otel_version.__version__
    return attrs


class InstrumentationScope:
    __slots__ = ("_name", "_version", "_schema_url")

    def __init__(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> None:
        self._name = name
        self._version = version
        self._schema_url = schema_url or ""
        # attributes: accepted for API compatibility with TracerProvider.get_tracer() but not stored or encoded.

    @property
    def schema_url(self) -> Optional[str]:
        return self._schema_url

    @property
    def version(self) -> Optional[str]:
        return self._version

    @property
    def name(self) -> str:
        return self._name


class SpanProcessor:
    def on_start(self, span: "Span", parent_context: Optional[context_api.Context] = None) -> None:
        pass

    def on_end(self, span: "Span") -> None:
        pass

    def shutdown(self, timeout_millis: int = 30000) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


class Event:
    def __init__(self, name: str, attributes: types.Attributes = None, timestamp: Optional[int] = None) -> None:
        self._name = name
        self._timestamp = timestamp if timestamp is not None else time_ns()
        self._attributes = attributes

    @property
    def name(self) -> str:
        return self._name

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def attributes(self) -> types.Attributes:
        return self._attributes


class TracerProvider(trace_api.TracerProvider):

    def __init__(self, resource: Optional[dict] = None) -> None:
        self._span_processors: List[SpanProcessor] = []
        self._resource = resource if resource is not None else _build_lambda_resource()

    @property
    def resource(self) -> dict:
        return self._resource

    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> "trace_api.Tracer":
        if not instrumenting_module_name:
            instrumenting_module_name = ""
        return Tracer(
            self.resource,
            self,
            InstrumentationScope(
                instrumenting_module_name, instrumenting_library_version or "", schema_url, attributes
            ),
        )

    def add_span_processor(self, span_processor: SpanProcessor) -> None:
        self._span_processors.append(span_processor)

    def on_start(self, span, parent_context=None) -> None:
        for sp in self._span_processors:
            sp.on_start(span, parent_context=parent_context)

    def on_end(self, span) -> None:
        for sp in self._span_processors:
            sp.on_end(span)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        for sp in self._span_processors:
            sp.force_flush(timeout_millis)
        return True

    def shutdown(self) -> None:
        for sp in self._span_processors:
            sp.shutdown()


class Tracer(trace_api.Tracer):

    def __init__(self, resource: dict, provider: TracerProvider, instrumentation_scope: InstrumentationScope) -> None:
        self.resource = resource
        self._provider = provider
        self._instrumentation_scope = instrumentation_scope

    @_agnosticcontextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Optional[context_api.Context] = None,
        kind: trace_api.SpanKind = trace_api.SpanKind.INTERNAL,
        attributes: Attributes = None,
        links: Optional[Sequence[trace_api.Link]] = (),
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> Iterator[trace_api.Span]:
        span = self.start_span(
            name=name,
            context=context,
            kind=kind,
            attributes=attributes,
            links=links,
            start_time=start_time,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        )
        with trace_api.use_span(  # pylint: disable=not-context-manager
            span,
            end_on_exit=end_on_exit,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        ) as span:
            yield span

    def start_span(
        self,
        name: str,
        context: Optional[context_api.Context] = None,
        kind: trace_api.SpanKind = trace_api.SpanKind.INTERNAL,
        attributes: Attributes = None,
        links: Optional[Sequence[trace_api.Link]] = (),
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> trace_api.Span:
        parent_span_context = trace_api.get_current_span(context).get_span_context()

        if parent_span_context is None or not parent_span_context.is_valid:
            parent_span_context = None
            trace_id = random.getrandbits(128)
            trace_flags = trace_api.TraceFlags(trace_api.TraceFlags.SAMPLED)
        else:
            trace_id = parent_span_context.trace_id
            trace_flags = parent_span_context.trace_flags

        trace_state = parent_span_context.trace_state if parent_span_context else None
        span_context = trace_api.SpanContext(
            trace_id, random.getrandbits(64), is_remote=False, trace_flags=trace_flags, trace_state=trace_state
        )

        span = Span(
            name=name,
            context=span_context,
            parent=parent_span_context,
            resource=self.resource,
            attributes=attributes,
            span_processor=self._provider,
            kind=kind,
            links=links,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
            instrumentation_scope=self._instrumentation_scope,
        )
        span.start(start_time=start_time, parent_context=context)
        return span


class Span(trace_api.Span):

    # links: accepted for API compatibility but not stored (Lambda spans never use them).
    def __init__(
        self,
        name: str,
        context: trace_api.SpanContext,
        parent: Optional[trace_api.SpanContext] = None,
        resource: Optional[dict] = None,
        attributes: types.Attributes = None,
        events: Optional[Sequence[Event]] = None,
        links: Sequence[trace_api.Link] = (),
        kind: trace_api.SpanKind = trace_api.SpanKind.INTERNAL,
        span_processor=None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        instrumentation_scope: Optional[InstrumentationScope] = None,
    ) -> None:
        self._name = name
        self._context = context
        self._parent = parent
        self._kind = kind
        self._resource = resource or {}
        self._instrumentation_scope = instrumentation_scope
        self._record_exception = record_exception
        self._set_status_on_exception = set_status_on_exception
        self._span_processor = span_processor or SpanProcessor()
        self._start_time = None
        self._end_time = None
        self._status = Status(StatusCode.UNSET)
        self._attributes = dict(attributes) if attributes else {}
        self._events = list(events) if events else []

    def get_span_context(self):
        return self._context

    @property
    def context(self):
        return self._context

    @property
    def kind(self):
        return self._kind

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @property
    def instrumentation_scope(self):
        return self._instrumentation_scope

    @property
    def attributes(self):
        return self._attributes

    @property
    def resource(self):
        return self._resource

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def status(self):
        return self._status

    @property
    def events(self):
        return self._events

    @property
    def links(self):
        return ()

    def set_attribute(self, key, value):
        if self._end_time is not None:
            return
        self._attributes[key] = value

    def set_attributes(self, attributes):
        if self._end_time is not None:
            return
        for key, value in attributes.items():
            self._attributes[key] = value

    def add_event(self, name, attributes=None, timestamp=None):
        if self._end_time is not None:
            return
        self._events.append(Event(name=name, attributes=attributes, timestamp=timestamp))

    def update_name(self, name):
        if self._end_time is not None:
            return
        self._name = name

    def is_recording(self):
        return self._end_time is None

    def set_status(self, status, description=None):
        if self._end_time is not None:
            return
        if isinstance(status, Status):
            if self._status.status_code is StatusCode.OK:
                return
            if status.status_code is StatusCode.UNSET:
                return
            self._status = status
        elif isinstance(status, StatusCode):
            if self._status.status_code is StatusCode.OK:
                return
            if status is StatusCode.UNSET:
                return
            self._status = Status(status, description)

    def start(self, start_time=None, parent_context=None):
        if self._start_time is not None:
            return
        self._start_time = start_time if start_time is not None else time_ns()
        self._span_processor.on_start(self, parent_context=parent_context)

    def end(self, end_time=None):
        if self._start_time is None:
            raise RuntimeError("Calling end() on a not started span.")
        if self._end_time is not None:
            return
        self._end_time = end_time if end_time is not None else time_ns()
        self._span_processor.on_end(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None and self.is_recording():
            if self._record_exception:
                self.record_exception(exception=exc_val, escaped=True)
            if self._set_status_on_exception:
                self.set_status(Status(status_code=StatusCode.ERROR, description=f"{exc_type.__name__}: {exc_val}"))
        super().__exit__(exc_type, exc_val, exc_tb)

    def record_exception(self, exception, attributes=None, timestamp=None, escaped=False):
        stacktrace = "".join(traceback.format_exception(type(exception), value=exception, tb=exception.__traceback__))
        module = type(exception).__module__
        qualname = type(exception).__qualname__
        exception_type = f"{module}.{qualname}" if module and module != "builtins" else qualname
        _attributes = {
            _EXCEPTION_TYPE: exception_type,
            _EXCEPTION_MESSAGE: str(exception),
            _EXCEPTION_STACKTRACE: stacktrace,
            _EXCEPTION_ESCAPED: str(escaped),
        }
        if attributes:
            _attributes.update(attributes)
        self.add_event(name="exception", attributes=_attributes, timestamp=timestamp)


def _resolve_remote_service(attributes):
    rpc_service = attributes.get("rpc.service")
    rpc_system = attributes.get("rpc.system")
    if rpc_service and rpc_system == "aws-api":
        return "AWS::" + rpc_service
    if rpc_service:
        return rpc_service
    http_url = attributes.get("http.url") or attributes.get("url.full") or ""
    if http_url:
        return urlparse(http_url).hostname or "UnknownRemoteService"
    return "UnknownRemoteService"


def _resolve_remote_operation(attributes):
    rpc_method = attributes.get("rpc.method")
    if rpc_method:
        return rpc_method
    http_method = attributes.get("http.method") or attributes.get("http.request.method") or ""
    http_url = attributes.get("http.url") or attributes.get("url.full") or ""
    if http_method and http_url:
        path = urlparse(http_url).path or "/"
        return f"{http_method} {path}"
    if http_method:
        return http_method
    return "UnknownRemoteOperation"


_VARINT = 0
_FIXED64 = 1
_LEN_DELIMITED = 2
_FIXED32 = 5


def _encode_varint(value):
    if value < 0:
        value &= 0xFFFFFFFFFFFFFFFF
    buf = b""
    while value > 0x7F:
        buf += bytes([(value & 0x7F) | 0x80])
        value >>= 7
    buf += bytes([value & 0x7F])
    return buf


def _encode_tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


def _encode_bytes_field(field_number, data):
    if not data:
        return b""
    return _encode_tag(field_number, _LEN_DELIMITED) + _encode_varint(len(data)) + data


def _encode_string_field(field_number, value):
    if not value:
        return b""
    return _encode_bytes_field(field_number, value.encode("utf-8"))


def _encode_varint_field(field_number, value):
    if value is None:
        return b""
    return _encode_tag(field_number, _VARINT) + _encode_varint(value)


def _encode_fixed64_field(field_number, value):
    if value is None:
        return b""
    return _encode_tag(field_number, _FIXED64) + struct.pack("<Q", value)


def _encode_fixed32_field(field_number, value):
    if value is None:
        return b""
    return _encode_tag(field_number, _FIXED32) + struct.pack("<I", value)


def _encode_any_value(value):
    if isinstance(value, bool):
        return _encode_tag(2, _VARINT) + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        return _encode_tag(3, _VARINT) + _encode_varint(value)
    if isinstance(value, float):
        return _encode_tag(4, _FIXED64) + struct.pack("<d", value)
    if isinstance(value, (list, tuple)):
        array_buf = b""
        for item in value:
            array_buf += _encode_bytes_field(1, _encode_any_value(item))
        return _encode_bytes_field(5, array_buf)
    return _encode_string_field(1, str(value))


def _encode_key_value(key, value):
    return _encode_string_field(1, key) + _encode_bytes_field(2, _encode_any_value(value))


def _encode_span_status(status):
    if status is None or status.status_code == StatusCode.UNSET:
        return b""
    buf = b""
    if status.description:
        buf += _encode_string_field(2, status.description)
    code_map = {StatusCode.UNSET: 0, StatusCode.OK: 1, StatusCode.ERROR: 2}
    buf += _encode_varint_field(3, code_map.get(status.status_code, 0))
    return buf


def _encode_span_event(event):
    buf = b""
    buf += _encode_fixed64_field(1, event.timestamp)
    buf += _encode_string_field(2, event.name)
    if event.attributes:
        for key, value in event.attributes.items():
            buf += _encode_bytes_field(3, _encode_key_value(key, value))
    return buf


def _span_kind_to_otlp(kind):
    return {
        SpanKind.INTERNAL: 1,
        SpanKind.SERVER: 2,
        SpanKind.CLIENT: 3,
        SpanKind.PRODUCER: 4,
        SpanKind.CONSUMER: 5,
    }.get(kind, 0)


def _encode_span_otlp(span):
    ctx = span.get_span_context()
    buf = b""
    buf += _encode_bytes_field(1, ctx.trace_id.to_bytes(16, "big"))
    buf += _encode_bytes_field(2, ctx.span_id.to_bytes(8, "big"))
    if ctx.trace_state:
        buf += _encode_string_field(3, str(ctx.trace_state))
    if span.parent:
        buf += _encode_bytes_field(4, span.parent.span_id.to_bytes(8, "big"))
    flags = int(ctx.trace_flags) | 0x100
    if span.parent and span.parent.is_remote:
        flags |= 0x200
    buf += _encode_fixed32_field(16, flags)
    buf += _encode_string_field(5, span.name)
    buf += _encode_varint_field(6, _span_kind_to_otlp(span.kind))
    buf += _encode_fixed64_field(7, span.start_time)
    buf += _encode_fixed64_field(8, span.end_time)
    if span.attributes:
        for key, value in span.attributes.items():
            buf += _encode_bytes_field(9, _encode_key_value(key, value))
    if span.events:
        for event in span.events:
            buf += _encode_bytes_field(11, _encode_span_event(event))
    status_bytes = _encode_span_status(span.status)
    if status_bytes:
        buf += _encode_bytes_field(15, status_bytes)
    return buf


def _encode_resource(resource_attrs):
    buf = b""
    if resource_attrs:
        for key, value in resource_attrs.items():
            buf += _encode_bytes_field(1, _encode_key_value(key, value))
    return buf


def _encode_instrumentation_scope(scope):
    buf = b""
    if scope:
        buf += _encode_string_field(1, scope.name)
        if scope.version:
            buf += _encode_string_field(2, scope.version)
    return buf


def _scope_key(scope):
    if scope is None:
        return ("", "")
    return (scope.name or "", scope.version or "")


def _resource_key(resource):
    if not resource:
        return ()
    return tuple(sorted(resource.items()))


def _encode_export_trace_request(spans):
    if not spans:
        return b""

    resource_groups = {}
    resource_for_key = {}
    for span in spans:
        r_key = _resource_key(span.resource)
        s_key = _scope_key(span.instrumentation_scope)
        if r_key not in resource_groups:
            resource_groups[r_key] = {}
            resource_for_key[r_key] = span.resource
        if s_key not in resource_groups[r_key]:
            resource_groups[r_key][s_key] = []
        resource_groups[r_key][s_key].append(span)

    buf = b""
    for r_key, scope_groups in resource_groups.items():
        resource_spans = b""
        resource_bytes = _encode_resource(resource_for_key[r_key])
        if resource_bytes:
            resource_spans += _encode_bytes_field(1, resource_bytes)

        for s_key, scope_spans_list in scope_groups.items():
            scope_spans = b""
            scope = scope_spans_list[0].instrumentation_scope
            scope_bytes = _encode_instrumentation_scope(scope)
            if scope_bytes:
                scope_spans += _encode_bytes_field(1, scope_bytes)
            for span in scope_spans_list:
                scope_spans += _encode_bytes_field(2, _encode_span_otlp(span))
            resource_spans += _encode_bytes_field(2, scope_spans)

        buf += _encode_bytes_field(1, resource_spans)
    return buf


class UdpExporter:
    def __init__(self, endpoint=None):
        self._endpoint = endpoint or _DEFAULT_ENDPOINT
        host, port = self._endpoint.rsplit(":", 1)
        self._host = host
        self._port = int(port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)

    def send_otlp(self, data, prefix="T1U"):
        encoded = base64.b64encode(data).decode("utf-8")
        message = f"{_PROTOCOL_HEADER}{prefix}{encoded}"
        try:
            self._socket.sendto(message.encode("utf-8"), (self._host, self._port))
        except Exception as exc:
            logger.error("Error sending OTLP UDP data: %s", exc)
            raise

    def shutdown(self):
        self._socket.close()


class UdpSpanExporter:
    def __init__(self, endpoint=None):
        self._udp_exporter = UdpExporter(endpoint=endpoint)
        self._app_signals_enabled = os.environ.get("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false").lower() == "true"

    def export(self, spans):
        if not spans:
            return True
        try:
            if self._app_signals_enabled:
                for span in spans:
                    self._inject_app_signals_attributes(span)
            otlp_data = _encode_export_trace_request(list(spans))
            prefix = "T1S" if spans[0].get_span_context().trace_flags.sampled else "T1U"
            self._udp_exporter.send_otlp(otlp_data, prefix=prefix)
            return True
        except Exception:
            logger.error("Failed to export %d span(s)", len(spans), exc_info=True)
            return False

    def _inject_app_signals_attributes(self, span):
        resource = span.resource or {}
        service_name = resource.get("service.name", "")
        lambda_function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
        local_operation = f"{lambda_function_name}/FunctionHandler" if lambda_function_name else span.name

        # Write directly: spans are already ended, so set_attribute() would no-op.
        span._attributes["aws.local.service"] = service_name
        span._attributes["aws.local.operation"] = local_operation
        span._attributes["aws.local.environment"] = "lambda:default"

        if span.kind in (SpanKind.CLIENT, SpanKind.PRODUCER):
            span._attributes["aws.remote.service"] = _resolve_remote_service(span._attributes)
            span._attributes["aws.remote.operation"] = _resolve_remote_operation(span._attributes)

    def force_flush(self, timeout_millis=30000):
        return True

    def shutdown(self):
        self._udp_exporter.shutdown()


class BatchingSpanProcessor(SpanProcessor):
    def __init__(self, exporter):
        self._exporter = exporter
        self._spans = []

    def on_end(self, span):
        self._spans.append(span)

    def force_flush(self, timeout_millis=30000):
        if self._spans:
            # Clear buffer unconditionally: retaining failed spans would leak memory across warm starts
            # since systematic encoding errors would re-fail every invocation. Errors are logged in export().
            self._exporter.export(self._spans)
            self._spans = []
        return True

    def shutdown(self, timeout_millis=30000):
        self.force_flush(timeout_millis)
        self._exporter.shutdown()


def configure_lite_mode():
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.propagators.aws.aws_xray_propagator import AwsXRayPropagator
    from opentelemetry.propagators.composite import CompositePropagator
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    provider = TracerProvider()
    endpoint = os.environ.get("AWS_XRAY_DAEMON_ADDRESS", "127.0.0.1:2000")
    try:
        provider.add_span_processor(BatchingSpanProcessor(UdpSpanExporter(endpoint=endpoint)))
    except Exception:
        logger.warning("Failed to create UDP exporter; telemetry will be dropped", exc_info=True)

    trace_api.set_tracer_provider(provider)

    set_global_textmap(
        CompositePropagator(
            [
                W3CBaggagePropagator(),
                AwsXRayPropagator(),
                TraceContextTextMapPropagator(),
            ]
        )
    )

    enabled = {"botocore", "requests", "urllib3"}
    disabled = set(
        d.strip() for d in os.environ.get("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "").split(",") if d.strip()
    )

    _instrumentors = {
        "botocore": "opentelemetry.instrumentation.botocore:BotocoreInstrumentor",
        "requests": "opentelemetry.instrumentation.requests:RequestsInstrumentor",
        "urllib3": "opentelemetry.instrumentation.urllib3:URLLib3Instrumentor",
        "aio-pika": "opentelemetry.instrumentation.aio_pika:AioPikaInstrumentor",
        "aiohttp-client": "opentelemetry.instrumentation.aiohttp_client:AioHttpClientInstrumentor",
        "aiohttp-server": "opentelemetry.instrumentation.aiohttp_server:AioHttpServerInstrumentor",
        "aiopg": "opentelemetry.instrumentation.aiopg:AiopgInstrumentor",
        "asgi": "opentelemetry.instrumentation.asgi:OpenTelemetryMiddleware",
        "asyncpg": "opentelemetry.instrumentation.asyncpg:AsyncPGInstrumentor",
        "boto": "opentelemetry.instrumentation.boto:BotoInstrumentor",
        "boto3sqs": "opentelemetry.instrumentation.boto3sqs:Boto3SQSInstrumentor",
        "cassandra": "opentelemetry.instrumentation.cassandra:CassandraInstrumentor",
        "celery": "opentelemetry.instrumentation.celery:CeleryInstrumentor",
        "confluent_kafka": "opentelemetry.instrumentation.confluent_kafka:ConfluentKafkaInstrumentor",
        "dbapi": "opentelemetry.instrumentation.dbapi:DatabaseApiIntegration",
        "django": "opentelemetry.instrumentation.django:DjangoInstrumentor",
        "elasticsearch": "opentelemetry.instrumentation.elasticsearch:ElasticsearchInstrumentor",
        "falcon": "opentelemetry.instrumentation.falcon:FalconInstrumentor",
        "fastapi": "opentelemetry.instrumentation.fastapi:FastAPIInstrumentor",
        "flask": "opentelemetry.instrumentation.flask:FlaskInstrumentor",
        "grpc_client": "opentelemetry.instrumentation.grpc:GrpcInstrumentorClient",
        "grpc_server": "opentelemetry.instrumentation.grpc:GrpcInstrumentorServer",
        "httpx": "opentelemetry.instrumentation.httpx:HTTPXClientInstrumentor",
        "jinja2": "opentelemetry.instrumentation.jinja2:Jinja2Instrumentor",
        "kafka": "opentelemetry.instrumentation.kafka:KafkaInstrumentor",
        "logging": "opentelemetry.instrumentation.logging:LoggingInstrumentor",
        "mysql": "opentelemetry.instrumentation.mysql:MySQLInstrumentor",
        "mysqlclient": "opentelemetry.instrumentation.mysqlclient:MySQLClientInstrumentor",
        "pika": "opentelemetry.instrumentation.pika:PikaInstrumentor",
        "psycopg": "opentelemetry.instrumentation.psycopg:PsycopgInstrumentor",
        "psycopg2": "opentelemetry.instrumentation.psycopg2:Psycopg2Instrumentor",
        "pymemcache": "opentelemetry.instrumentation.pymemcache:PymemcacheInstrumentor",
        "pymongo": "opentelemetry.instrumentation.pymongo:PymongoInstrumentor",
        "pymysql": "opentelemetry.instrumentation.pymysql:PyMySQLInstrumentor",
        "pyramid": "opentelemetry.instrumentation.pyramid:PyramidInstrumentor",
        "redis": "opentelemetry.instrumentation.redis:RedisInstrumentor",
        "remoulade": "opentelemetry.instrumentation.remoulade:RemouladeInstrumentor",
        "sqlalchemy": "opentelemetry.instrumentation.sqlalchemy:SQLAlchemyInstrumentor",
        "sqlite3": "opentelemetry.instrumentation.sqlite3:SQLite3Instrumentor",
        "starlette": "opentelemetry.instrumentation.starlette:StarletteInstrumentor",
        "threading": "opentelemetry.instrumentation.threading:ThreadingInstrumentor",
        "tornado": "opentelemetry.instrumentation.tornado:TornadoInstrumentor",
        "tortoiseorm": "opentelemetry.instrumentation.tortoiseorm:TortoiseORMInstrumentor",
        "wsgi": "opentelemetry.instrumentation.wsgi:OpenTelemetryMiddleware",
    }

    for name, path in _instrumentors.items():
        if enabled and name not in enabled:
            continue
        if name in disabled:
            continue
        module_path, class_name = path.rsplit(":", 1)
        try:
            module = __import__(module_path, fromlist=[class_name])
            instrumentor_class = getattr(module, class_name)
            instrumentor_class().instrument()
        except Exception:
            pass

    return provider
