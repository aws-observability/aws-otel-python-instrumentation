# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
End-to-end tests for the OTel Lite SDK in Lambda environment.

These tests simulate the Lambda execution lifecycle with AWS_LAMBDA_LITE_MODE=true,
verifying that:
- The lite TracerProvider is registered and creates spans correctly
- Parent context is extracted from X-Ray trace headers
- Application Signals attributes are injected when enabled
- OTLP encoding and UDP export work end-to-end
- BatchingSpanProcessor flushes all spans at invocation end
"""

import os
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from opentelemetry.propagate import get_global_textmap
from opentelemetry.propagators.aws.aws_xray_propagator import TRACE_ID_FIRST_PART_LENGTH, TRACE_ID_VERSION
from opentelemetry.trace import SpanKind

INIT_OTEL_SCRIPTS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "lambda-layer", "src")
)

_SKIP_REASON = "lambda-layer/src or required dependencies not available"
try:
    sys.path.insert(0, INIT_OTEL_SCRIPTS_DIR)
    import wrapt  # noqa: F401

    from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor  # noqa: F401

    _HAS_LAMBDA_LAYER = True
except (ImportError, ModuleNotFoundError):
    _HAS_LAMBDA_LAYER = False
finally:
    if INIT_OTEL_SCRIPTS_DIR in sys.path:
        sys.path.remove(INIT_OTEL_SCRIPTS_DIR)

_X_AMZN_TRACE_ID = "_X_AMZN_TRACE_ID"
_HANDLER = "_HANDLER"

MOCK_XRAY_TRACE_ID = 0x5FB7331105E8BB83207FA31D4D9CDB4C
MOCK_XRAY_TRACE_ID_STR = f"{MOCK_XRAY_TRACE_ID:x}"
MOCK_XRAY_PARENT_SPAN_ID = 0x3328B8445A6DBAD2
MOCK_XRAY_LAMBDA_LINEAGE = "Lineage=01cfa446:0"
MOCK_XRAY_TRACE_CONTEXT_COMMON = (
    f"Root={TRACE_ID_VERSION}-{MOCK_XRAY_TRACE_ID_STR[:TRACE_ID_FIRST_PART_LENGTH]}"
    f"-{MOCK_XRAY_TRACE_ID_STR[TRACE_ID_FIRST_PART_LENGTH:]};Parent={MOCK_XRAY_PARENT_SPAN_ID:x}"
)
MOCK_XRAY_TRACE_CONTEXT_SAMPLED = f"{MOCK_XRAY_TRACE_CONTEXT_COMMON};Sampled=1;{MOCK_XRAY_LAMBDA_LINEAGE}"
MOCK_XRAY_TRACE_CONTEXT_NOT_SAMPLED = f"{MOCK_XRAY_TRACE_CONTEXT_COMMON};Sampled=0;{MOCK_XRAY_LAMBDA_LINEAGE}"

MOCK_W3C_TRACE_ID = 0x5CE0E9A56015FEC5AADFA328AE398115
MOCK_W3C_PARENT_SPAN_ID = 0xAB54A98CEB1F0AD2
MOCK_W3C_TRACE_CONTEXT_SAMPLED = f"00-{MOCK_W3C_TRACE_ID:x}-{MOCK_W3C_PARENT_SPAN_ID:x}-01"


class MockLambdaContext:
    def __init__(self, aws_request_id="mock_request_id", invoked_function_arn=None):
        self.aws_request_id = aws_request_id
        self.invoked_function_arn = (
            invoked_function_arn or "arn:aws:lambda:us-west-2:123456789012:function:test-function"
        )


MOCK_LAMBDA_CONTEXT = MockLambdaContext(
    aws_request_id="mock_aws_request_id",
    invoked_function_arn="arn:aws:lambda:us-west-2:123456789012:function:my-function",
)


@unittest.skipUnless(_HAS_LAMBDA_LAYER, _SKIP_REASON)
class TestLiteSdkLambdaE2E(unittest.TestCase):
    """E2E tests for the lite SDK Lambda path (AWS_LAMBDA_LITE_MODE=true)."""

    def setUp(self):
        self.env_patcher = mock.patch.dict(
            "os.environ",
            {
                "AWS_LAMBDA_LITE_MODE": "true",
                "AWS_LAMBDA_FUNCTION_NAME": "my-function",
                "AWS_LAMBDA_LOG_GROUP_NAME": "/aws/lambda/my-function",
                "AWS_REGION": "us-west-2",
                "OTEL_SERVICE_NAME": "my-function",
                "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2,cloud.platform=aws_lambda,cloud.provider=aws",
                "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false",
            },
        )
        self.env_patcher.start()

        self.urllib3_patcher = mock.patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.urllib3": MagicMock(),
            },
        )
        self.urllib3_patcher.start()

        sys.path.insert(0, INIT_OTEL_SCRIPTS_DIR)

        from amazon.opentelemetry.distro.opentelemetry_lite_sdk import (
            BatchingSpanProcessor,
            TracerProvider,
            UdpSpanExporter,
            configure_lite_mode,
        )

        self.TracerProvider = TracerProvider
        self.BatchingSpanProcessor = BatchingSpanProcessor
        self.UdpSpanExporter = UdpSpanExporter
        self.configure_lite_mode = configure_lite_mode

    def tearDown(self):
        self.env_patcher.stop()
        self.urllib3_patcher.stop()
        if INIT_OTEL_SCRIPTS_DIR in sys.path:
            sys.path.remove(INIT_OTEL_SCRIPTS_DIR)

    @patch("socket.socket")
    def test_configure_lite_mode_registers_provider(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()

        self.assertIsInstance(provider, self.TracerProvider)
        self.assertEqual(provider.resource["service.name"], "my-function")
        self.assertEqual(provider.resource["cloud.region"], "us-west-2")

    @patch("socket.socket")
    def test_span_creation_with_lite_provider(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER) as span:
            span.set_attribute("faas.invocation_id", "test-request-id")
            span.set_attribute("cloud.resource_id", "arn:aws:lambda:us-west-2:123:function:my-function")

        self.assertEqual(span.name, "my-function.handler")
        self.assertEqual(span.kind, SpanKind.SERVER)
        self.assertEqual(span.attributes["faas.invocation_id"], "test-request-id")
        self.assertIsNotNone(span.start_time)
        self.assertIsNotNone(span.end_time)

    @patch("socket.socket")
    def test_batching_processor_collects_and_flushes(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        span1 = tracer.start_span("handler", kind=SpanKind.SERVER)
        span1.end()
        span2 = tracer.start_span("S3.ListBuckets", kind=SpanKind.CLIENT)
        span2.end()

        mock_socket.sendto.assert_not_called()

        provider.force_flush()

        mock_socket.sendto.assert_called_once()

    @patch("socket.socket")
    def test_xray_context_propagation(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        headers = {
            "X-Amzn-Trace-Id": MOCK_XRAY_TRACE_CONTEXT_SAMPLED,
        }
        ctx = get_global_textmap().extract(headers)

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER, context=ctx) as span:
            span_context = span.get_span_context()
            self.assertEqual(span_context.trace_id, MOCK_XRAY_TRACE_ID)
            self.assertTrue(span_context.trace_flags.sampled)
            self.assertIsNotNone(span.parent)
            self.assertEqual(span.parent.span_id, MOCK_XRAY_PARENT_SPAN_ID)

    @patch("socket.socket")
    def test_unsampled_trace_propagation(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        headers = {
            "X-Amzn-Trace-Id": MOCK_XRAY_TRACE_CONTEXT_NOT_SAMPLED,
        }
        ctx = get_global_textmap().extract(headers)

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER, context=ctx) as span:
            span_context = span.get_span_context()
            self.assertEqual(span_context.trace_id, MOCK_XRAY_TRACE_ID)
            self.assertFalse(span_context.trace_flags.sampled)

    @patch("socket.socket")
    def test_udp_prefix_sampled(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        headers = {"X-Amzn-Trace-Id": MOCK_XRAY_TRACE_CONTEXT_SAMPLED}
        ctx = get_global_textmap().extract(headers)

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER, context=ctx):
            pass

        provider.force_flush()

        sent_data = mock_socket.sendto.call_args[0][0].decode("utf-8")
        self.assertIn("T1S", sent_data)

    @patch("socket.socket")
    def test_udp_prefix_unsampled(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        headers = {"X-Amzn-Trace-Id": MOCK_XRAY_TRACE_CONTEXT_NOT_SAMPLED}
        ctx = get_global_textmap().extract(headers)

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER, context=ctx):
            pass

        provider.force_flush()

        sent_data = mock_socket.sendto.call_args[0][0].decode("utf-8")
        self.assertIn("T1U", sent_data)

    @patch("socket.socket")
    def test_parent_child_span_relationship(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER) as parent:
            with tracer.start_as_current_span("S3.ListBuckets", kind=SpanKind.CLIENT) as child:
                parent_ctx = parent.get_span_context()
                child_ctx = child.get_span_context()

                self.assertEqual(child_ctx.trace_id, parent_ctx.trace_id)
                self.assertNotEqual(child_ctx.span_id, parent_ctx.span_id)
                self.assertEqual(child.parent.span_id, parent_ctx.span_id)

    @patch("socket.socket")
    def test_exception_recording(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        with self.assertRaises(ValueError):
            with tracer.start_as_current_span("handler", kind=SpanKind.SERVER) as span:
                raise ValueError("handler failed")

        from opentelemetry.trace.status import StatusCode

        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")
        self.assertIn("ValueError", span.events[0].attributes["exception.type"])

    @patch("socket.socket")
    def test_otlp_encoding_in_flush(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER) as span:
            span.set_attribute("faas.invocation_id", "req-123")

        provider.force_flush()

        self.assertTrue(mock_socket.sendto.called)
        sent_bytes = mock_socket.sendto.call_args[0][0]
        self.assertIsInstance(sent_bytes, bytes)
        self.assertGreater(len(sent_bytes), 50)


@unittest.skipUnless(_HAS_LAMBDA_LAYER, _SKIP_REASON)
class TestLiteSdkAppSignalsE2E(unittest.TestCase):
    """E2E tests for lite SDK with Application Signals enabled."""

    def setUp(self):
        self.env_patcher = mock.patch.dict(
            "os.environ",
            {
                "AWS_LAMBDA_LITE_MODE": "true",
                "AWS_LAMBDA_FUNCTION_NAME": "my-function",
                "AWS_LAMBDA_LOG_GROUP_NAME": "/aws/lambda/my-function",
                "AWS_REGION": "us-west-2",
                "OTEL_SERVICE_NAME": "my-function",
                "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2,cloud.platform=aws_lambda,cloud.provider=aws",
                "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true",
            },
        )
        self.env_patcher.start()

        self.urllib3_patcher = mock.patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.urllib3": MagicMock(),
            },
        )
        self.urllib3_patcher.start()

        from amazon.opentelemetry.distro.opentelemetry_lite_sdk import TracerProvider, configure_lite_mode

        self.TracerProvider = TracerProvider
        self.configure_lite_mode = configure_lite_mode

    def tearDown(self):
        self.env_patcher.stop()
        self.urllib3_patcher.stop()

    @patch("socket.socket")
    def test_server_span_gets_app_signals_attributes(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        span = tracer.start_span("handler", kind=SpanKind.SERVER)
        span.end()

        provider.force_flush()

        self.assertEqual(span._attributes["aws.local.service"], "my-function")
        self.assertEqual(span._attributes["aws.local.operation"], "my-function/FunctionHandler")
        self.assertEqual(span._attributes["aws.local.environment"], "lambda:default")

    @patch("socket.socket")
    def test_client_span_gets_remote_attributes_aws(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        span = tracer.start_span("S3.ListBuckets", kind=SpanKind.CLIENT)
        span.set_attribute("rpc.service", "S3")
        span.set_attribute("rpc.system", "aws-api")
        span.set_attribute("rpc.method", "ListBuckets")
        span.end()

        provider.force_flush()

        self.assertEqual(span._attributes["aws.remote.service"], "AWS::S3")
        self.assertEqual(span._attributes["aws.remote.operation"], "ListBuckets")

    @patch("socket.socket")
    def test_client_span_gets_remote_attributes_http(self, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        span = tracer.start_span("GET", kind=SpanKind.CLIENT)
        span.set_attribute("http.url", "https://api.example.com/users")
        span.set_attribute("http.method", "GET")
        span.end()

        provider.force_flush()

        self.assertEqual(span._attributes["aws.remote.service"], "api.example.com")
        self.assertEqual(span._attributes["aws.remote.operation"], "GET /users")

    @patch("socket.socket")
    def test_full_invocation_simulation(self, mock_socket_class):
        """Simulates a complete Lambda invocation with SERVER + CLIENT spans."""
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        provider = self.configure_lite_mode()
        tracer = provider.get_tracer("test")

        headers = {"X-Amzn-Trace-Id": MOCK_XRAY_TRACE_CONTEXT_SAMPLED}
        ctx = get_global_textmap().extract(headers)

        with tracer.start_as_current_span("lambda_function.handler", kind=SpanKind.SERVER, context=ctx) as server_span:
            server_span.set_attribute("faas.invocation_id", "req-abc")
            server_span.set_attribute("cloud.resource_id", "arn:aws:lambda:us-west-2:123:function:my-function")
            server_span.set_attribute("cloud.account.id", "123456789012")

            with tracer.start_as_current_span("S3.ListBuckets", kind=SpanKind.CLIENT) as client_span:
                client_span.set_attribute("rpc.service", "S3")
                client_span.set_attribute("rpc.system", "aws-api")
                client_span.set_attribute("rpc.method", "ListBuckets")
                client_span.set_attribute("http.status_code", 200)

        provider.force_flush()

        self.assertEqual(server_span._attributes["aws.local.service"], "my-function")
        self.assertEqual(server_span._attributes["aws.local.operation"], "my-function/FunctionHandler")
        self.assertEqual(client_span._attributes["aws.remote.service"], "AWS::S3")
        self.assertEqual(client_span._attributes["aws.remote.operation"], "ListBuckets")

        self.assertEqual(server_span.get_span_context().trace_id, MOCK_XRAY_TRACE_ID)
        self.assertEqual(client_span.get_span_context().trace_id, MOCK_XRAY_TRACE_ID)

        mock_socket.sendto.assert_called_once()
        sent_data = mock_socket.sendto.call_args[0][0].decode("utf-8")
        self.assertIn("T1S", sent_data)

    @patch("socket.socket")
    def test_app_signals_disabled_no_injection(self, mock_socket_class):
        """When app signals is disabled, no aws.local.* attributes are injected."""
        self.env_patcher.stop()
        with mock.patch.dict(
            "os.environ",
            {
                "AWS_LAMBDA_LITE_MODE": "true",
                "AWS_LAMBDA_FUNCTION_NAME": "my-function",
                "AWS_LAMBDA_LOG_GROUP_NAME": "/aws/lambda/my-function",
                "AWS_REGION": "us-west-2",
                "OTEL_SERVICE_NAME": "my-function",
                "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2,cloud.platform=aws_lambda",
                "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false",
            },
        ):
            mock_socket_class.return_value = MagicMock()

            from amazon.opentelemetry.distro.opentelemetry_lite_sdk import (
                BatchingSpanProcessor,
                TracerProvider,
                UdpSpanExporter,
            )

            provider = TracerProvider()
            exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
            exporter._udp_exporter = MagicMock()
            provider.add_span_processor(BatchingSpanProcessor(exporter))

            tracer = provider.get_tracer("test")
            span = tracer.start_span("handler", kind=SpanKind.SERVER)
            span.end()

            provider.force_flush()

            self.assertNotIn("aws.local.service", span._attributes)
            self.assertNotIn("aws.local.operation", span._attributes)
        self.env_patcher.start()


_MOCK_HANDLER_MODULE = MagicMock(handler=MagicMock())


@unittest.skipUnless(_HAS_LAMBDA_LAYER, _SKIP_REASON)
class TestLiteModeEnvVarGating(unittest.TestCase):
    """Tests for the AWS_LAMBDA_LITE_MODE env var dispatch in otel_wrapper.py."""

    def setUp(self):
        sys.path.insert(0, INIT_OTEL_SCRIPTS_DIR)

    def tearDown(self):
        if INIT_OTEL_SCRIPTS_DIR in sys.path:
            sys.path.remove(INIT_OTEL_SCRIPTS_DIR)

    def _reload_otel_wrapper(self):
        import importlib

        if "otel_wrapper" in sys.modules:
            importlib.reload(sys.modules["otel_wrapper"])
        else:
            import otel_wrapper  # noqa: F401
        return sys.modules["otel_wrapper"]

    @patch("socket.socket")
    @patch("opentelemetry.instrumentation.aws_lambda.AwsLambdaInstrumentor.instrument")
    @patch.dict(
        "sys.modules",
        {
            "opentelemetry.instrumentation.urllib3": MagicMock(),
            "lambda_function": _MOCK_HANDLER_MODULE,
        },
    )
    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "true",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
            "OTEL_SERVICE_NAME": "my-function",
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2",
            "ORIG_HANDLER": "lambda_function.handler",
        },
    )
    def test_configure_lite_mode_called_when_true(self, mock_instrument, mock_socket_class):
        mock_socket_class.return_value = MagicMock()
        with patch("amazon.opentelemetry.distro.opentelemetry_lite_sdk.configure_lite_mode") as mock_configure:
            mock_configure.return_value = None
            call_count_before = mock_configure.call_count
            self._reload_otel_wrapper()
            self.assertEqual(mock_configure.call_count - call_count_before, 1)

    @patch("opentelemetry.instrumentation.aws_lambda.AwsLambdaInstrumentor.instrument")
    @patch.dict(
        "sys.modules",
        {
            "opentelemetry.instrumentation.urllib3": MagicMock(),
            "lambda_function": _MOCK_HANDLER_MODULE,
        },
    )
    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
            "OTEL_SERVICE_NAME": "my-function",
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2",
            "ORIG_HANDLER": "lambda_function.handler",
        },
        clear=False,
    )
    def test_configure_lite_mode_not_called_when_unset(self, mock_instrument):
        """Lite mode is not activated when AWS_LAMBDA_LITE_MODE is not set."""
        os.environ.pop("AWS_LAMBDA_LITE_MODE", None)
        with patch("amazon.opentelemetry.distro.opentelemetry_lite_sdk.configure_lite_mode") as mock_configure:
            self._reload_otel_wrapper()
            mock_configure.assert_not_called()

    @patch("opentelemetry.instrumentation.aws_lambda.AwsLambdaInstrumentor.instrument")
    @patch.dict(
        "sys.modules",
        {
            "opentelemetry.instrumentation.urllib3": MagicMock(),
            "lambda_function": _MOCK_HANDLER_MODULE,
        },
    )
    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "false",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
            "OTEL_SERVICE_NAME": "my-function",
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2",
            "ORIG_HANDLER": "lambda_function.handler",
        },
    )
    def test_configure_lite_mode_not_called_when_false(self, mock_instrument):
        with patch("amazon.opentelemetry.distro.opentelemetry_lite_sdk.configure_lite_mode") as mock_configure:
            self._reload_otel_wrapper()
            mock_configure.assert_not_called()

    @patch("socket.socket")
    @patch("opentelemetry.instrumentation.aws_lambda.AwsLambdaInstrumentor.instrument")
    @patch.dict(
        "sys.modules",
        {
            "opentelemetry.instrumentation.urllib3": MagicMock(),
            "lambda_function": _MOCK_HANDLER_MODULE,
        },
    )
    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "True",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
            "OTEL_SERVICE_NAME": "my-function",
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2",
            "ORIG_HANDLER": "lambda_function.handler",
        },
    )
    def test_configure_lite_mode_called_with_case_variation(self, mock_instrument, mock_socket_class):
        """'True' (capitalized) should still trigger lite mode since .lower() is used."""
        mock_socket_class.return_value = MagicMock()
        with patch("amazon.opentelemetry.distro.opentelemetry_lite_sdk.configure_lite_mode") as mock_configure:
            mock_configure.return_value = None
            call_count_before = mock_configure.call_count
            self._reload_otel_wrapper()
            self.assertEqual(mock_configure.call_count - call_count_before, 1)


@unittest.skipUnless(_HAS_LAMBDA_LAYER, _SKIP_REASON)
class TestAddCodeAttributesNoOpInLiteMode(unittest.TestCase):
    """Tests that add_code_attributes_to_span is a no-op in lite mode."""

    def setUp(self):
        sys.path.insert(0, INIT_OTEL_SCRIPTS_DIR)

    def tearDown(self):
        if INIT_OTEL_SCRIPTS_DIR in sys.path:
            sys.path.remove(INIT_OTEL_SCRIPTS_DIR)

    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "true",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
        },
    )
    def test_add_code_attributes_is_noop_in_lite_mode(self):
        import importlib

        import opentelemetry.instrumentation.aws_lambda as aws_lambda_mod

        importlib.reload(aws_lambda_mod)

        mock_span = MagicMock()
        mock_func = MagicMock()
        aws_lambda_mod.add_code_attributes_to_span(mock_span, mock_func)

        mock_span.set_attribute.assert_not_called()

    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "false",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
        },
    )
    def test_add_code_attributes_not_noop_when_disabled(self):
        import importlib

        import opentelemetry.instrumentation.aws_lambda as aws_lambda_mod

        importlib.reload(aws_lambda_mod)

        from amazon.opentelemetry.distro.code_correlation import add_code_attributes_to_span

        self.assertIs(aws_lambda_mod.add_code_attributes_to_span, add_code_attributes_to_span)


@unittest.skipUnless(_HAS_LAMBDA_LAYER, _SKIP_REASON)
class TestLiteSdkColdStartPath(unittest.TestCase):
    """Tests verifying the cold start optimization path works correctly."""

    @patch("socket.socket")
    @patch.dict("sys.modules", {"opentelemetry.instrumentation.urllib3": MagicMock()})
    @patch.dict(
        "os.environ",
        {
            "AWS_LAMBDA_LITE_MODE": "true",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
            "AWS_LAMBDA_LOG_GROUP_NAME": "/aws/lambda/my-function",
            "OTEL_SERVICE_NAME": "my-function",
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2,cloud.platform=aws_lambda",
            "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true",
        },
    )
    def test_multiple_invocations(self, mock_socket_class):
        """Simulates multiple Lambda invocations (warm starts) reusing the same provider."""
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        from amazon.opentelemetry.distro.opentelemetry_lite_sdk import configure_lite_mode

        provider = configure_lite_mode()
        tracer = provider.get_tracer("test")

        for i in range(3):
            with tracer.start_as_current_span(f"invocation-{i}", kind=SpanKind.SERVER) as span:
                span.set_attribute("faas.invocation_id", f"req-{i}")
            provider.force_flush()

        self.assertEqual(mock_socket.sendto.call_count, 3)
