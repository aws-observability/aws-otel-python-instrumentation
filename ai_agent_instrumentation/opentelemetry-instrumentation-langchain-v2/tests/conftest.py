import os

import pytest
from src.opentelemetry.instrumentation.langchain_v2 import LangChainInstrumentor

from opentelemetry.sdk._logs.export import InMemoryLogExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"


@pytest.fixture(scope="session", name="span_exporter")
def fixture_span_exporter():
    exporter = InMemorySpanExporter()
    yield exporter


@pytest.fixture(scope="function", name="log_exporter")
def fixture_log_exporter():
    exporter = InMemoryLogExporter()
    yield exporter


@pytest.fixture(scope="session", name="tracer_provider")
def fixture_tracer_provider(span_exporter):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    return provider


@pytest.fixture(autouse=True)
def environment():

    if not os.getenv("AWS_ACCESS_KEY_ID"):
        os.environ["AWS_ACCESS_KEY_ID"] = "test_aws_access_key_id"

    if not os.getenv("AWS_SECRET_ACCESS_KEY"):
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test_aws_secret_access_key"

    if not os.getenv("AWS_REGION"):
        os.environ["AWS_REGION"] = "us-west-2"

    if not os.getenv("AWS_BEDROCK_ENDPOINT_URL"):
        os.environ["AWS_BEDROCK_ENDPOINT_URL"] = "https://bedrock.us-west-2.amazonaws.com"

    if not os.getenv("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = "default"


def scrub_aws_credentials(response):
    """Remove sensitive data from response headers."""
    if "headers" in response:
        for sensitive_header in ["x-amz-security-token", "x-amz-request-id", "x-amzn-requestid", "x-amz-id-2"]:
            if sensitive_header in response["headers"]:
                response["headers"][sensitive_header] = ["REDACTED"]
    return response


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [
            ("authorization", "AWS4-HMAC-SHA256 REDACTED"),
            ("x-amz-date", "REDACTED_DATE"),
            ("x-amz-security-token", "REDACTED_TOKEN"),
            ("x-amz-content-sha256", "REDACTED_CONTENT_HASH"),
        ],
        "filter_query_parameters": [
            ("X-Amz-Security-Token", "REDACTED"),
            ("X-Amz-Signature", "REDACTED"),
        ],
        "decode_compressed_response": True,
        "before_record_response": scrub_aws_credentials,
    }


@pytest.fixture(scope="session")
def instrument_langchain(tracer_provider):
    langchain_instrumentor = LangChainInstrumentor()
    langchain_instrumentor.instrument(tracer_provider=tracer_provider)

    yield

    langchain_instrumentor.uninstrument()


@pytest.fixture(scope="function")
def instrument_no_content(tracer_provider):
    os.environ.update({OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT: "False"})

    instrumentor = LangChainInstrumentor()
    instrumentor.instrument(
        tracer_provider=tracer_provider,
    )
    yield instrumentor
    os.environ.pop(OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT, None)
    instrumentor.uninstrument()


@pytest.fixture(scope="function")
def instrument_with_content(tracer_provider):
    os.environ.update({OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT: "True"})
    instrumentor = LangChainInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider)

    yield instrumentor
    os.environ.pop(OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT, None)
    instrumentor.uninstrument()


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": ["Authorization", "X-Amz-Date", "X-Amz-Security-Token"],
        "filter_query_parameters": ["X-Amz-Signature", "X-Amz-Credential", "X-Amz-SignedHeaders"],
        "record_mode": "once",
        "cassette_library_dir": "tests/fixtures/vcr_cassettes",
    }


# Create the directory for cassettes if it doesn't exist
os.makedirs("tests/fixtures/vcr_cassettes", exist_ok=True)
