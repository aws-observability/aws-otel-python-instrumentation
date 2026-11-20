# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""``aws.service.type`` and the AgentCore Runtime identity on the resource, on both SDK update paths (no upstream
packages needed)."""
import pytest
from plugins.opentelemetry.cloudwatch.genai import resource

from opentelemetry import _logs, trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my_agent-AbCdEf1234"
RUNTIME_URL = (
    "https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/"
    "arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A123456789012%3Aruntime%2Fmy_agent-AbCdEf1234/invocations"
)
RUNTIME = {
    "cloud.resource_id": ARN + "/runtime-endpoint/DEFAULT:DEFAULT",
    "cloud.platform": "aws_bedrock_agentcore",
}
SERVICE_TYPE = {"aws.service.type": "gen_ai_agent"}


@pytest.fixture(params=["update_resource", "fallback"])
def sdk_path(request, monkeypatch):
    """Runs a test on the SDK's ``_update_resource`` (SDK >= 1.44) and on the fallback (older SDKs)."""
    if request.param == "update_resource":
        if not hasattr(TracerProvider, "_update_resource"):
            pytest.skip("this SDK has no _update_resource")
    else:
        monkeypatch.setattr(TracerProvider, "_update_resource", None, raising=False)
        monkeypatch.setattr(LoggerProvider, "_update_resource", None, raising=False)
    return request.param


def _providers(attributes=None):
    """Fresh SDK tracer and logger providers, set globally; the tracer provider has an in-memory exporter."""
    tracer_provider = TracerProvider(resource=Resource.create(attributes or {}))
    tracer_provider.exporter = InMemorySpanExporter()
    tracer_provider.add_span_processor(SimpleSpanProcessor(tracer_provider.exporter))
    trace.set_tracer_provider(tracer_provider)
    logger_provider = LoggerProvider(resource=tracer_provider.resource)
    _logs.set_logger_provider(logger_provider)
    return tracer_provider, logger_provider


def _added(provider, keys=("aws.service.type", "cloud.resource_id", "cloud.platform")):
    return {key: provider.resource.attributes[key] for key in keys if key in provider.resource.attributes}


def test_the_marker_reaches_spans_of_tracers_created_before_and_after(genai_active, sdk_path):
    tracer_provider, _ = _providers({"service.name": "agent"})
    before = tracer_provider.get_tracer("before")

    assert resource.agent_identity_step() == "service.type"

    after = tracer_provider.get_tracer("after")
    before.start_span("from before").end()
    after.start_span("from after").end()
    spans = tracer_provider.exporter.get_finished_spans()
    assert [span.resource.attributes.get("aws.service.type") for span in spans] == ["gen_ai_agent"] * 2
    assert {span.resource.attributes["service.name"] for span in spans} == {"agent"}  # merged, not replaced


def test_the_logger_provider_is_updated_too(genai_active, sdk_path):
    _, logger_provider = _providers()
    before = logger_provider.get_logger("before")

    resource.agent_identity_step()

    assert _added(logger_provider) == SERVICE_TYPE
    assert before.resource.attributes["aws.service.type"] == "gen_ai_agent"
    assert logger_provider.get_logger("after").resource.attributes["aws.service.type"] == "gen_ai_agent"


def test_a_key_already_on_the_resource_is_kept(genai_active, sdk_path):
    tracer_provider, logger_provider = _providers({"aws.service.type": "custom"})

    assert resource.agent_identity_step() == resource.ALREADY_SET
    assert _added(tracer_provider) == _added(logger_provider) == {"aws.service.type": "custom"}


def test_a_key_in_otel_resource_attributes_is_kept(genai_active, sdk_path, monkeypatch):
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "team=agents,aws.service.type=custom")
    tracer_provider, logger_provider = _providers()  # Resource.create reads the variable, as the configurator does

    assert resource.agent_identity_step() == resource.ALREADY_SET
    assert _added(tracer_provider) == _added(logger_provider) == {"aws.service.type": "custom"}


def test_a_non_sdk_tracer_provider_is_skipped(genai_active):
    assert resource.agent_identity_step() == resource.NOT_SDK  # the API's proxy provider


def test_a_provider_without_an_sdk_resource_is_skipped(genai_active, monkeypatch):
    class Provider:  # takes span processors, but has no SDK Resource
        resource = None

        def add_span_processor(self, processor):
            pass

    monkeypatch.setattr(trace, "get_tracer_provider", Provider)
    assert resource.agent_identity_step() == resource.NO_SDK_RESOURCE


def test_no_active_genai_instrumentation_changes_nothing():
    tracer_provider, logger_provider = _providers()
    assert resource.agent_identity_step() is None
    assert _added(tracer_provider) == _added(logger_provider) == {}


# AgentCore Runtime identity -------------------------------------------------------------------------------------


def test_the_runtime_url_gives_the_runtime_endpoint_identity(genai_active, sdk_path, monkeypatch):
    monkeypatch.setenv("AGENTCORE_RUNTIME_URL", RUNTIME_URL)
    tracer_provider, logger_provider = _providers()

    assert resource.agent_identity_step() == "service.type+runtime"
    assert _added(tracer_provider) == _added(logger_provider) == {**SERVICE_TYPE, **RUNTIME}


def test_an_unencoded_arn_in_the_url_is_accepted(genai_active, monkeypatch):
    monkeypatch.setenv(
        "AGENTCORE_RUNTIME_URL", f"https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/{ARN}/invocations"
    )
    tracer_provider, _ = _providers()

    resource.agent_identity_step()
    assert _added(tracer_provider) == {**SERVICE_TYPE, **RUNTIME}


@pytest.mark.parametrize(
    "url",
    [
        "not a url",
        "http://[::1/runtimes/x/invocations",  # urlsplit raises ValueError
        "https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/invocations",
        RUNTIME_URL.replace("/invocations", "/ping"),
        RUNTIME_URL.replace("%3A123456789012%3A", "%3A1234%3A"),  # not an account id
        RUNTIME_URL.replace("bedrock-agentcore%3Aus", "s3%3Aus"),  # not an AgentCore ARN
        RUNTIME_URL.replace("runtime%2F", "memory%2F"),  # an AgentCore ARN, but not a runtime
        RUNTIME_URL.replace("%2Fmy_agent-AbCdEf1234", "%2Fmy_agent-AbCdEf1234%2Fx"),
    ],
)
def test_an_unrecognized_url_sets_neither_key(genai_active, monkeypatch, caplog, url):
    caplog.set_level("DEBUG", logger="plugins.opentelemetry.cloudwatch.genai")
    monkeypatch.setenv("AGENTCORE_RUNTIME_URL", url)
    tracer_provider, _ = _providers()

    assert resource.agent_identity_step() == "service.type"
    assert _added(tracer_provider) == SERVICE_TYPE
    assert [r.levelname for r in caplog.records] == ["DEBUG"]


def test_no_runtime_url_sets_neither_key(genai_active):
    tracer_provider, _ = _providers()
    assert resource.agent_identity_step() == "service.type"
    assert _added(tracer_provider) == SERVICE_TYPE


def test_user_values_win(genai_active, monkeypatch):
    monkeypatch.setenv("AGENTCORE_RUNTIME_URL", RUNTIME_URL)
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "cloud.resource_id=mine")
    tracer_provider, _ = _providers({"cloud.platform": "theirs"})

    assert resource.agent_identity_step() == "service.type"
    assert _added(tracer_provider) == {**SERVICE_TYPE, "cloud.resource_id": "mine", "cloud.platform": "theirs"}


def test_a_resource_that_already_has_the_identity_is_unchanged(genai_active, monkeypatch):
    """Attributes the user (or a distro) already put on the resource are not overridden."""
    monkeypatch.setenv("AGENTCORE_RUNTIME_URL", RUNTIME_URL)
    preset = {"aws.service.type": "gen_ai_agent", "cloud.resource_id": "preset"}
    tracer_provider, _ = _providers(preset)

    assert resource.agent_identity_step() == "runtime"  # only cloud.platform was missing
    assert _added(tracer_provider) == {**preset, "cloud.platform": "aws_bedrock_agentcore"}
