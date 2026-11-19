# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""LangChain activation against real installed-distribution metadata, with the real loader, in ``python -I``
children: a `langchain` package in and out of the instrumentor's range, and another vendor's instrumentor."""
import pytest
from genai_helpers import child_env, plugin_entry_points, run_child, summary_parts, write_stub_dist

pytest.importorskip("opentelemetry.instrumentation.genai.langchain")
pytest.importorskip("langchain_core")

UPSTREAM_SCOPE = "opentelemetry.instrumentation.genai.langchain"

OTHER_INSTRUMENTOR = '''
import wrapt

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class OtherLangChainInstrumentor(BaseInstrumentor):
    """Another vendor's LangChain instrumentor: it needs only langchain-core and records one span per call."""

    def instrumentation_dependencies(self):
        return ()

    def _instrument(self, **kwargs):
        tracer = trace.get_tracer("cw.test.other.langchain")

        def invoke(wrapped, instance, args, kwargs):
            with tracer.start_as_current_span("other.chat"):
                return wrapped(*args, **kwargs)

        wrapt.wrap_function_wrapper("langchain_core.language_models.chat_models", "BaseChatModel.invoke", invoke)

    def _uninstrument(self, **kwargs):
        pass
'''


def _loader_env(other_entry_point_name=None, **extra):
    """The child restricts the loader to entry points named `langchain`, plus this one."""
    env = child_env(**extra)
    if other_entry_point_name:
        env["CW_TEST_EXTRA_ENTRY_POINT"] = other_entry_point_name
    return env


@pytest.fixture(autouse=True)
def _installed_hooks():
    for group in ("opentelemetry_pre_instrument", "opentelemetry_post_instrument"):
        plugin_entry_points(group)


def _plugin(report, level):
    return [r["msg"] for r in report["records"] if r["name"].startswith("plugins.") and r["level"] == level]


def _summary(report):
    (line,) = _plugin(report, "INFO")
    return line


def test_langchain_in_range_is_instrumented_by_the_loader(tmp_path):
    write_stub_dist(tmp_path, "langchain", "0.3.25")

    report, _ = run_child("child_langchain_loader.py", tmp_path)

    assert report["upstream_instrumented"] is True
    assert _summary(report).endswith("langchain=instrumented by loader")
    assert _plugin(report, "WARNING") == []
    assert [s["scope"] for s in report["spans"]] == [UPSTREAM_SCOPE]


def test_langchain_out_of_range_is_not_forced(tmp_path):
    write_stub_dist(tmp_path, "langchain", "2.0.0")

    report, _ = run_child("child_langchain_loader.py", tmp_path)

    assert report["upstream_instrumented"] is False
    assert _summary(report).endswith("langchain=failed")
    (warning,) = _plugin(report, "WARNING")
    assert "`langchain` 2.0.0" in warning and "langchain >= 0.3.22, < 2" in warning and "Fix:" in warning
    assert report["spans"] == []


def _write_other_vendor(directory, entry_point_name="langchain"):
    write_stub_dist(
        directory,
        "cw-test-other-langchain",
        "1.0",
        entry_points=(
            f"[opentelemetry_instrumentor]\n{entry_point_name} = cw_test_other_langchain:OtherLangChainInstrumentor\n"
        ),
        module_name="cw_test_other_langchain",
        module_code=OTHER_INSTRUMENTOR,
    )


@pytest.mark.parametrize("entry_point_name", ["langchain", "aws_langchain"])
def test_another_vendors_instrumentor_is_not_doubled(tmp_path, entry_point_name):
    _write_other_vendor(tmp_path, entry_point_name)

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_loader_env(entry_point_name))

    assert report["upstream_instrumented"] is False
    assert _summary(report).endswith("langchain=skipped: cw-test-other-langchain already instruments LangChain")
    assert _plugin(report, "WARNING") == []
    assert [s["name"] for s in report["spans"]] == ["other.chat"]  # one set of spans, not two


@pytest.mark.parametrize(
    "entry_point_name,fix",
    [
        ("aws_langchain", "Fix: add aws_langchain to OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"),
        ("langchain", "or uninstall cw-test-other-langchain"),
    ],
)
def test_upstream_and_another_vendor_both_active_warn(tmp_path, entry_point_name, fix):
    write_stub_dist(tmp_path, "langchain", "0.3.25")  # in range: the loader instruments upstream too
    _write_other_vendor(tmp_path, entry_point_name)

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_loader_env(entry_point_name))

    assert report["upstream_instrumented"] is True
    assert sorted(s["name"] for s in report["spans"]) == ["chat fake-chat-model", "other.chat"]  # duplicated
    (warning,) = _plugin(report, "WARNING")
    assert "duplicated" in warning and "cw-test-other-langchain" in warning and warning.endswith(fix)
    assert "; LangChain also instrumented by cw-test-other-langchain" in _summary(report)  # upstream's mode kept


ADOT_AGENT_MODE = {"AGENT_OBSERVABILITY_ENABLED": "true", "CW_TEST_ADOT_AGENT_MODE": "1"}


def _adot(directory):
    """ADOT's distribution metadata, with an `aws_langchain` instrumentor stand-in."""
    write_stub_dist(
        directory,
        "aws-opentelemetry-distro",
        "0.23.0",
        entry_points=(
            "[opentelemetry_instrumentor]\naws_langchain = cw_test_other_langchain:OtherLangChainInstrumentor\n"
        ),
        module_name="cw_test_other_langchain",
        module_code=OTHER_INSTRUMENTOR,
    )


def _adot_env(**extra):
    return _loader_env("aws_langchain", **ADOT_AGENT_MODE, **extra)


def test_adot_agent_mode_auto_with_core_only_forces_upstream_with_content(tmp_path):
    _adot(tmp_path)

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_adot_env())

    assert report["upstream_instrumented"] is True  # ADOT skipped aws_langchain, the loader skipped upstream
    assert [(s["scope"], s["content"]) for s in report["spans"]] == [(UPSTREAM_SCOPE, True)]  # ADOT's `true`
    content, langchain = summary_parts(_summary(report))
    assert content == "SPAN_ONLY (translated from true)" and langchain.startswith("activated")
    assert _plugin(report, "WARNING") == []


def test_adot_agent_mode_auto_with_langchain_is_instrumented_by_the_loader(tmp_path):
    _adot(tmp_path)
    write_stub_dist(tmp_path, "langchain", "0.3.25")

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_adot_env())

    assert summary_parts(_summary(report))[1] == "instrumented by loader"
    assert [(s["scope"], s["content"]) for s in report["spans"]] == [(UPSTREAM_SCOPE, True)]


def test_adot_agent_mode_enabled_with_langchain_warns_about_duplicates(tmp_path):
    _adot(tmp_path)
    write_stub_dist(tmp_path, "langchain", "0.3.25")

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_adot_env(AWS_GENAI_INSTRUMENTATION="enabled"))

    assert sorted(s["name"] for s in report["spans"]) == ["chat fake-chat-model", "other.chat"]
    (warning,) = _plugin(report, "WARNING")
    assert "duplicated" in warning and "aws-opentelemetry-distro" in warning
    assert "unset AWS_GENAI_INSTRUMENTATION" in warning and "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS" not in warning
    assert "LangChain also instrumented by aws-opentelemetry-distro" in _summary(report)


def test_adot_agent_mode_with_aws_langchain_disabled_forces_upstream(tmp_path):
    _adot(tmp_path)
    env = _adot_env(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="aws_langchain")

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=env)

    assert report["upstream_instrumented"] is True
    assert [s["scope"] for s in report["spans"]] == [UPSTREAM_SCOPE]


def test_adot_genai_disabled_does_not_govern_upstream(tmp_path):
    """ADOT's switch disables only its own aws_* instrumentors; upstream still fills the gap."""
    _adot(tmp_path)

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_adot_env(AWS_GENAI_INSTRUMENTATION="disabled"))

    assert report["upstream_instrumented"] is True
    assert [(s["scope"], s["content"]) for s in report["spans"]] == [(UPSTREAM_SCOPE, True)]
    assert summary_parts(_summary(report))[1].startswith("activated")


def test_nameless_distribution_with_langchain_installed(tmp_path):
    """One instrumentor distribution has no Name; the step still completes and the summary stays correct."""
    write_stub_dist(tmp_path, "langchain", "0.3.25")
    _write_other_vendor(tmp_path, "aws_langchain")
    metadata = next(tmp_path.glob("cw_test_other_langchain-*.dist-info")) / "METADATA"
    metadata.write_text("Metadata-Version: 2.1\nVersion: 1.0\n", "utf-8")  # no Name

    report, _ = run_child("child_langchain_loader.py", tmp_path, env=_loader_env("aws_langchain"))

    summary = _summary(report)
    assert summary.endswith("langchain=instrumented by loader")
    assert "LangChain also instrumented by unknown" in summary
