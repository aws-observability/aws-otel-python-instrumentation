# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""LangChain activation against fake instrumentors, with discovery patched (no upstream packages needed)."""
import logging
import sys
import types

import pytest
from genai_helpers import (
    FakeLangChainInstrumentor,
    FakeOtherLangChainInstrumentor,
    fake_entry_point,
    fake_upstream_module,
    install_fake_util_genai,
    messages,
)
from plugins.opentelemetry.cloudwatch.genai import _settings
from plugins.opentelemetry.cloudwatch.genai import langchain as lc

ADOT_DIST = "aws-opentelemetry-distro"
UPSTREAM_EP = fake_entry_point("langchain", "FakeLangChainInstrumentor", lc.UPSTREAM_DIST)
# Other vendors: OpenInference shares the `langchain` entry point name; ADOT uses `aws_langchain`.
OTHER_NAMES = ["langchain", "aws_langchain"]


def _other(name, dist="other-langchain-dist"):
    return fake_entry_point(name, "FakeOtherLangChainInstrumentor", dist)


@pytest.fixture
def discovery(monkeypatch, genai_installed):
    """Controls what activation sees: langchain_core, the upstream module, versions and entry points."""
    state = {"langchain_core": True, "versions": {"langchain-core": "0.3.60"}, "entry_points": [UPSTREAM_EP]}
    monkeypatch.setattr(_settings, "langchain_core_present", lambda: state["langchain_core"])
    monkeypatch.setattr(lc, "distribution_version", lambda name: state["versions"].get(name))
    monkeypatch.setattr(lc, "_entry_points", lambda: list(state["entry_points"]))
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, fake_upstream_module())
    return state


def _warnings(caplog):
    return messages(caplog, logging.WARNING)


def test_activates_once_with_skip_dep_check(discovery, caplog):
    assert lc.langchain_activation_step() == lc.ACTIVATED
    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER  # no second instrument()

    assert FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert FakeLangChainInstrumentor.calls == [{}]  # skip_dep_check consumed by instrument(); no provider kwargs
    assert "#915" in lc.ACTIVATED and "langchain-core only" in lc.ACTIVATED
    assert messages(caplog) == []  # the reason is carried by the summary line only


def test_activation_bypasses_the_dependency_check(discovery):
    # The fake declares `langchain >= 0.3.22, < 2`, which is not installed: a checked instrument() would refuse.
    assert FakeLangChainInstrumentor()._check_dependency_conflicts() is not None
    assert lc.langchain_activation_step() == lc.ACTIVATED


def test_upstream_is_imported_only_when_forcing(discovery, monkeypatch):
    monkeypatch.delitem(sys.modules, lc.UPSTREAM_MODULE)
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "langchain")

    assert lc.upstream_active() is False  # not imported by the loader: cannot be instrumented
    assert lc.langchain_activation_step() == lc.DISABLED
    assert lc.UPSTREAM_MODULE not in sys.modules


@pytest.mark.parametrize("disabled", ["langchain", " flask , langchain ", "*", "flask,*"])
def test_respects_the_disabled_instrumentations_list(discovery, monkeypatch, caplog, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", disabled)

    assert lc.langchain_activation_step() == lc.DISABLED
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []


@pytest.mark.parametrize("disabled", ["flask", "langchain_core", "lang chain", ""])
def test_other_disabled_entries_do_not_match(discovery, monkeypatch, disabled):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", disabled)
    assert lc.langchain_activation_step() == lc.ACTIVATED


def test_no_langchain_core_no_action(discovery, caplog):
    discovery["langchain_core"] = False  # the layer is off

    assert lc.langchain_activation_step() is None
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog, logging.WARNING) == []


def test_installed_langchain_is_not_overridden(discovery, caplog):
    discovery["versions"]["langchain"] = "2.0.0"

    assert lc.langchain_activation_step() == lc.FAILED
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    (warning,) = _warnings(caplog)
    assert "`langchain` 2.0.0" in warning and lc.UPSTREAM_DIST in warning and "langchain >= 0.3.22, < 2" in warning
    assert "Fix: use a langchain version in that range" in warning


def test_requirement_in_the_warning_comes_from_the_instrumentor(discovery, monkeypatch, caplog):
    discovery["versions"]["langchain"] = "9.0"
    monkeypatch.setattr(FakeLangChainInstrumentor, "instrumentation_dependencies", lambda self: ("langchain < 3",))

    lc.langchain_activation_step()
    assert "(it requires langchain < 3)" in _warnings(caplog)[0]


@pytest.mark.parametrize("version,outcome", [("0.3.48", lc.FAILED), ("0.3.49", lc.ACTIVATED), ("1.8.3", lc.ACTIVATED)])
def test_langchain_core_floor(discovery, caplog, version, outcome):
    discovery["versions"]["langchain-core"] = version

    assert lc.langchain_activation_step() == outcome
    if outcome == lc.FAILED:
        assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
        (warning,) = _warnings(caplog)
        assert f"langchain-core {version}" in warning and "0.3.49" in warning
    else:
        assert _warnings(caplog) == []


def test_instrument_raising_warns_and_does_not_raise(discovery, caplog):
    FakeLangChainInstrumentor.fail = True

    assert lc.langchain_activation_step() == lc.FAILED
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    (warning,) = _warnings(caplog)
    assert warning.startswith("CW-PLUGIN genai: no LangChain spans: activating ") and "RuntimeError('boom')" in warning


def test_instrumentor_import_failure_warns_and_does_not_raise(discovery, monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, lc.UPSTREAM_MODULE, None)  # import raises ImportError

    assert lc.langchain_activation_step() == lc.FAILED
    (warning,) = _warnings(caplog)
    assert "no LangChain spans: opentelemetry-instrumentation-genai-langchain could not be imported" in warning


def test_already_instrumented_by_the_loader_is_a_no_op(discovery, caplog):
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)
    FakeLangChainInstrumentor.calls = []

    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER
    assert FakeLangChainInstrumentor.calls == []
    assert messages(caplog) == []


@pytest.mark.parametrize("env", [{"OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": "langchain"}, {}])
def test_instrumented_by_loader_wins_over_disabled_and_installed_langchain(discovery, monkeypatch, caplog, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    discovery["versions"]["langchain"] = "1.4.0"
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)

    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER
    assert _warnings(caplog) == []


# Another vendor's LangChain instrumentor -----------------------------------------------------------------------


@pytest.mark.parametrize("name", OTHER_NAMES)
def test_another_active_langchain_instrumentor_is_not_doubled(discovery, caplog, name):
    discovery["entry_points"].append(_other(name, "openinference-instrumentation-langchain"))
    FakeOtherLangChainInstrumentor().instrument()

    assert (
        lc.langchain_activation_step()
        == "skipped: openinference-instrumentation-langchain already instruments LangChain"
    )
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []


@pytest.mark.parametrize("name", OTHER_NAMES)
def test_another_active_instrumentor_wins_over_disabled(discovery, monkeypatch, name):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "flask,*")
    discovery["entry_points"].append(_other(name, "aws-opentelemetry-distro"))
    FakeOtherLangChainInstrumentor().instrument()

    assert lc.langchain_activation_step() == "skipped: aws-opentelemetry-distro already instruments LangChain"


@pytest.mark.parametrize("name", OTHER_NAMES)
def test_an_inactive_other_instrumentor_does_not_block(discovery, name):
    discovery["entry_points"].append(_other(name))
    assert lc.langchain_activation_step() == lc.ACTIVATED


@pytest.mark.parametrize("name", ["flask", "openai"])
def test_unrelated_active_instrumentors_are_ignored(discovery, name):
    discovery["entry_points"].append(_other(name))
    FakeOtherLangChainInstrumentor().instrument()
    assert lc.langchain_activation_step() == lc.ACTIVATED


@pytest.mark.parametrize(
    "name,fix",
    [
        ("aws_langchain", "Fix: add aws_langchain to OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"),
        (
            "langchain",
            f"Fix: drop the [langchain] extra and uninstall {lc.UPSTREAM_DIST}, or uninstall other-vendor-langchain",
        ),
    ],
)
def test_upstream_and_another_instrumentor_both_active_warns(discovery, caplog, name, fix):
    discovery["entry_points"].append(_other(name, "other-vendor-langchain"))
    FakeOtherLangChainInstrumentor().instrument()
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)

    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER
    (warning,) = _warnings(caplog)
    assert "duplicated" in warning and lc.UPSTREAM_DIST in warning and "other-vendor-langchain" in warning
    assert warning.endswith(fix) and len(warning) < 300


def test_existing_outcome_is_pure(discovery, caplog):
    assert lc.existing_outcome(True, [("other", "aws_langchain")]) == lc.INSTRUMENTED_BY_LOADER
    assert lc.existing_outcome(False, [("other", "aws_langchain")]) == "skipped: other already instruments LangChain"
    assert lc.existing_outcome(False, []) is None
    assert messages(caplog) == []


def test_distribution_without_a_name_is_unknown(discovery, caplog):
    nameless = fake_entry_point("aws_langchain", "FakeOtherLangChainInstrumentor", None)
    discovery["entry_points"].append(nameless)
    discovery["versions"]["langchain"] = "0.3.25"  # installed: the loader would have instrumented upstream
    FakeOtherLangChainInstrumentor().instrument()

    assert lc.langchain_activation_step() == "skipped: unknown already instruments LangChain"


def test_probing_other_instrumentors_never_imports_or_raises(discovery, monkeypatch):
    never_imported = types.SimpleNamespace(
        name="aws_langchain", value="cw_plugin_never_imported:X", module="cw_plugin_never_imported", attr="X", dist=None
    )
    monkeypatch.setitem(sys.modules, "cw_plugin_broken_probe", types.ModuleType("cw_plugin_broken_probe"))
    broken = types.SimpleNamespace(name="langchain", module="cw_plugin_broken_probe", attr="X", dist=None)  # no .value
    bad_metadata = types.SimpleNamespace(name="langchain", dist=types.SimpleNamespace(metadata={}))
    discovery["entry_points"] += [never_imported, broken, bad_metadata]

    assert lc.langchain_activation_step() == lc.ACTIVATED
    assert "cw_plugin_never_imported" not in sys.modules


def test_upstream_entry_point_is_not_counted_as_another_instrumentor(discovery):
    discovery["entry_points"] = [_other("langchain", lc.UPSTREAM_DIST)]
    FakeOtherLangChainInstrumentor().instrument()
    assert lc.langchain_activation_step() == lc.ACTIVATED


# ADOT agent mode: no special case; activation fills the gap as everywhere --------------------------------


@pytest.fixture
def adot(discovery, monkeypatch):
    """ADOT installed, in agent mode."""
    monkeypatch.setenv("AGENT_OBSERVABILITY_ENABLED", "true")
    discovery["versions"][ADOT_DIST] = "0.23.0"
    return discovery


def test_agent_mode_auto_with_core_only_forces_upstream(adot):
    # ADOT's auto mode skipped aws_langchain (a `langchain` entry point is registered); the loader skipped upstream.
    assert lc.langchain_activation_step() == lc.ACTIVATED
    assert FakeLangChainInstrumentor().is_instrumented_by_opentelemetry


def test_agent_mode_auto_with_langchain_is_instrumented_by_the_loader(adot, caplog):
    adot["versions"]["langchain"] = "0.3.25"
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)  # what the loader did
    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER
    assert _warnings(caplog) == []


def test_agent_mode_enabled_with_langchain_warns_about_duplicates_with_the_agent_mode_remedy(adot, monkeypatch, caplog):
    monkeypatch.setenv("AWS_GENAI_INSTRUMENTATION", "enabled")  # ADOT runs aws_langchain as well
    adot["entry_points"].append(_other("aws_langchain", ADOT_DIST))
    FakeOtherLangChainInstrumentor().instrument()
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)

    assert lc.langchain_activation_step() == lc.INSTRUMENTED_BY_LOADER
    (warning,) = _warnings(caplog)
    assert "duplicated" in warning and ADOT_DIST in warning
    assert "unset AWS_GENAI_INSTRUMENTATION" in warning and f"and uninstall {lc.UPSTREAM_DIST}" in warning
    assert "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS" not in warning  # ADOT setdefaults it in agent mode


def test_agent_mode_with_aws_langchain_disabled_forces_upstream(adot, monkeypatch):
    monkeypatch.setenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "aws_langchain")
    assert lc.langchain_activation_step() == lc.ACTIVATED


@pytest.mark.parametrize(
    "env", [{"AWS_GENAI_INSTRUMENTATION": "disabled"}, {"AWS_AGENTIC_INSTRUMENTATION": "disabled"}]
)
def test_adot_genai_switch_does_not_govern_upstream(adot, monkeypatch, env):
    # ADOT: "third-party instrumentors are never disabled" by it (0.23.0, aws_opentelemetry_distro.py:112-114).
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert lc.langchain_activation_step() == lc.ACTIVATED


@pytest.mark.parametrize("variable", ["AWS_GENAI_INSTRUMENTATION", "AWS_AGENTIC_INSTRUMENTATION"])
def test_agent_mode_duplicate_remedy_names_the_variable_the_user_set(adot, monkeypatch, caplog, variable):
    monkeypatch.setenv(variable, "enabled")
    adot["entry_points"].append(_other("aws_langchain", ADOT_DIST))
    FakeOtherLangChainInstrumentor().instrument()
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)

    lc.langchain_activation_step()
    (warning,) = _warnings(caplog)
    assert f"unset {variable} (ADOT's auto mode" in warning
    assert f"drop the [langchain] extra and uninstall {lc.UPSTREAM_DIST}" in warning


def test_outside_agent_mode_the_aws_langchain_remedy_is_the_disabled_list(discovery, caplog):
    discovery["entry_points"].append(_other("aws_langchain", ADOT_DIST))
    FakeOtherLangChainInstrumentor().instrument()
    FakeLangChainInstrumentor().instrument(skip_dep_check=True)

    lc.langchain_activation_step()
    (warning,) = _warnings(caplog)
    assert warning.endswith("Fix: add aws_langchain to OTEL_PYTHON_DISABLED_INSTRUMENTATIONS")


def test_agent_mode_summary_is_printed(adot, monkeypatch, caplog):
    # pylint: disable=import-outside-toplevel
    from plugins.opentelemetry.cloudwatch import _auto_instrumentation

    install_fake_util_genai(monkeypatch)
    monkeypatch.setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true")  # ADOT's setdefault
    _auto_instrumentation.pre_instrument()
    _auto_instrumentation.post_instrument()
    (line,) = messages(caplog, logging.INFO)
    assert "content capture=SPAN_ONLY (translated from true)" in line and line.endswith(lc.ACTIVATED)


# Discovery helpers ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("find_spec,expected", [(lambda name: object(), True), (lambda name: None, False)])
def test_langchain_core_discovery(monkeypatch, find_spec, expected):
    monkeypatch.setattr(_settings.importlib.util, "find_spec", find_spec)
    assert _settings.langchain_core_present() is expected


def test_langchain_core_discovery_tolerates_broken_specs(monkeypatch):
    def broken(name):
        raise ValueError("langchain_core.__spec__ is None")

    monkeypatch.setattr(_settings.importlib.util, "find_spec", broken)
    assert _settings.langchain_core_present() is False


def test_real_entry_point_listing_names_distributions():
    assert "cloudwatch-plugin-otel" in {_settings.dist_name(ep) for ep in lc._entry_points()}  # span_metrics


def test_dist_name_without_metadata_name():
    assert _settings.dist_name(types.SimpleNamespace(dist=None)) == "unknown"
    assert _settings.dist_name(types.SimpleNamespace(dist=types.SimpleNamespace(metadata={}))) == "unknown"


def test_step_does_nothing_when_the_layer_is_inactive(discovery, genai_not_installed, caplog):
    assert lc.langchain_activation_step() is None
    assert not FakeLangChainInstrumentor().is_instrumented_by_opentelemetry
    assert messages(caplog) == []
