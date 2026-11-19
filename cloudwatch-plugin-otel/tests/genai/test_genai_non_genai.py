# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""A process without the GenAI extra never imports the GenAI modules or util-genai."""
import importlib.metadata

import pytest
from genai_helpers import child_env, plugin_entry_points, run_child

pytest.importorskip("flask")
pytest.importorskip("opentelemetry.instrumentation.flask")


def _extra_installed():
    try:
        importlib.metadata.version("opentelemetry-instrumentation-genai-langchain")
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


@pytest.mark.skipif(_extra_installed(), reason="the [langchain] extra is installed in this env")
@pytest.mark.parametrize("log_level", [None, "debug"])
def test_flask_only_app_imports_no_genai_module_and_keeps_its_logging(log_level):
    plugin_entry_points("opentelemetry_pre_instrument")

    env = child_env(**({"OTEL_PYTHON_LOG_LEVEL": log_level} if log_level else {}))
    report, stderr = run_child("child_non_genai.py", env=env)

    assert report["imported"] == []
    assert report["loggers"] == {  # level NOTSET, no handlers: untouched
        "plugins.opentelemetry.cloudwatch": [0, 0],
        "plugins.opentelemetry.cloudwatch.genai": [0, 0],
    }
    assert "CW-PLUGIN" not in stderr
