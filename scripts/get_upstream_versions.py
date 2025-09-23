#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import sys

import requests


def get_latest_otel_versions():
    """Get latest OpenTelemetry versions from GitHub releases."""
    try:
        # Query GitHub API for latest release
        response = requests.get(
            "https://api.github.com/repos/open-telemetry/opentelemetry-python/releases/latest", timeout=30
        )
        response.raise_for_status()

        release_data = response.json()
        release_title = release_data["name"]

        # Parse "Version 1.37.0/0.58b0" format
        match = re.search(r"Version\s+(\d+\.\d+\.\d+)/(\d+\.\d+b\d+)", release_title)
        if not match:
            print(f"Could not parse release title: {release_title}")
            sys.exit(1)

        otel_python_version = match.group(1)
        otel_contrib_version = match.group(2)

        return otel_python_version, otel_contrib_version

    except requests.RequestException as request_error:
        print(f"Error getting OpenTelemetry versions: {request_error}")
        sys.exit(1)


def main():
    otel_python_version, otel_contrib_version = get_latest_otel_versions()

    print(f"OTEL_PYTHON_VERSION={otel_python_version}")
    print(f"OTEL_CONTRIB_VERSION={otel_contrib_version}")

    # Write to GitHub output if in CI
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output_file:
            output_file.write(f"otel_python_version={otel_python_version}\n")
            output_file.write(f"otel_contrib_version={otel_contrib_version}\n")


if __name__ == "__main__":
    main()
