#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import sys

import requests

# AWS-specific packages with independent versioning
AWS_DEPS = [
    "opentelemetry-sdk-extension-aws",
    "opentelemetry-propagator-aws-xray",
]


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


def get_latest_aws_versions():
    """Get latest versions of AWS dependencies from PyPI."""
    versions = {}
    for dep in AWS_DEPS:
        try:
            response = requests.get(f"https://pypi.org/pypi/{dep}/json", timeout=30)
            response.raise_for_status()
            data = response.json()
            versions[dep] = data["info"]["version"]
        except requests.RequestException as e:
            print(f"Warning: Could not get latest version for {dep}: {e}", file=sys.stderr)
    return versions


def main():
    otel_python_version, otel_contrib_version = get_latest_otel_versions()
    aws_versions = get_latest_aws_versions()

    print(f"OTEL_PYTHON_VERSION={otel_python_version}")
    print(f"OTEL_CONTRIB_VERSION={otel_contrib_version}")
    
    # Print AWS dependency versions
    for dep, version in aws_versions.items():
        env_name = dep.replace("-", "_").upper() + "_VERSION"
        print(f"{env_name}={version}")

    # Write to GitHub output if in CI
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output_file:
            output_file.write(f"otel_python_version={otel_python_version}\n")
            output_file.write(f"otel_contrib_version={otel_contrib_version}\n")
            
            # Write AWS dependency versions
            for dep, version in aws_versions.items():
                env_name = dep.replace("-", "_").lower() + "_version"
                output_file.write(f"{env_name}={version}\n")


if __name__ == "__main__":
    main()
