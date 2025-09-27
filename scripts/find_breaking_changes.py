#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import sys

import requests
from packaging import version


def get_current_version_from_pyproject():
    """Extract current OpenTelemetry versions from pyproject.toml."""
    try:
        with open("aws-opentelemetry-distro/pyproject.toml", "r", encoding="utf-8") as file:
            content = file.read()

        # Find first opentelemetry-api version (core version)
        api_match = re.search(r'"opentelemetry-api == ([^"]*)"', content)
        current_core_version = api_match.group(1) if api_match else None

        # Find first opentelemetry-distro version (contrib version)
        distro_match = re.search(r'"opentelemetry-distro == ([^"]*)"', content)
        current_contrib_version = distro_match.group(1) if distro_match else None

        return current_core_version, current_contrib_version

    except Exception as error:
        print(f"Error reading current versions: {error}")
        return None, None


def get_releases_with_breaking_changes(repo, current_version, new_version):
    """Get releases between current and new version that mention breaking changes."""
    try:
        response = requests.get(f"https://api.github.com/repos/open-telemetry/{repo}/releases", timeout=30)
        response.raise_for_status()

        releases = response.json()
        breaking_releases = []

        for release in releases:
            release_version = release["tag_name"].lstrip("v")

            # Check if this release is between current and new version
            try:
                if version.parse(release_version) > version.parse(current_version) and version.parse(
                    release_version
                ) <= version.parse(new_version):

                    # Check if release notes mention breaking changes
                    body = release.get("body", "").lower()
                    if any(
                        keyword in body for keyword in ["breaking change", "breaking changes", "breaking:", "breaking"]
                    ):
                        breaking_releases.append(
                            {
                                "version": release_version,
                                "name": release["name"],
                                "url": release["html_url"],
                                "body": release.get("body", ""),
                            }
                        )
            except Exception:
                # Skip releases with invalid version formats
                continue

        return breaking_releases

    except requests.RequestException as request_error:
        print(f"Warning: Could not get releases for {repo}: {request_error}")
        return []


def main():
    new_core_version = os.environ.get("OTEL_PYTHON_VERSION")
    new_contrib_version = os.environ.get("OTEL_CONTRIB_VERSION")

    if not new_core_version or not new_contrib_version:
        print("Error: OTEL_PYTHON_VERSION and OTEL_CONTRIB_VERSION environment variables required")
        sys.exit(1)

    current_core_version, current_contrib_version = get_current_version_from_pyproject()

    if not current_core_version or not current_contrib_version:
        print("Could not determine current versions")
        sys.exit(1)

    print("Checking for breaking changes:")
    print(f"Core: {current_core_version} → {new_core_version}")
    print(f"Contrib: {current_contrib_version} → {new_contrib_version}")

    # Check both repos for breaking changes
    core_breaking = get_releases_with_breaking_changes("opentelemetry-python", current_core_version, new_core_version)
    contrib_breaking = get_releases_with_breaking_changes(
        "opentelemetry-python-contrib", current_contrib_version, new_contrib_version
    )

    # Output for GitHub Actions
    breaking_info = ""

    if core_breaking:
        breaking_info += "**opentelemetry-python:**\n"
        for release in core_breaking:
            breaking_info += f"- [{release['name']}]({release['url']})\n"

    if contrib_breaking:
        breaking_info += "\n**opentelemetry-python-contrib:**\n"
        for release in contrib_breaking:
            breaking_info += f"- [{release['name']}]({release['url']})\n"

    # Set GitHub output
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output_file:
            output_file.write(f"breaking_changes_info<<EOF\n{breaking_info}EOF\n")


if __name__ == "__main__":
    main()
