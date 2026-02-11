#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
VENV_DIR="$REPO_ROOT/.venv_attribution"
OUTPUT_FILE="$REPO_ROOT/attribution/DEPENDENCY_ATTRIBUTION"
TEMP_FILE="$REPO_ROOT/.temp_attribution.json"

cleanup() {
    echo "Cleaning up..."
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate 2>/dev/null || true
    fi
    rm -rf "$VENV_DIR"
    rm -f "$TEMP_FILE" "${TEMP_FILE}.merged" "$REPO_ROOT/.temp_python.json" "$REPO_ROOT/.temp_rust.json" "$REPO_ROOT/.temp_rust.json.raw"
}

trap cleanup EXIT

echo "Creating virtual environment..."
python3 -m venv "$VENV_DIR"

echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

echo "Installing aws-opentelemetry-distro..."
pip install --quiet "$REPO_ROOT/aws-opentelemetry-distro"

echo "Installing pip-licenses..."
pip install --quiet pip-licenses

echo "Generating dependency attribution..."
TEMP_FILE="$REPO_ROOT/.temp_attribution.json"
TEMP_PYTHON="$REPO_ROOT/.temp_python.json"
TEMP_RUST="$REPO_ROOT/.temp_rust.json"

# Generate Python dependencies
pip-licenses --format=json --with-urls --with-authors --with-license-file --with-notice-file --no-license-path | jq --sort-keys 'sort_by(.Name)' > "$TEMP_PYTHON"

# Generate Rust dependencies for cp-utility
echo "Generating Rust dependency attribution for cp-utility..."
if ! command -v cargo-about &> /dev/null; then
    echo "Installing cargo-about..."
    cargo install --quiet --locked cargo-about
fi

cd "$REPO_ROOT/tools/cp-utility"
cargo about generate about.hbs > "$TEMP_RUST.raw" 2>/dev/null || echo "[]" > "$TEMP_RUST.raw"

# Convert cargo-about format to pip-licenses format
jq '[.[] | {
    Name: .name,
    Version: .version,
    License: (.license // "UNKNOWN"),
    LicenseText: (.license_text // "UNKNOWN"),
    NoticeText: "UNKNOWN",
    URL: (.repository // "UNKNOWN"),
    Author: (.authors // [] | join(", "))
}]' "$TEMP_RUST.raw" > "$TEMP_RUST"
rm "$TEMP_RUST.raw"
cd "$REPO_ROOT"

# Combine Python and Rust dependencies
jq -s 'add | sort_by(.Name)' "$TEMP_PYTHON" "$TEMP_RUST" > "$TEMP_FILE"
rm "$TEMP_PYTHON" "$TEMP_RUST"

if [ -f "$OUTPUT_FILE" ]; then
    echo "Merging with existing attribution..."
    # Merge: Index by Name@Version, preserve License/NoticeText/LicenseText from old file when new value is UNKNOWN allowing manual replacement if pip-license cannot find data.
    # Version changes trigger full refresh. Removes stale entries.
    jq -s '
        (.[0] | map({"\(.Name)@\(.Version)": .}) | add) as $old |
        (.[1] | map({"\(.Name)@\(.Version)": .}) | add) as $new |
        $new | to_entries | map(
            .value as $newDep |
            ($old[.key] // {}) as $oldDep |
            $newDep |
            if ($newDep.License == "UNKNOWN" and $oldDep.License != "UNKNOWN" and $oldDep.License != null and $oldDep.Version == $newDep.Version) then .License = $oldDep.License else . end |
            if ($newDep.NoticeText == "UNKNOWN" and $oldDep.NoticeText != "UNKNOWN" and $oldDep.NoticeText != null and $oldDep.Version == $newDep.Version) then .NoticeText = $oldDep.NoticeText else . end |
            if ($newDep.LicenseText == "UNKNOWN" and $oldDep.LicenseText != "UNKNOWN" and $oldDep.LicenseText != null and $oldDep.Version == $newDep.Version) then .LicenseText = $oldDep.LicenseText else . end
        ) | sort_by(.Name)
    ' "$OUTPUT_FILE" "$TEMP_FILE" > "${TEMP_FILE}.merged"
    mv "${TEMP_FILE}.merged" "$OUTPUT_FILE"
else
    echo "No existing attribution found, creating new file..."
    mv "$TEMP_FILE" "$OUTPUT_FILE"
fi

echo "Applying Apache-2.0 license text where applicable..."
# Sometimes pip-license struggles to find license text specifically for Apache-2.0/Apache Software License, give it a hand when this happens.
APACHE_LICENSE=$(curl -sS https://www.apache.org/licenses/LICENSE-2.0.txt)
jq --arg apacheLicense "$APACHE_LICENSE" '
    map(
        if ((.License == "Apache-2.0" or .License == "Apache Software License") and (.LicenseText == "UNKNOWN" or .LicenseText == null or .LicenseText == "")) then
            .LicenseText = $apacheLicense
        else
            .
        end
    )
' "$OUTPUT_FILE" > "${OUTPUT_FILE}.tmp"
mv "${OUTPUT_FILE}.tmp" "$OUTPUT_FILE"

echo "Dependency attribution generated at: $OUTPUT_FILE"
