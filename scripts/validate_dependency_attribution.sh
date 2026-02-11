#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
ATTRIBUTION_FILE="$REPO_ROOT/attribution/DEPENDENCY_ATTRIBUTION"

usage() {
    echo "Usage: $0 <license-config.yml>"
    echo "Example: $0 .github/allowed-licenses.yml"
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

CONFIG_FILE="$1"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    exit 1
fi

if [ ! -f "$ATTRIBUTION_FILE" ]; then
    echo "Error: Attribution file not found: $ATTRIBUTION_FILE"
    echo "Run ./scripts/generate_dependency_attribution.sh first"
    exit 1
fi

echo "Validating dependency licenses..."

# Extract allowed licenses from YAML
ALLOWED_LICENSES=$(python3 -c "
import sys
import yaml

with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)
    
allowed = config.get('allow-licenses', [])
for license in allowed:
    print(license)
" | jq -R . | jq -s .)

FAILURES=0

# Check for UNKNOWN licenses or license text
UNKNOWN_ITEMS=$(jq -r '
    map(
        select(.License == "UNKNOWN" or .LicenseText == "UNKNOWN") |
        {Name: .Name, Version: .Version, License: .License, HasLicenseText: (.LicenseText != "UNKNOWN")}
    ) | 
    sort_by(.Name)
' "$ATTRIBUTION_FILE")

UNKNOWN_COUNT=$(echo "$UNKNOWN_ITEMS" | jq 'length')

if [ "$UNKNOWN_COUNT" -gt 0 ]; then
    echo "❌ Found $UNKNOWN_COUNT dependencies with UNKNOWN license information:"
    echo ""
    echo "$UNKNOWN_ITEMS" | jq -r '.[] | "  • \(.Name) (\(.Version)): License=\(.License), LicenseText=\(if .HasLicenseText then "Present" else "UNKNOWN" end)"'
    echo ""
    FAILURES=$((FAILURES + 1))
fi

# Find disallowed licenses (handles multi-license with semicolon separator)
VIOLATIONS=$(jq -r --argjson allowed "$ALLOWED_LICENSES" '
    map(
        select(.License != "UNKNOWN") |
        . as $dep |
        (.License | split(";") | map(gsub("^\\s+|\\s+$"; "")) | map(select(. != ""))) as $licenses |
        select([$licenses[] | . as $lic | $allowed | index($lic) | . == null] | any) |
        {Name: .Name, License: .License, Version: .Version}
    ) | 
    sort_by(.Name)
' "$ATTRIBUTION_FILE")

VIOLATION_COUNT=$(echo "$VIOLATIONS" | jq 'length')

if [ "$VIOLATION_COUNT" -gt 0 ]; then
    echo "❌ Found $VIOLATION_COUNT dependencies with disallowed licenses:"
    echo ""
    echo "$VIOLATIONS" | jq -r '.[] | "  • \(.Name) (\(.Version)): \(.License)"'
    echo ""
    FAILURES=$((FAILURES + 1))
fi

if [ "$FAILURES" -eq 0 ]; then
    echo "✅ All dependency licenses are valid and allowed"
    exit 0
else
    echo "To resolve UNKNOWN licenses:"
    echo "  1. Manually add license information to $ATTRIBUTION_FILE"
    echo "  2. Ensure package metadata includes license info"
    echo ""
    echo "To resolve disallowed licenses:"
    echo "  1. Add license to allow-licenses in $CONFIG_FILE, or"
    echo "  2. Remove dependency, or"
    echo "  3. Find alternative with allowed license"
    exit 1
fi
