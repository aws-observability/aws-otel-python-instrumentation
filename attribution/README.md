# Attribution

This directory contains attribution information for the AWS OpenTelemetry Python Instrumentation project.

## Files

### `SOURCE_CODE_COMPLIANCE`
Documents copied source code within the ADOT repository. This includes:
- Upstream OpenTelemetry patches
- Copied software (e.g., gradlew)
- Any modified third-party code included in the repository

**When to update:** When copying or modifying code from external sources.

### `DEPENDENCY_ATTRIBUTION`
JSON-formatted attribution for installed Python dependencies. Generated automatically from pip-licenses.

**When to update:** Run the generation script when dependencies change.

## Attribution Requirements by Distribution Type

### GitHub Repository
- Attribute copied code only (e.g., OTEL upstream patches, gradlew)
- File: `SOURCE_CODE_COMPLIANCE`

### PyPi/NPM/NuGet Packages
- Thin artifacts: attribute copied code within ADOT repo that is vended in the package
- File: `SOURCE_CODE_COMPLIANCE`

### Docker Images
- Fat artifacts: attribute copied code **AND** installed dependencies
- Languages using AmazonLinux/Windows base: include OS bundle attributions
- Languages using `FROM scratch`: only ADOT code + dependencies
- Files: `SOURCE_CODE_COMPLIANCE` + `DEPENDENCY_ATTRIBUTION`

### Lambda Layers
- Fat artifacts: attribute copied code **AND** installed dependencies
- Files: `SOURCE_CODE_COMPLIANCE` + `DEPENDENCY_ATTRIBUTION`

## Generating Dependency Attribution

Run the generation script to update `DEPENDENCY_ATTRIBUTION`:

```bash
./scripts/generate_dependency_attribution.sh
```

The script:
1. Creates isolated virtual environment
2. Installs `aws-opentelemetry-distro` from local source
3. Generates Python dependency info using `pip-licenses`
4. Generates Rust dependency info using `cargo-about` for cp-utility
5. Combines Python and Rust dependencies
6. Merges with existing file, preserving known License/NoticeText/LicenseText values
7. Applies Apache-2.0 license text where applicable
8. Outputs deterministically sorted JSON
9. Cleans up automatically

### Merge Behavior
When updating, the script preserves values from the existing file when pip-licenses cannot determine them:
- **License**: Preserved if new value is UNKNOWN and existing is not
- **NoticeText**: Preserved if new value is UNKNOWN and existing is not
- **LicenseText**: Preserved if new value is UNKNOWN and existing is not

Version changes trigger full refresh. Manual overrides are preserved only when pip-licenses reports UNKNOWN for that field.

## Validating Dependency Attribution

Validate dependency licenses against an allowed list:

```bash
./scripts/validate_dependency_attribution.sh <license-config.yml>
```

Example configuration file:
```yaml
allow-licenses:
  - license-1
  - license-2
  - ...
```

The script validates:
- All dependencies have known licenses (not UNKNOWN)
- All dependencies have license text (not UNKNOWN)
- All licenses are in the allowed list

Exit codes:
- `0`: All validations passed
- `1`: Validation failures found
