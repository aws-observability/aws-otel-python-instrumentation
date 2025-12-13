# Changelog

All notable changes to this project will be documented in this file.

> **Note:** This CHANGELOG was created starting from version 0.12.0. Earlier changes are not documented here.

For any change that affects end users of this package, please add an entry under the **Unreleased** section. Briefly summarize the change and provide the link to the PR. Example:
- add GenAI attribute support for Amazon Bedrock models
  ([#300](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/300))

If your change does not need a CHANGELOG entry, add the "skip changelog" label to your PR.

## Unreleased
- Add custom ADOT UserAgent for OTLP Spans Exporter
  ([#554](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/554))
- Disable django instrumentation if DJANGO_SETTINGS_MODULE is not set
  ([#549](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/549))
- [PATCH] Add safety check for bedrock ConverseStream responses
  ([#547](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/547))
- Add Service and Environment dimensions to EMF metrics when Application Signals EMF export is enabled
  ([#548](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/548))
- Refactor configuration for adding Application Signals Dimensions to EMF exporter
  ([#552](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/552))
- Fix CVE-2025-66471. No associated PR since `urllib3` dependency will auto-bump to `2.6.x` upon release.

## v0.14.0 - 2025-11-19
- Add Resource and CFN Attributes for Bedrock AgentCore spans
  ([#495](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/495))
- Add botocore instrumentation extension for Bedrock AgentCore services with span attributes
  ([#490](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/490))
- [PATCH] Only decode JSON input buffer in Anthropic Claude streaming
    ([#497](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/497))
- Fix timeout handling for exceeded deadline in retry logic in OTLPAwsLogsExporter
  ([#501](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/501))
- Fix Gevent patch regression with correct import order
  ([#522](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/522))
- Support credentials provider name for BedrockAgentCore Identity
  ([#534](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/534))

