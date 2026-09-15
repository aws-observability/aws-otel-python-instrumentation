# Changelog

All notable changes to the `cloudwatch-plugin-otel` package will be documented in this file.

For any change that affects end users of this package, please add an entry under the **Unreleased** section. Briefly summarize the change and provide the link to the PR. Example:
- add span metrics support
  ([#123](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/123))

If your change does not need a CHANGELOG entry, add the "skip changelog" label to your PR.

## Unreleased

- Add derived metric dimensions for dependency-edge metrics (messaging operation/consumer-group,
  peer, GenAI, AWS resource identity, and FaaS semantic-convention attributes)
  ([#896](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/896))

## v0.1.0 - 2026-08-14

- Initial span metrics release
  ([#857](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/857))
