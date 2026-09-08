# Changelog

All notable changes to the `cloudwatch-plugin-otel` package will be documented in this file.

For any change that affects end users of this package, please add an entry under the **Unreleased** section. Briefly summarize the change and provide the link to the PR. Example:
- add span metrics support
  ([#123](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/123))

If your change does not need a CHANGELOG entry, add the "skip changelog" label to your PR.

## Unreleased

- `service.name` is no longer emitted as a metric datapoint attribute; it is carried by the
  metric's resource (the host SDK's resource). Consumers reading `service.name` from datapoint
  dimensions must read it from the resource instead.
- The `traces.span.metrics.calls` counter now uses the `{call}` unit (UCUM annotation) instead of
  an unset unit.

## v0.1.0 - 2026-08-14

- Initial span metrics release
  ([#857](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/857))
