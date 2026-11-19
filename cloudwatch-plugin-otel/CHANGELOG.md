# Changelog

All notable changes to the `cloudwatch-plugin-otel` package will be documented in this file.

For any change that affects end users of this package, please add an entry under the **Unreleased** section. Briefly summarize the change and provide the link to the PR. Example:
- add span metrics support
  ([#123](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/123))

If your change does not need a CHANGELOG entry, add the "skip changelog" label to your PR.

## Unreleased

- Add GenAI observability for LangChain/LangGraph (auto-instrumentation, `[langchain]` extra): activates the upstream
  LangChain instrumentation when only `langchain-core` is installed, and honors
  `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` for util-genai instrumentations; content capture is
  never enabled by default. `CW_PLUGIN_GENAI_ENABLED=false` turns this off; a warning names duplicate LangChain
  instrumentors. Privacy: if you already set `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` (ADOT agent
  mode sets it for you) and use util-genai-based instrumentations, their prompts and responses are now captured on
  spans after upgrading
  ([#979](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/979))
- Fix unbounded span metrics cardinality on SERVER spans: the legacy fallback for
  `server.address`/`server.port` now uses `net.host.name`/`net.host.port` on SERVER spans instead of
  `net.peer.name`/`net.peer.port`, which describe the client (including its ephemeral port)
  ([#925](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/925))

## v0.1.1 - 2026-09-18

- Add peer, GenAI, AWS resource-identity, FaaS, and messaging derived dimensions, with legacy net.peer.*/net.host.* fallbacks for server.address/server.port ([#896](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/896))
- `service.name` is no longer emitted as a metric datapoint attribute; it is carried by the
  metric's resource (the host SDK's resource). Consumers reading `service.name` from datapoint
  dimensions must read it from the resource instead.
- The `traces.span.metrics.calls` counter now uses the `{call}` unit (UCUM annotation) instead of
  an unset unit.

## v0.1.0 - 2026-08-14

- Initial span metrics release
  ([#857](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/857))
