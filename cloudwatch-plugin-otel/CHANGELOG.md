# Changelog

All notable changes to the `cloudwatch-plugin-otel` package will be documented in this file.

For any change that affects end users of this package, please add an entry under the **Unreleased** section. Briefly summarize the change and provide the link to the PR. Example:
- add span metrics support
  ([#123](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/123))

If your change does not need a CHANGELOG entry, add the "skip changelog" label to your PR.

## Unreleased

- Add GenAI observability: activate the upstream LangChain instrumentation when only `langchain-core` is installed,
  and honor `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` for util-genai instrumentations (content capture
  is never enabled by default). Privacy: if you already set it to `true`, util-genai instrumentations now capture
  prompts and responses on spans ([#979](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/979))
- Make LangChain/LangGraph agents evaluable in CloudWatch: set `session.id` on spans from W3C baggage, and set the
  `aws.service.type` resource attribute (plus the AgentCore Runtime identity on AgentCore Runtime) unless already
  set. A CLIENT span with a GenAI inference operation (`chat`, `text_completion`, `generate_content`, `embeddings`)
  that has a nested inference CLIENT span, such as LangChain's `chat` over botocore's, is now INTERNAL, so tokens
  count once; this changes its span kind in queries, dashboards and span-metric series
  ([#991](https://github.com/aws-observability/aws-otel-python-instrumentation/pull/991))
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
