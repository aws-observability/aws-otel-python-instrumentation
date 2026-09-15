# Claude Code Review — Repo-Specific Guidance

This file holds OpenTelemetry-specific review heuristics for
`aws-otel-python-instrumentation`. It is loaded **on demand** by the Claude
Code Review workflow (`.github/workflows/claude-code-review.yml`) — the review
only reads it when a diff touches the areas below. Keep it short; it is a
signal booster, not a second rubric.

## When to load this file

Read this file only if the diff touches any of:

- Span / metric / log **attribute** creation or naming
- **Semantic convention** keys (`gen_ai.*`, `http.*`, `db.*`, resource attrs)
- Instrumentation of a new library, or changes to context propagation
- Sampling, exporters, or anything on a request **hot path**
- GenAI / AI-agent instrumentation

If the diff is pure build/CI/docs/test plumbing with none of the above, skip
this file entirely.

## OTel-specific checks (high signal only)

1. **Semantic conventions over custom names.** New attributes should reuse
   stable OTel semantic-convention keys rather than inventing custom ones.
   Flag custom keys that duplicate an existing convention. For GenAI, preserve
   `gen_ai.provider.name` for the model/provider and use `service.name` (or a
   natively emitted agent attribute) for agent identity — do not write the
   agent name into the provider field.

2. **Cardinality — the Rule of 100.** High-cardinality values (user/request/
   session/trace IDs, raw URLs, unbounded enums — anything likely >100 unique
   values) MUST NOT become **metric** dimensions; they belong on spans or logs.
   Flag any metric attribute that risks time-series explosion.

3. **Component stability.** If the change depends on an Alpha/experimental
   OTel component or convention in a default/production code path, call it out
   and note the stability level.

4. **PII / sensitive data.** Flag capture of request/response bodies, prompt or
   tool content, headers, or credentials into spans/attributes unless there is
   an explicit opt-in and redaction path.

5. **Hot-path overhead.** Instrumentation added to per-request/per-span hot
   paths should avoid unbounded allocation, blocking I/O, or work that runs
   even when tracing is disabled/sampled out.

6. **API/back-compat.** This is a published instrumentation library — flag
   changes that break public API signatures, default behavior, or emitted
   attribute names that downstream dashboards/alerts may depend on.

## Anti-patterns worth a comment

- New custom attribute where a semantic convention already exists.
- Unbounded value used as a metric label.
- Capturing payloads/PII by default.
- Silently changing an emitted attribute name (breaks existing queries).
