# cloudwatch-plugin-otel

Amazon CloudWatch plugin for OpenTelemetry.

## Span metrics

The span metrics instrumentor registers a span processor that emits:

- `traces.span.metrics.calls`, a counter (unit `{call}`) incremented once per ended span.
- `traces.span.metrics.duration`, a histogram of span duration in seconds.

Both metrics use the `cloudwatch.plugin.otel.span_metrics` instrumentation
scope. They include `span.name`, `span.kind`, `status.code`, and supported
HTTP, RPC, database, messaging, and error semantic-convention attributes when
those attributes are present on the span. `service.name` is carried by the
metric's resource (the host SDK's resource), not duplicated on each datapoint.

The instrumentor also wraps the active root sampler. A `DROP` decision becomes
`RECORD_ONLY`, allowing the span processor to derive metrics without changing
which spans are exported.

### Usage

Installing this package registers `SpanMetricsInstrumentor` as an
`opentelemetry_instrumentor` entry point. It is loaded automatically when the
application runs with OpenTelemetry auto-instrumentation:

```shell
opentelemetry-instrument python app.py
```

It can also be enabled programmatically:

```python
from plugins.opentelemetry.cloudwatch import SpanMetricsInstrumentor

SpanMetricsInstrumentor().instrument()
```

Pass `tracer_provider` or `meter_provider` to `instrument()` when using
application-managed providers:

```python
SpanMetricsInstrumentor().instrument(
    tracer_provider=tracer_provider,
    meter_provider=meter_provider,
)
```

The connector binds its meter when it is created. Configure the global
`MeterProvider`, or pass `meter_provider`, before instrumenting so the derived
metrics are recorded.

## GenAI observability (LangChain / LangGraph)

The plugin makes the upstream OpenTelemetry LangChain instrumentation produce the agent, chat and tool spans
that the CloudWatch GenAI console and AgentCore Evaluations read.

### Install

```shell
pip install "cloudwatch-plugin-otel[langchain]"
```

The `langchain` extra installs `opentelemetry-instrumentation-genai-langchain` (and, through it,
`opentelemetry-util-genai`), which require OpenTelemetry API/SDK 1.43 or later and instrumentation 0.64b0 or
later. It does not install the `langchain` framework package: a LangGraph application usually needs only
`langchain-core`. The plugin never turns content capture on by itself (see [Content capture](#content-capture)).

### Usage

Run the application under auto-instrumentation, as for span metrics:

```shell
opentelemetry-instrument python app.py
```

The plugin registers `opentelemetry_pre_instrument` and `opentelemetry_post_instrument` hooks, which the
auto-instrumentation loader runs before and after the instrumentors. They never stop auto-instrumentation: a
failure is logged as a `CW-PLUGIN:` warning and the rest carries on. When the GenAI layer is active, it logs one
line at startup, for example:

```
CW-PLUGIN genai: active; content capture=SPAN_ONLY (translated from true); langchain=activated (...)
```

GenAI warnings start with `CW-PLUGIN genai: `. The plugin configures logging only on its GenAI logger
(`plugins.opentelemetry.cloudwatch.genai`), and only when the GenAI layer is not off. It logs at INFO, or at
`OTEL_PYTHON_LOG_LEVEL` (`debug`, `info`, `warning` or `error`) when that is set. `debug` shows why the layer is
active, and `warning` silences the per-process INFO startup line. That variable also affects other OTel
components, such as the basicConfig level of opentelemetry-instrumentation-logging and ADOT's loader logger. The
plugin then adds a stderr handler to the GenAI logger, which stays silent if your app has configured its own
logging handler. Processes where the layer is off see no logging change at all.

### Content capture

The plugin never turns content capture on by itself. ADOT agent mode sets
`OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` by default, which the plugin then honors for util-genai.
Otherwise util-genai's default stands: nothing is captured unless you ask for it. AgentCore Evaluations needs
prompts and responses, so turn capture on with `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` (or
`SPAN_ONLY`). When it is unset, the startup line says so.

| `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` | util-genai uses |
|---|---|
| unset | `NO_CONTENT` (upstream default) |
| `true` (any case) | `SPAN_ONLY` (the plugin's translation, applied only where util-genai itself resolved `NO_CONTENT`) |
| `false` | `NO_CONTENT`, as upstream (util-genai logs its own "not a valid option" warning) |
| `NO_CONTENT`, `SPAN_ONLY`, `EVENT_ONLY` or `SPAN_AND_EVENT` (any case) | that mode, as upstream |
| any other value | `NO_CONTENT`, as upstream (util-genai warns) |

**Why `true` is honored.** The AgentCore documentation and ADOT set `true`, and the botocore and older openai-v2
instrumentations accept only `true`. util-genai accepts only the mode names and reads `true` as no content, so
without the plugin you would get no util-genai content. The translation always asks util-genai first, so it steps
back by itself once util-genai resolves `true`. It is retired when util-genai resolves `true` itself, or when
botocore and openai-v2 accept the mode names and the AgentCore docs and ADOT's agent-mode default switch to
`SPAN_ONLY`.

**`true` or `SPAN_ONLY`?** `true` makes botocore, openai-v2 and (through the plugin) util-genai instrumentations
capture content. `SPAN_ONLY` works for util-genai instrumentations only, and turns botocore content off. The plugin
changes no environment variable. It does not translate `true` while `OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT`
is set, so that name wins once util-genai reads it (util-genai 1.3b0 does not; the startup line notes it).

### LangChain activation

The upstream instrumentor declares `langchain >= 0.3.22, < 2` as its instrumented package, so the loader skips it
when only `langchain-core` is installed
([opentelemetry-python-genai#915](https://github.com/open-telemetry/opentelemetry-python-genai/issues/915)). In
that case the plugin activates it with `LangChainInstrumentor().instrument(skip_dep_check=True)`, called
directly with the global providers (not through the distro's `load_instrumentor`).

It does not do this, and the startup line or a warning says why, when:

- `langchain` is in `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` (or the list is `*`);
- another LangChain instrumentor (OpenInference, ADOT's `aws_langchain`, Traceloop, ...) is already active; the
  startup line then reports content capture as `n/a` for LangChain, because that instrumentor's own settings apply;
- the `langchain` package is installed and the instrumentor is still not active, for example because the version
  is outside that range (a warning);
- `langchain-core` is older than 0.3.49, the version `langchain` 0.3.22 requires (a warning).

ADOT's `AWS_GENAI_INSTRUMENTATION` does not affect this: ADOT documents that it governs only ADOT's own `aws_*`
instrumentors. To opt out of the upstream one, use `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=langchain`.

If the upstream instrumentor and another LangChain instrumentor are both active, every LangChain span is
duplicated, and the plugin warns with a remedy. Dropping the `[langchain]` extra from your requirements does not
remove the package, so the remedies say to uninstall `opentelemetry-instrumentation-genai-langchain` too.

- For ADOT's `aws_langchain` in agent mode: unset the variable you set (`AWS_GENAI_INSTRUMENTATION`, or the legacy
  `AWS_AGENTIC_INSTRUMENTATION`; ADOT's `auto` mode then defers to the upstream instrumentor), or drop the extra
  and uninstall the package. ADOT sets `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` itself in agent mode, so editing it
  is not advised there.
- For `aws_langchain` outside agent mode: add it to `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` (keep any entries
  already there).
- For an instrumentor that shares the name `langchain` (OpenInference, Traceloop): drop the extra and uninstall
  the package, or uninstall the other one.

**ADOT agent mode.** The same activation logic applies; only the duplicate remedy wording differs. ADOT's `auto`
mode skips its own `aws_langchain` whenever any `langchain` entry point is installed, even a disabled one. So with
`opentelemetry-instrumentation-genai-langchain` installed:

- `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=langchain` gives no LangChain spans in ADOT's `auto` mode (ADOT's own
  with `AWS_GENAI_INSTRUMENTATION=enabled`), and also replaces ADOT's default disabled list;
- `CW_PLUGIN_GENAI_ENABLED=false` gives no LangChain spans with only `langchain-core`; with `langchain` installed
  the loader still runs upstream (no content; util-genai warns about `true`), duplicated by ADOT's if `enabled`.

To get ADOT's own LangChain spans instead, uninstall `opentelemetry-instrumentation-genai-langchain`.

### When the GenAI layer is off

The GenAI layer is off when `OTEL_SDK_DISABLED=true`, when `CW_PLUGIN_GENAI_ENABLED=false`, when no supported
GenAI instrumentation is installed, or when `langchain_core` is not importable. Without a supported GenAI
distribution, or with `CW_PLUGIN_GENAI_ENABLED=false` or `OTEL_SDK_DISABLED=true`, the hooks return before
importing the plugin package at all. Span metrics are not affected by any of this.

### Switches

`CW_PLUGIN_GENAI_ENABLED` accepts `true` or `false` (any case) and defaults to `true`; `false` turns off every
GenAI feature of the plugin. Any other value keeps the default and logs a warning. To stop the plugin from
activating the LangChain instrumentation, use the standard `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=langchain`.

### Manual setup

The hooks run only under auto-instrumentation (`opentelemetry-instrument`, or
`opentelemetry.instrumentation.auto_instrumentation.initialize()`). When you set up OpenTelemetry in code, do
what the plugin would do yourself, before instrumenting. The env var applies to the whole process (and to child
processes), unlike the plugin's translation. A `true` you rewrite to `SPAN_ONLY` stops the botocore and
openai-v2 instrumentations, which accept only `true`, from capturing content:

```python
import os

name = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"
# util-genai accepts only mode names: `true` would mean no content.
if os.environ.get(name, "").strip().lower() == "true":
    os.environ[name] = "SPAN_ONLY"

from opentelemetry.instrumentation.genai.langchain import LangChainInstrumentor

# skip_dep_check=True is needed only when the `langchain` package is not installed.
LangChainInstrumentor().instrument(skip_dep_check=True)
```

### Not handled yet

- `session.id` is not set on the spans.
- With Amazon Bedrock models, the botocore instrumentation also records the model call, so token usage can be
  counted twice.
- The CloudWatch console does not identify the service as an agent.
- A LangChain instrumentor that your app starts from code, rather than through auto-instrumentation (for
  example Traceloop's), is not detected. The plugin then activates the upstream one as well, and spans are
  duplicated. Set `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=langchain` (add it to any entries already there).
- The `true` translation replaces `get_content_capturing_mode` in util-genai's own modules (`utils`, `handler`,
  `_invocation`) when the hooks run. Code that imported the function by name before then keeps the original.
- The hook shim is a top-level module copied into the install, also for editable installs: reinstall the package
  after editing `src/cloudwatch_plugin_otel_hooks.py`.
- Installs that ship util-genai without `.py` sources (`.pyc` only) cannot be scanned, so `true` is not
  translated there (one warning; nothing is patched). Set `SPAN_ONLY` instead.

## License

This project is licensed under the Apache-2.0 License. See [LICENSE](./LICENSE).
