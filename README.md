# AWS Distro for OpenTelemetry - Instrumentation for Python

## Introduction

This project is a redistribution of the [OpenTelemetry Distro for Python](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/opentelemetry-distro),
preconfigured for use with AWS services. Please check out that project too to get a better
understanding of the underlying internals. You won't see much code in this repository since we only
apply some small configuration changes, and our OpenTelemetry friends takes care of the rest. The 
exception to this is support for Application Signals.

We provided a Python agent that can be attached to any application using a supported Python version and dynamically injects
bytecode to capture telemetry from a number of popular libraries and frameworks. The telemetry data
can be exported in a variety of formats. In addition, the agent and exporter can be configured via
command line arguments or environment variables. The net result is the ability to gather telemetry
data from a Python application without any code changes.

## Getting Started

Check out the [getting started documentation](https://aws-otel.github.io/docs/getting-started/python-sdk/auto-instr).

### Additional trace destination

An optional additional OTLP/HTTP protobuf destination can receive application
spans alongside the existing trace exporters. Leave the primary exporter
configuration unchanged and set the following variables before starting the
instrumented process:

```sh
export OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT='https://collector.example.com/v1/traces'
export OTEL_AWS_ADDITIONAL_TRACES_HEADERS='authorization=Bearer%20example-token'
```

The endpoint is the complete traces URL; `/v1/traces` is not appended. The
additional destination does not use AWS SigV4 authentication or inherit primary
OTLP credentials. Header values use the same comma-separated, URL-encoded format
as standard OTLP headers. Protect credentials in deployment configuration and do
not commit them to source control.

| Environment variable | Default | Description |
|---|---|---|
| `OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT` | Unset (disabled) | Full HTTP or HTTPS traces URL. Use HTTPS for remote destinations. |
| `OTEL_AWS_ADDITIONAL_TRACES_HEADERS` | Empty | Headers sent only to the additional destination. |
| `OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT` | `10000` | Positive, finite export timeout in milliseconds, including retries. |
| `OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION` | `none` | `none` or `gzip`. |
| `OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE` | Default TLS trust | Optional CA certificate file for the additional destination. Primary OTLP CA and client certificates are not inherited. |
| `OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED` | `false` | Apply `AWS_REDACT_SPAN_ATTRIBUTES` patterns to the additional copy's span, event and link attributes. |
| `OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED` | `false` | Remove supported GenAI content attributes and content events from the additional copy, without sending removed content to logs. |

The additional destination has its own batch processors and export workers.
It respects the existing sampler and includes recorded, unsampled spans when
the distribution enables their export for agent observability or Lambda
Application Signals. Explicitly disabling primary trace exporters also leaves
the additional destination disabled. Logs and metrics are not duplicated.
Boolean flags accept `true` or `false` (case-insensitive). Invalid additional
endpoint, header, timeout, compression or boolean configuration emits
a warning without logging configured values and leaves primary export intact.

The additional destination snapshots completed spans after span-kind adjustment
and before primary AWS redaction or content extraction. By default, it sends the
recorded attributes and events unchanged, including captured content, even when
primary AWS filtering is configured. Redaction and content removal for this copy
are independent, opt-in settings; neither changes AWS's existing filtering or
content-extraction behavior. Content removal uses the distribution's existing
LLO pattern registry and applies to span attributes and events, not arbitrary
resource, scope or link content.

To enable both options for the additional destination:

```sh
export OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED=true
export OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED=true
```

Redaction uses the configured `AWS_REDACT_SPAN_ATTRIBUTES` patterns; with no
patterns set, it has nothing to redact. These options do not enable content
capture or recover content already omitted by instrumentation. **Enabling the
additional destination with the default filtering settings can export sensitive
recorded content.** Obtain appropriate consent and review the destination's
privacy requirements before enabling export.

This feature requires a distribution version containing the additional-export
support and a process restart or runtime deployment. It does not configure
separate AWS service telemetry or classic Bedrock Agent traces. Delivery is
best-effort: queues are bounded, outages can cause drops, and flush/shutdown
still share the process's time budget. The feature does not provide durable
buffering or an exactly-once delivery guarantee.

## Supported Python libraries and frameworks
For the complete list of supported frameworks, please refer to the [OpenTelemetry for Python documentation](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/README.md).

## Generative AI

As of version `0.20.0`, this distribution officially supports Generative AI
instrumentation for the following frameworks and SDKs:

- [CrewAI](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/crewai/README.rst) (`crewai >= 1.10.0, < 2`)
- [LangChain](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/langchain/README.rst) (`langchain >= 0.3.21, < 2`)
- [LlamaIndex](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/llama_index/README.rst) (`llama-index-core >= 0.13.0, < 1`; `llama-index-workflows >= 1.0.1, != 2.24.0, < 3`)
- [Model Context Protocol (MCP)](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/mcp/README.rst) (`mcp >= 1.10.0, < 2`)
- [OpenAI Agents SDK](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/openai_agents/README.rst) (`openai-agents >= 0.3.3, < 1`)

These instrumentations provide end-to-end visibility into agent applications,
including framework orchestration, model calls, tool invocations, and downstream
dependencies.

### Configuration

<table>
  <thead>
    <tr>
      <th>Environment variable</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>AGENT_OBSERVABILITY_ENABLED</code></td>
      <td>
        <p>Set to <code>true</code> to enable agent-observability defaults. The default is <code>false</code>.</p>
        <p>When enabled, the following environment variable defaults are applied unless you have already configured them:</p>
        <pre><code>OTEL_TRACES_EXPORTER=otlp
OTEL_LOGS_EXPORTER=otlp
OTEL_METRICS_EXPORTER=awsemf
OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://xray.&lt;region&gt;.amazonaws.com/v1/traces
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://logs.&lt;region&gt;.amazonaws.com/v1/logs
OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,urllib3,requests,system_metrics,google-genai,jinja2
OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
OTEL_PYTHON_LOG_CORRELATION=true
OTEL_AWS_APPLICATION_SIGNALS_ENABLED=false
OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS=false
CREWAI_DISABLE_TELEMETRY=true</code></pre>
        <blockquote>
          <p>[!NOTE]</p>
          <p>The trace and log endpoints are configured only when <code>OTEL_EXPORTER_OTLP_ENDPOINT</code> is not set and an AWS Region can be determined.</p>
        </blockquote>
        <br>
      </td>
    </tr>
    <tr>
      <td><code>AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT</code></td>
      <td>
        <p><strong>We recommend setting <code>AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT=true</code> to keep captured content in span attributes.</strong> The current default is <code>false</code>: captured content is removed from span attributes and routed to a separate logs pipeline. If that logs pipeline is disabled, the content is discarded.</p>
        <blockquote>
          <p>[!WARNING]</p>
          <p><strong><code>AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT</code> will be deprecated in a future release.</strong> As we align with the latest OTel GenAI semantic conventions, captured content will remain in span attributes.</p>
        </blockquote>
        <br>
        <blockquote>
          <p>[!NOTE]</p>
          <p>For context about why this environment variable exists, OTel recently replaced per-message events with structured attributes in Semantic Conventions v1.37.0.</p>
          <ul>
            <li><a href="https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.37.0"><code>https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.37.0</code></a></li>
            <li><a href="https://github.com/open-telemetry/semantic-conventions/pull/2179"><code>https://github.com/open-telemetry/semantic-conventions/pull/2179</code></a></li>
          </ul>
        </blockquote>
        <br>
      </td>
    </tr>
    <tr>
      <td><code>AWS_REDACT_SPAN_ATTRIBUTES</code></td>
      <td>
        <p>Your spans may contain sensitive information from LLM interactions, such as your users' prompt data and tool call information; use <code>AWS_REDACT_SPAN_ATTRIBUTES</code> to specify a comma-separated list of span attributes to redact. Matching values in spans, span events, and span links are all replaced with <code>REDACTED</code>. Note that this applies to all span attributes, not just those produced by this distribution's instrumentations.</p>
        <p>Supports wildcard patterns.</p>
        <p><strong>Examples:</strong></p>
        <p>To redact specific sensitive data GenAI attributes:</p>
        <pre><code>export AWS_REDACT_SPAN_ATTRIBUTES='gen_ai.input.messages,gen_ai.output.messages'</code></pre>
        <p>To redact multiple attributes matching a pattern:</p>
        <pre><code>export AWS_REDACT_SPAN_ATTRIBUTES='llm.input_messages.*,llm.output_messages.*'</code></pre>
        <blockquote>
          <p>[!WARNING]</p>
          <p>Redaction occurs in-process within the agent, before telemetry is exported. This may affect other integrations that rely on these attribute values.</p>
        </blockquote>
        <br>
        <blockquote>
          <p>[!NOTE]</p>
          <p>For framework-specific span attributes to redact, refer to the corresponding instrumentation README linked above.</p>
        </blockquote>
        <br>
      </td>
    </tr>
    <tr>
      <td>
        <p><code>AWS_GENAI_INSTRUMENTATION</code></p>
        <br>
        <blockquote>
          <p>[!NOTE]</p>
          <p><code>AWS_AGENTIC_INSTRUMENTATION</code> is the legacy environment variable name and remains supported as a fallback when <code>AWS_GENAI_INSTRUMENTATION</code> is not set.</p>
        </blockquote>
      </td>
      <td>
        <p>Set to <code>disabled</code> to force all of the above instrumentations to remain disabled. Set to <code>enabled</code> to force all of the above instrumentations to load.</p>
        <blockquote>
          <p>[!NOTE]</p>
          <p>When agent observability is enabled (<code>AGENT_OBSERVABILITY_ENABLED=true</code>), instrumentation is skipped when a conflicting third-party instrumentation is detected for the same framework.</p>
          <p>You may set <code>AWS_GENAI_INSTRUMENTATION=disabled</code> to force all of the above instrumentations to remain disabled if you are using another instrumentation source and automatic detection does not work. If another third-party instrumentation is installed, you should uninstall it or otherwise resolve any dependency conflicts before using the above instrumentations.</p>
          <p>You may set <code>AWS_GENAI_INSTRUMENTATION=enabled</code> to force the above instrumentations to load. We recommend that you do not use this setting when third-party instrumentation for the same framework is enabled, because both instrumentations may run and produce duplicate or inconsistent telemetry.</p>
        </blockquote>
        <br>
      </td>
    </tr>
  </tbody>
</table>

## Support

Please note that as per policy, we're providing support via GitHub on a best effort basis. However, if you have AWS Enterprise Support you can create a ticket and we will provide direct support within the respective SLAs.

## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## License

This project is licensed under the Apache-2.0 License.

## Notices

### Python Version Support
This project ensures compatibility with the following supported Python versions: 3.10, 3.11, 3.12, 3.13, 3.14

### Note on Amazon CloudWatch Application Signals
[Amazon CloudWatch Application Signals](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Application-Monitoring-Sections.html) components are designed to seamlessly work with all library instrumentations offered by [OpenTelemetry Python auto-instrumentation](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/README.md). While upstream OpenTelemetry Python instrumentations are in beta, Application Signals components are stable, production ready and have also been tested for popular libraries/frameworks such as [Django, Boto3, and others](https://github.com/aws-observability/aws-otel-python-instrumentation/tree/main/contract-tests/images/applications). We will prioritize backward compatibility for Application Signals components, striving to ensure that they remain functional even in the face of potential breaking changes introduced by OpenTelemetry upstream libraries. Please [raise an issue](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/CONTRIBUTING.md#reporting-bugsfeature-requests) if you notice Application Signals doesn't work for a particular OpenTelemetry supported library.

## Checksum Verification
Artifacts released will include a `.sha256` file for checksum verification starting from v0.7.0
To verify, run the command `shasum -a 256 -c <artifact_name>.sha256` 
It should return the output `<artifact_name>: OK` if the validation is successful
