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

## Supported Python libraries and frameworks
For the complete list of supported frameworks, please refer to the [OpenTelemetry for Python documentation](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/README.md).

## Generative AI

As of version `0.20.0`, this distribution officially supports Generative AI
instrumentation for the following frameworks and SDKs:

- [CrewAI](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/crewai/README.rst) (`crewai >= 1.10.0, < 2`)
- [LangChain](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/langchain/README.rst) (`langchain >= 0.3.21, < 2`)
- [LlamaIndex](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/instrumentation/llama_index/README.rst) (`llama-index-core >= 0.13.0, < 1`)
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
        Set to <code>true</code> to enable agent-observability defaults. The default is <code>false</code>.
        <br>
      </td>
    </tr>
    <tr>
      <td><code>AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT</code></td>
      <td>
        <p><strong>We strongly recommend setting this variable to <code>true</code> to keep captured content in span attributes.</strong> The current default is <code>false</code>: captured content is removed from span attributes and routed to a separate logs pipeline. If that logs pipeline is disabled, the content is discarded.</p>
        <blockquote>
          <p>[!NOTE]</p>
          <p>In a future release, routing captured content to the separate logs pipeline will become opt-in.</p>
        </blockquote>
        <br>
      </td>
    </tr>
    <tr>
      <td><code>AWS_REDACT_SPAN_ATTRIBUTES</code></td>
      <td>
        <p>A comma separated list of span attributes to redact. Matching values in spans, span events, and span links are all replaced with <code>REDACTED</code>. Note that this applies to all span attributes, not just those produced by this distribution's instrumentations.</p>
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
      </td>
    </tr>
    <tr>
      <td>
        <p><code>ADOT_GENAI_INSTRUMENTATION</code></p>
        <br>
        <blockquote>
          <p>[!NOTE]</p>
          <p><code>AWS_AGENTIC_INSTRUMENTATION</code> is the legacy environment variable name and remains supported as a fallback when <code>ADOT_GENAI_INSTRUMENTATION</code> is not set.</p>
        </blockquote>
      </td>
      <td>
        <p>Set to <code>disabled</code> to disable all of the above instrumentations. Set to <code>enabled</code> to force all of the above instrumentations to load.</p>
        <blockquote>
          <p>[!NOTE]</p>
          <p>When agent observability is enabled (<code>AGENT_OBSERVABILITY_ENABLED=true</code>), instrumentation is skipped when a conflicting third-party instrumentation is detected for the same framework.</p>
          <p>You may set <code>ADOT_GENAI_INSTRUMENTATION=disabled</code> to disable all of the above instrumentations if you are using another instrumentation source and automatic detection does not work. If another third-party instrumentation is installed, you should uninstall it or otherwise resolve any dependency conflicts before using the above instrumentations.</p>
          <p>You may set <code>ADOT_GENAI_INSTRUMENTATION=enabled</code> to force the above instrumentations to load. We recommend that you do not use this setting because both instrumentations may run and produce duplicate or inconsistent telemetry.</p>
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
