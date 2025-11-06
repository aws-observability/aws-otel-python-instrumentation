# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Dict

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_AUTH_CREDENTIAL_PROVIDER,
    AWS_BEDROCK_AGENTCORE_BROWSER_ARN,
    AWS_BEDROCK_AGENTCORE_CODE_INTERPRETER_ARN,
    AWS_BEDROCK_AGENTCORE_GATEWAY_ARN,
    AWS_BEDROCK_AGENTCORE_MEMORY_ARN,
    AWS_BEDROCK_AGENTCORE_RUNTIME_ARN,
    AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN,
    AWS_BEDROCK_AGENTCORE_WORKLOAD_IDENTITY_ARN,
    AWS_GATEWAY_TARGET_ID,
)
from amazon.opentelemetry.distro.patches.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_BROWSER_ID,
    GEN_AI_CODE_INTERPRETER_ID,
    GEN_AI_GATEWAY_ID,
    GEN_AI_MEMORY_ID,
    GEN_AI_RUNTIME_ID,
)
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
    _BotoResultT,
)
from opentelemetry.trace.span import Span

# Mapping of flattened JSON paths to attribute keys
_ATTRIBUTE_MAPPING = {
    "agentRuntimeArn": AWS_BEDROCK_AGENTCORE_RUNTIME_ARN,
    "agentRuntimeEndpointArn": AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN,
    "agentRuntimeId": GEN_AI_RUNTIME_ID,
    "browserArn": AWS_BEDROCK_AGENTCORE_BROWSER_ARN,
    "browserId": GEN_AI_BROWSER_ID,
    "browserIdentifier": GEN_AI_BROWSER_ID,
    "codeInterpreterArn": AWS_BEDROCK_AGENTCORE_CODE_INTERPRETER_ARN,
    "codeInterpreterId": GEN_AI_CODE_INTERPRETER_ID,
    "codeInterpreterIdentifier": GEN_AI_CODE_INTERPRETER_ID,
    "gatewayArn": AWS_BEDROCK_AGENTCORE_GATEWAY_ARN,
    "gatewayId": GEN_AI_GATEWAY_ID,
    "gatewayIdentifier": GEN_AI_GATEWAY_ID,
    "targetId": AWS_GATEWAY_TARGET_ID,
    "memory.arn": AWS_BEDROCK_AGENTCORE_MEMORY_ARN,
    "memory.id": GEN_AI_MEMORY_ID,
    "memoryId": GEN_AI_MEMORY_ID,
    "credentialProviderArn": AWS_AUTH_CREDENTIAL_PROVIDER,
    "resourceCredentialProviderName": AWS_AUTH_CREDENTIAL_PROVIDER,
    "workloadIdentityArn": AWS_BEDROCK_AGENTCORE_WORKLOAD_IDENTITY_ARN,
    "workloadIdentityDetails.workloadIdentityArn": AWS_BEDROCK_AGENTCORE_WORKLOAD_IDENTITY_ARN,
}


class _BedrockAgentCoreExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        extracted_attrs = self._extract_attributes(self._call_context.params)
        attributes.update(extracted_attrs)

    def on_success(
        self,
        span: Span,
        result: _BotoResultT,
        instrumentor_context: _BotocoreInstrumentorContext,
    ):
        if span is None or not span.is_recording():
            return

        extracted_attrs = self._extract_attributes(result)
        for attr_name, attr_value in extracted_attrs.items():
            span.set_attribute(attr_name, attr_value)

    @staticmethod
    def _extract_attributes(params: Dict[str, Any]):
        """Extracts all Bedrock AgentCore attributes using mapping-based traversal"""
        attrs = {}
        for path, attr_key in _ATTRIBUTE_MAPPING.items():
            value = _BedrockAgentCoreExtension._get_nested_value(params, path)
            if value:
                attrs[attr_key] = value
        return attrs

    @staticmethod
    def _get_nested_value(data: Dict[str, Any], path: str):
        """Get value from nested dictionary using dot notation path"""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
