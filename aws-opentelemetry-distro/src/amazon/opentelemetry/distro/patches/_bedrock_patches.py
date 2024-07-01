# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import abc
import inspect
import json
from typing import Any, Dict, Optional

from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotoResultT,
)
from opentelemetry.trace.span import Span


class _BedrockAgentOperation(abc.ABC):
    start_attributes: Optional[Dict[str, str]] = None
    response_attributes: Optional[Dict[str, str]] = None

    @classmethod
    @abc.abstractmethod
    def operation_names(cls):
        pass


class _AgentOperation(_BedrockAgentOperation):
    start_attributes = {
        "aws.bedrock.agent_id": "agentId",
    }
    response_attributes = {
        "aws.bedrock.agent_id": "agentId",
    }

    @classmethod
    def operation_names(cls):
        return [
            "CreateAgentActionGroup",
            "CreateAgentAlias",
            "DeleteAgentActionGroup",
            "DeleteAgentAlias",
            "DeleteAgent",
            "DeleteAgentVersion",
            "GetAgentActionGroup",
            "GetAgentAlias",
            "GetAgent",
            "GetAgentVersion",
            "ListAgentActionGroups",
            "ListAgentAliases",
            "ListAgentKnowledgeBases",
            "ListAgentVersions",
            "PrepareAgent",
            "UpdateAgentActionGroup",
            "UpdateAgentAlias",
            "UpdateAgent",
        ]


class _KnowledgeBaseOperation(_BedrockAgentOperation):
    start_attributes = {
        "aws.bedrock.knowledgebase_id": "knowledgeBaseId",
    }
    response_attributes = {}

    @classmethod
    def operation_names(cls):
        return [
            "AssociateAgentKnowledgeBase",
            "CreateDataSource",
            "DeleteKnowledgeBase",
            "DisassociateAgentKnowledgeBase",
            "GetAgentKnowledgeBase",
            "GetKnowledgeBase",
            "ListDataSources",
            "UpdateAgentKnowledgeBase",
        ]


class _DataSourceOperation(_BedrockAgentOperation):
    start_attributes = {
        "aws.bedrock.datasource_id": "dataSourceId",
    }
    response_attributes = {
        "aws.bedrock.datasource_id": "dataSourceId",
    }

    @classmethod
    def operation_names(cls):
        return ["DeleteDataSource", "GetDataSource", "UpdateDataSource"]


_OPERATION_MAPPING = {
    op_name: op_class
    for op_class in [_KnowledgeBaseOperation, _DataSourceOperation, _AgentOperation]
    for op_name in op_class.operation_names()
    if inspect.isclass(op_class) and issubclass(op_class, _BedrockAgentOperation) and not inspect.isabstract(op_class)
}


class _BedrockAgentExtension(_AwsSdkExtension):
    def __init__(self, call_context: _AwsSdkCallContext):
        super().__init__(call_context)
        self._op = _OPERATION_MAPPING.get(call_context.operation)

    def extract_attributes(self, attributes: _AttributeMapT):
        if self._op is None:
            return
        for key, value in self._op.start_attributes.items():
            extracted_value = self._call_context.params.get(value)
            if extracted_value:
                attributes[key] = extracted_value

    def on_success(self, span: Span, result: _BotoResultT):
        if self._op is None:
            return

        for key, value in self._op.response_attributes.items():
            response_value = result.get(value)
            if response_value:
                span.set_attribute(
                    key,
                    response_value,
                )


class _BedrockAgentRuntimeExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        agent_id = self._call_context.params.get("agentId")
        if agent_id:
            attributes["aws.bedrock.agent_id"] = agent_id

        knowledgebase_id = self._call_context.params.get("knowledgeBaseId")
        if knowledgebase_id:
            attributes["aws.bedrock.knowledgebase_id"] = knowledgebase_id


class _BedrockExtension(_AwsSdkExtension):
    # pylint: disable=no-self-use
    def on_success(self, span: Span, result: _BotoResultT):
        # GuardrailId
        guardrail_id = result.get("guardrailId")
        if guardrail_id:
            span.set_attribute(
                "aws.bedrock.guardrail_id",
                guardrail_id,
            )


class BaseBedrockRuntimeModel(abc.ABC):
    @classmethod
    def model_name(cls):
        pass

    @classmethod
    def extract_attributes(cls, context_param: Dict[str, Any], attributes: _AttributeMapT):
        pass

    @classmethod
    def on_success(cls, span: Span, result: _BotoResultT):
        pass


class TitanBedrockRuntimeModel(BaseBedrockRuntimeModel):
    @classmethod
    def model_name(cls):
        return "amazon.titan"

    @classmethod
    def extract_attributes(cls, context_param: Dict[str, Any], attributes: _AttributeMapT):
        if "body" not in context_param:
            return

        text_generation_config = json.loads(context_param.get("body")).get("textGenerationConfig")
        if text_generation_config is None:
            return

        top_p = text_generation_config.get("topP")
        if top_p:
            attributes["gen_ai.request.top_p"] = float(top_p)

        temperature = text_generation_config.get("temperature")
        if temperature:
            attributes["gen_ai.request.temperature"] = float(temperature)

        max_token_count = text_generation_config.get("maxTokenCount")
        if max_token_count:
            attributes["gen_ai.request.max_tokens"] = int(max_token_count)

    @classmethod
    def on_success(cls, span: Span, result: _BotoResultT):
        if ("ResponseMetadata" not in result) or ("HTTPHeaders" not in result["ResponseMetadata"]):
            return

        headers = result["ResponseMetadata"].get("HTTPHeaders")
        input_token_count = headers.get("x-amzn-bedrock-input-token-count")
        if input_token_count:
            span.set_attribute(
                "gen_ai.usage.prompt_tokens",
                int(input_token_count),
            )

        output_token_count = headers.get("x-amzn-bedrock-output-token-count")
        if output_token_count:
            span.set_attribute(
                "gen_ai.usage.completion_tokens",
                int(output_token_count),
            )


class ClaudeBedrockRuntimeModel(BaseBedrockRuntimeModel):
    @classmethod
    def model_name(cls):
        return "anthropic.claude"

    @classmethod
    def extract_attributes(cls, context_param: Dict[str, Any], attributes: _AttributeMapT):
        if "body" not in context_param:
            return

        body = json.loads(context_param.get("body"))
        top_p = body.get("top_p")
        if top_p:
            attributes["gen_ai.request.top_p"] = float(top_p)

        temperature = body.get("temperature")
        if temperature:
            attributes["gen_ai.request.temperature"] = float(temperature)

        max_token_count = body.get("max_tokens_to_sample", body.get("max_tokens"))
        if max_token_count:
            attributes["gen_ai.request.max_tokens"] = int(max_token_count)

    @classmethod
    def on_success(cls, span: Span, result: _BotoResultT):
        if ("ResponseMetadata" not in result) or ("HTTPHeaders" not in result["ResponseMetadata"]):
            return

        headers = result["ResponseMetadata"].get("HTTPHeaders")
        input_token_count = headers.get("x-amzn-bedrock-input-token-count")
        if input_token_count:
            span.set_attribute(
                "gen_ai.usage.prompt_tokens",
                int(input_token_count),
            )

        output_token_count = headers.get("x-amzn-bedrock-output-token-count")
        if output_token_count:
            span.set_attribute(
                "gen_ai.usage.completion_tokens",
                int(output_token_count),
            )


class LlamaBedrockRuntimeModel(BaseBedrockRuntimeModel):
    @classmethod
    def model_name(cls):
        return "meta.llama2"

    @classmethod
    def extract_attributes(cls, context_param: Dict[str, Any], attributes: _AttributeMapT):
        if "body" not in context_param:
            return

        body = json.loads(context_param.get("body"))
        top_p = body.get("top_p")
        if top_p:
            attributes["gen_ai.request.top_p"] = float(top_p)

        temperature = body.get("temperature")
        if temperature:
            attributes["gen_ai.request.temperature"] = float(temperature)

        max_token_count = body.get("max_gen_len")
        if max_token_count:
            attributes["gen_ai.request.max_tokens"] = int(max_token_count)

    @classmethod
    def on_success(cls, span: Span, result: _BotoResultT):
        if ("ResponseMetadata" not in result) or ("HTTPHeaders" not in result["ResponseMetadata"]):
            return

        headers = result["ResponseMetadata"].get("HTTPHeaders")
        input_token_count = headers.get("x-amzn-bedrock-input-token-count")
        if input_token_count:
            span.set_attribute(
                "gen_ai.usage.prompt_tokens",
                int(input_token_count),
            )

        output_token_count = headers.get("x-amzn-bedrock-output-token-count")
        if output_token_count:
            span.set_attribute(
                "gen_ai.usage.completion_tokens",
                int(output_token_count),
            )


_MODEL_MAPPING = {
    md.model_name(): md
    for md in globals().values()
    if inspect.isclass(md) and issubclass(md, BaseBedrockRuntimeModel) and not inspect.isabstract(md)
}


class _BedrockRuntimeExtension(_AwsSdkExtension):
    def __init__(self, call_context: _AwsSdkCallContext):
        super().__init__(call_context)
        self._model_id = call_context.params.get("modelId")
        self._op = call_context.operation

    def extract_attributes(self, attributes: _AttributeMapT):
        attributes["gen_ai.system"] = "AWS Bedrock"

        if self._model_id:
            attributes["gen_ai.request.model"] = self._model_id
            model = _MODEL_MAPPING.get(self._model_id.split("-")[0])
            if model and self._op == "InvokeModel":
                model.extract_attributes(self._call_context.params, attributes)

    def on_success(self, span: Span, result: _BotoResultT):
        if self._model_id:
            model = _MODEL_MAPPING.get(self._model_id.split("-")[0])
            if model and self._op == "InvokeModel":
                model.on_success(span, result)
