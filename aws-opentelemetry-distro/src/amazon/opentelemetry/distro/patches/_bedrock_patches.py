# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import abc
import inspect
from typing import Any, Dict

from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotoResultT,
)
from opentelemetry.trace.span import Span


class _BedrockAgentOperation(abc.ABC):
    start_attributes: Optional[Dict[str, _AttrSpecT]] = None
    response_attributes: Optional[Dict[str, _AttrSpecT]] = None

    @classmethod
    @abc.abstractmethod
    def operation_names(cls):
        pass

class _AgentOperation(_BedrockAgentOperation):
    # AgentId Operations !!! do both request and response
    # AssociateAgentKnowledgeBase -> KnowledgeBaseId
    # DisassociateAgentKnowledgeBase -> KnowledgeBaseId
    # UpdateAgentKnowledgeBase -> KnowledgeBaseId
    # GetAgentKnowledgeBase -> KnowledgeBaseId
    start_attributes = {
        "aws.bedrock.agent_id": "AgentId",
    }
    response_attributes = {
        "aws.bedrock.agent_id": "AgentId",
    }

    @classmethod
    @abc.abstractmethod
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
            "UpdateAgent"
        ]


class _KnowledgeBaseOperation(_BedrockAgentOperation):
    # KnowledgeId !!! only do request
    # UpdateDataSource -> DataSourceId
    # GetDataSource -> DataSourceId
    # DeleteDataSource -> DataSourceId
    # GetIngestionJob -> not support
    # ListIngestionJobs -> not support
    # StartIngestionJob -> not support
    start_attributes = {
        "aws.bedrock.knowledgebase_id": "KnowledgeBaseId",
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
            "UpdateAgentKnowledgeBase"
        ]


class _DataSourceOperation(_BedrockAgentOperation):
    # DataSourceId !!! do both request and response
    # GetIngestionJob -> not support
    # ListIngestionJobs -> not support
    # StartIngestionJob -> not support
    start_attributes = {
        "aws.bedrock.datasource_id": "DataSourceId",
    }
    response_attributes = {
        "aws.bedrock.datasource_id": "DataSourceId",
    }

    @classmethod
    def operation_names(cls):
        return [
            "DeleteDataSource",
            "GetDataSource",
            "UpdateDataSource"
        ]

_OPERATION_MAPPING = {
    op_name: op_class
    for op_class in [_KnowledgeBaseOperation, _DataSourceOperation]
    for op_name in op_class.operation_names()
    if inspect.isclass(op_class)
    and issubclass(op_class, _BedrockAgentOperation)
    and not inspect.isabstract(op_class)
}


class _BedrockAgentExtension(_AwsSdkExtension): # -> AgentId, KnowledgeId, DataSourceId
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
                    value,
                )


class _BedrockAgentRuntimeExtension(_AwsSdkExtension):# -> AgentId, KnowledgebaseId  -> no overlap
    def extract_attributes(self, attributes: _AttributeMapT):
        # AgentId, KnowledgebaseId
        agent_id = self._call_context.params.get("AgentId")
        if agent_id:
            attributes["aws.bedrock.agent_id"] = agent_id

        knowledgebase_id = self._call_context.params.get("KnowledgeBaseId")
        if knowledgebase_id:
            attributes["aws.bedrock.knowledgebase_id"] = knowledgebase_id


class _BedrockExtension(_AwsSdkExtension): # -> ModelId, GaurdrailId -> no overlap
    def extract_attributes(self, attributes: _AttributeMapT):
        # ModelId
        model_id = self._call_context.params.get("ModelId")
        if model_id:
            attributes["aws.bedrock.model_id"] = model_id

    def on_success(self, span: Span, result: _BotoResultT):
        # GuardrailId
        gaurdrail_id = result.get("GuardrailId")
        if gaurdrail_id:
            span.set_attribute(
                "aws.bedrock.guardrail_id",
                gaurdrail_id,
            )

