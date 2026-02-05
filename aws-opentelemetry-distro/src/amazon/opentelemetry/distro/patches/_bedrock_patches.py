# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import abc
import inspect
import logging
from typing import Dict, Optional

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_BEDROCK_AGENT_ID,
    AWS_BEDROCK_DATA_SOURCE_ID,
    AWS_BEDROCK_GUARDRAIL_ARN,
    AWS_BEDROCK_GUARDRAIL_ID,
    AWS_BEDROCK_KNOWLEDGE_BASE_ID,
)
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
    _BotoResultT,
)
from opentelemetry.trace.span import Span

_AGENT_ID: str = "agentId"
_KNOWLEDGE_BASE_ID: str = "knowledgeBaseId"
_DATA_SOURCE_ID: str = "dataSourceId"
_GUARDRAIL_ID: str = "guardrailId"
_GUARDRAIL_ARN: str = "guardrailArn"
_MODEL_ID: str = "modelId"
_AWS_BEDROCK_SYSTEM: str = "aws.bedrock"

_logger = logging.getLogger(__name__)
# Set logger level to DEBUG
_logger.setLevel(logging.DEBUG)


class _BedrockAgentOperation(abc.ABC):
    """
    We use subclasses and operation names to handle specific Bedrock Agent operations.
    - Only operations involving Agent, DataSource, or KnowledgeBase resources are supported.
    - Operations without these specified resources are not covered.
    - When an operation involves multiple resources (e.g., AssociateAgentKnowledgeBase),
      we map it to one resource based on some judgement classification of rules.

    For detailed API documentation on Bedrock Agent operations, visit:
    https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Agents_for_Amazon_Bedrock.html
    """

    request_attributes: Optional[Dict[str, str]] = None
    response_attributes: Optional[Dict[str, str]] = None

    @classmethod
    @abc.abstractmethod
    def operation_names(cls):
        pass


class _AgentOperation(_BedrockAgentOperation):
    """
    This class covers BedrockAgent API related to <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_Agent.html">Agents</a>,
    and extracts agent-related attributes.
    """

    request_attributes = {
        AWS_BEDROCK_AGENT_ID: _AGENT_ID,
    }
    response_attributes = {
        AWS_BEDROCK_AGENT_ID: _AGENT_ID,
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
    """
    This class covers BedrockAgent API related to <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_KnowledgeBase.html">KnowledgeBases</a>,
    and extracts knowledge base-related attributes.

    Note: The 'CreateDataSource' operation does not have a 'dataSourceId' in the context,
    but it always comes with a 'knowledgeBaseId'. Therefore, we categorize it under 'knowledgeBaseId' operations.
    """

    request_attributes = {
        AWS_BEDROCK_KNOWLEDGE_BASE_ID: _KNOWLEDGE_BASE_ID,
    }
    response_attributes = {
        AWS_BEDROCK_KNOWLEDGE_BASE_ID: _KNOWLEDGE_BASE_ID,
    }

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
    """
    This class covers BedrockAgent API related to <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_DataSource.html">DataSources</a>,
    and extracts data source-related attributes.
    """

    request_attributes = {
        AWS_BEDROCK_KNOWLEDGE_BASE_ID: _KNOWLEDGE_BASE_ID,
        AWS_BEDROCK_DATA_SOURCE_ID: _DATA_SOURCE_ID,
    }
    response_attributes = {
        AWS_BEDROCK_DATA_SOURCE_ID: _DATA_SOURCE_ID,
    }

    @classmethod
    def operation_names(cls):
        return ["DeleteDataSource", "GetDataSource", "UpdateDataSource"]


# _OPERATION_NAME_TO_CLASS_MAPPING maps operation names to their corresponding classes
# by iterating over all subclasses of _BedrockAgentOperation and extract operations
# by calling operation_names() function.
_OPERATION_NAME_TO_CLASS_MAPPING = {
    op_name: op_class
    for op_class in [_KnowledgeBaseOperation, _DataSourceOperation, _AgentOperation]
    for op_name in op_class.operation_names()
    if inspect.isclass(op_class) and issubclass(op_class, _BedrockAgentOperation) and not inspect.isabstract(op_class)
}


class _BedrockAgentExtension(_AwsSdkExtension):
    """
    This class is an extension for <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Agents_for_Amazon_Bedrock.html">
    Agents for Amazon Bedrock</a>.

    This class primarily identify three types of resource based operations: _AgentOperation, _KnowledgeBaseOperation,
    and _DataSourceOperation. We only support operations that are related to the resource
    and where the context contains the resource ID.
    """

    def __init__(self, call_context: _AwsSdkCallContext):
        super().__init__(call_context)
        self._operation_class = _OPERATION_NAME_TO_CLASS_MAPPING.get(call_context.operation)

    def extract_attributes(self, attributes: _AttributeMapT):
        if self._operation_class is None:
            return
        for attribute_key, request_param_key in self._operation_class.request_attributes.items():
            request_param_value = self._call_context.params.get(request_param_key)
            if request_param_value:
                attributes[attribute_key] = request_param_value

    def on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        if self._operation_class is None:
            return

        for attribute_key, response_param_key in self._operation_class.response_attributes.items():
            response_param_value = result.get(response_param_key)
            if response_param_value:
                span.set_attribute(
                    attribute_key,
                    response_param_value,
                )


class _BedrockAgentRuntimeExtension(_AwsSdkExtension):
    """
    This class is an extension for <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Agents_for_Amazon_Bedrock_Runtime.html">
    Agents for Amazon Bedrock Runtime</a>.
    """

    def extract_attributes(self, attributes: _AttributeMapT):
        agent_id = self._call_context.params.get(_AGENT_ID)
        if agent_id:
            attributes[AWS_BEDROCK_AGENT_ID] = agent_id

        knowledge_base_id = self._call_context.params.get(_KNOWLEDGE_BASE_ID)
        if knowledge_base_id:
            attributes[AWS_BEDROCK_KNOWLEDGE_BASE_ID] = knowledge_base_id


class _BedrockExtension(_AwsSdkExtension):
    """
    This class is an extension for <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Amazon_Bedrock.html">Bedrock</a>.
    """

    # pylint: disable=no-self-use
    def on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        # _GUARDRAIL_ID can only be retrieved from the response, not from the request
        guardrail_id = result.get(_GUARDRAIL_ID)
        if guardrail_id:
            span.set_attribute(
                AWS_BEDROCK_GUARDRAIL_ID,
                guardrail_id,
            )

        guardrail_arn = result.get(_GUARDRAIL_ARN)
        if guardrail_arn:
            span.set_attribute(
                AWS_BEDROCK_GUARDRAIL_ARN,
                guardrail_arn,
            )
