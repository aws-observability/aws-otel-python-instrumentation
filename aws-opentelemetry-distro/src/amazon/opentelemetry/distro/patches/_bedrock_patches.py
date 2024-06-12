# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import abc
import inspect
import json
from typing import Any, Dict

from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotoResultT,
)
from opentelemetry.trace.span import Span


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
