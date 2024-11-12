# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch
from io import BytesIO
import json
from botocore.response import StreamingBody

import gevent.monkey
import pkg_resources

from amazon.opentelemetry.distro.patches._instrumentation_patch import (
    AWS_GEVENT_PATCH_MODULES,
    apply_instrumentation_patches,
)
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span

_STREAM_NAME: str = "streamName"
_BUCKET_NAME: str = "bucketName"
_QUEUE_NAME: str = "queueName"
_QUEUE_URL: str = "queueUrl"
_BEDROCK_AGENT_ID: str = "agentId"
_BEDROCK_DATASOURCE_ID: str = "DataSourceId"
_BEDROCK_GUARDRAIL_ID: str = "GuardrailId"
_BEDROCK_KNOWLEDGEBASE_ID: str = "KnowledgeBaseId"
_GEN_AI_SYSTEM: str = "aws_bedrock"
_GEN_AI_REQUEST_MODEL: str = "genAiReuqestModelId"
_SECRET_ARN: str = "arn:aws:secretsmanager:us-west-2:000000000000:secret:testSecret-ABCDEF"
_TOPIC_ARN: str = "topicArn"
_STATE_MACHINE_ARN: str = "arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine"
_ACTIVITY_ARN: str = "arn:aws:states:us-east-1:007003123456789012:activity:testActivity"
_LAMBDA_FUNCTION_NAME: str = "lambdaFunctionName"
_LAMBDA_SOURCE_MAPPING_ID: str = "lambdaEventSourceMappingID"

# Patch names
GET_DISTRIBUTION_PATCH: str = (
    "amazon.opentelemetry.distro.patches._instrumentation_patch.pkg_resources.get_distribution"
)


class TestInstrumentationPatch(TestCase):
    """
    This test class has exactly one test, test_instrumentation_patch. This is an anti-pattern, but the scenario is
    fairly unusual and we feel justifies the code smell. Essentially the _instrumentation_patch module monkey-patches
    upstream components, so once it's run, it's challenging to "undo" between tests. To work around this, we have a
    monolith test framework that tests two major categories of test scenarios:
    1. Patch behaviour
    2. Patch mechanism

    Patch behaviour tests validate upstream behaviour without patches, apply patches, and validate patched behaviour.
    Patch mechanism tests validate the logic that is used to actually apply patches, and can be run regardless of the
    pre- or post-patch behaviour.
    """

    method_patches: Dict[str, patch] = {}
    mock_metric_exporter_init: patch

    def test_instrumentation_patch(self):
        # Set up method patches used by all tests
        self.method_patches[GET_DISTRIBUTION_PATCH] = patch(GET_DISTRIBUTION_PATCH).start()

        # Run tests that validate patch behaviour before and after patching
        self._run_patch_behaviour_tests()
        # Run tests not specifically related to patch behaviour
        self._run_patch_mechanism_tests()

        # Clean up method patches
        for method_patch in self.method_patches.values():
            method_patch.stop()

    def _run_patch_behaviour_tests(self):
        # Test setup
        self.method_patches[GET_DISTRIBUTION_PATCH].return_value = "CorrectDistributionObject"
        # Test setup to not patch gevent
        os.environ[AWS_GEVENT_PATCH_MODULES] = "none"

        # Validate unpatched upstream behaviour - important to detect upstream changes that may break instrumentation
        self._test_unpatched_botocore_instrumentation()
        self._test_unpatched_gevent_instrumentation()

        # Apply patches
        apply_instrumentation_patches()

        # Validate patched upstream behaviour - important to detect downstream changes that may break instrumentation
        self._test_patched_botocore_instrumentation()
        self._test_unpatched_gevent_instrumentation()

        # Test setup to check whether only these two modules get patched by gevent monkey
        os.environ[AWS_GEVENT_PATCH_MODULES] = "os, ssl"

        # Apply patches
        apply_instrumentation_patches()

        # Validate that os and ssl gevent monkey patch modules were patched
        self._test_patched_gevent_os_ssl_instrumentation()

        # Set the value to 'all' so that all the remaining gevent monkey patch modules are patched
        os.environ[AWS_GEVENT_PATCH_MODULES] = "all"

        # Apply patches again.
        apply_instrumentation_patches()

        # Validate that remaining gevent monkey patch modules were patched
        self._test_patched_gevent_instrumentation()

        # Test teardown
        self._reset_mocks()

    def _run_patch_mechanism_tests(self):
        """
        Each test should be invoked, resetting mocks in between each test. E.g.:
            self.test_x()
            self.reset_mocks()
            self.test_y()
            self.reset_mocks()
            etc.
        """
        self._test_botocore_installed_flag()
        self._reset_mocks()

    def _test_unpatched_botocore_instrumentation(self):
        # Kinesis
        self.assertFalse("kinesis" in _KNOWN_EXTENSIONS, "Upstream has added a Kinesis extension")

        # S3
        self.assertFalse("s3" in _KNOWN_EXTENSIONS, "Upstream has added a S3 extension")

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS, "Upstream has removed the SQS extension")
        attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in attributes)
        self.assertFalse("aws.sqs.queue.url" in attributes)
        self.assertFalse("aws.sqs.queue.name" in attributes)

        # Bedrock
        self.assertFalse("bedrock" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock extension")

        # Bedrock Agent
        self.assertFalse("bedrock-agent" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock Agent extension")

        # Bedrock Agent Runtime
        self.assertFalse(
            "bedrock-agent-runtime" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock Agent Runtime extension"
        )

        # BedrockRuntime
        self.assertFalse("bedrock-runtime" in _KNOWN_EXTENSIONS, "Upstream has added a bedrock-runtime extension")

        # SecretsManager
        self.assertFalse("secretsmanager" in _KNOWN_EXTENSIONS, "Upstream has added a SecretsManager extension")

        # SNS
        self.assertTrue("sns" in _KNOWN_EXTENSIONS, "Upstream has removed the SNS extension")

        # StepFunctions
        self.assertFalse("stepfunctions" in _KNOWN_EXTENSIONS, "Upstream has added a StepFunctions extension")

        # Lambda
        self.assertTrue("lambda" in _KNOWN_EXTENSIONS, "Upstream has removed the Lambda extension")

    def _test_unpatched_gevent_instrumentation(self):
        self.assertFalse(gevent.monkey.is_module_patched("os"), "gevent os module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("time"), "gevent time module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("socket"), "gevent socket module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("select"), "gevent select module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("signal"), "gevent signal module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("queue"), "gevent queue module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    # pylint: disable=too-many-statements
    def _test_patched_botocore_instrumentation(self):
        # Kinesis
        self.assertTrue("kinesis" in _KNOWN_EXTENSIONS)
        kinesis_attributes: Dict[str, str] = _do_extract_kinesis_attributes()
        self.assertTrue("aws.kinesis.stream.name" in kinesis_attributes)
        self.assertEqual(kinesis_attributes["aws.kinesis.stream.name"], _STREAM_NAME)

        # S3
        self.assertTrue("s3" in _KNOWN_EXTENSIONS)
        s3_attributes: Dict[str, str] = _do_extract_s3_attributes()
        self.assertTrue(SpanAttributes.AWS_S3_BUCKET in s3_attributes)
        self.assertEqual(s3_attributes[SpanAttributes.AWS_S3_BUCKET], _BUCKET_NAME)

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS)
        sqs_attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in sqs_attributes)
        self.assertTrue("aws.sqs.queue.url" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue.url"], _QUEUE_URL)
        self.assertTrue("aws.sqs.queue.name" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue.name"], _QUEUE_NAME)

        # Bedrock
        self._test_patched_bedrock_instrumentation()

        # Bedrock Agent Operation
        self._test_patched_bedrock_agent_instrumentation()

        # Bedrock Agent Runtime
        self.assertTrue("bedrock-agent-runtime" in _KNOWN_EXTENSIONS)
        bedrock_agent_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock("bedrock-agent-runtime")
        self.assertEqual(len(bedrock_agent_runtime_attributes), 2)
        self.assertEqual(bedrock_agent_runtime_attributes["aws.bedrock.agent.id"], _BEDROCK_AGENT_ID)
        self.assertEqual(bedrock_agent_runtime_attributes["aws.bedrock.knowledge_base.id"], _BEDROCK_KNOWLEDGEBASE_ID)
        bedrock_agent_runtime_sucess_attributes: Dict[str, str] = _do_on_success_bedrock("bedrock-agent-runtime")
        self.assertEqual(len(bedrock_agent_runtime_sucess_attributes), 0)

        # BedrockRuntime - Amazon Titan Models
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        request_body = {
            "textGenerationConfig": {
                "maxTokenCount": 512,
                "temperature":  0.9,
                "topP": 0.75,
            }
        }
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
            "bedrock-runtime", 
            model_id="amazon.titan",
            request_body=json.dumps(request_body)
        )
        self.assertEqual(len(bedrock_runtime_attributes), 5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], "amazon.titan")
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], 512)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], 0.9)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], 0.75)
        response_body = {
            "inputTextTokenCount": 123,
            "results": [{
                "tokenCount": 456,
                "outputText": "testing",
                "completionReason": "FINISH",
            }]
        }
        json_bytes = json.dumps(response_body).encode('utf-8')
        body_bytes = BytesIO(json_bytes)
        streaming_body = StreamingBody(body_bytes, len(json_bytes))
        bedrock_runtime_success_attributes: Dict[str, str] = _do_on_success_bedrock(
            "bedrock-runtime",
            model_id="amazon.titan",
            streaming_body=streaming_body
        )
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.input_tokens"], 123)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.output_tokens"], 456)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.response.finish_reasons"], ["FINISH"])

        #BedrockRuntime - Anthropic Claude Models

        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 512,
            "temperature": 0.5,
            "top_p":0.999,
        }
        
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
            "bedrock-runtime", 
            model_id="anthropic.claude",
            request_body=json.dumps(request_body)
        )
        self.assertEqual(len(bedrock_runtime_attributes), 5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], "anthropic.claude")
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], 512)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], 0.5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], 0.999)
        response_body = {
            'stop_reason': 'end_turn', 'stop_sequence': None, 'usage': {'input_tokens': 23, 'output_tokens': 36}
        }
        json_bytes = json.dumps(response_body).encode('utf-8')
        body_bytes = BytesIO(json_bytes)
        streaming_body = StreamingBody(body_bytes, len(json_bytes))
        bedrock_runtime_success_attributes: Dict[str, str] = _do_on_success_bedrock(
            "bedrock-runtime",
            model_id="anthropic.claude",
            streaming_body=streaming_body
        )
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.input_tokens"], 23)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.output_tokens"], 36)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.response.finish_reasons"], ["end_turn"])

        #BedrockRuntime - Cohere Command Models _testing Pending
        # self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        # request_body = {
        #     "max_tokens": 512,
        #     "temperature": 0.5,
        #     "p":0.75,
        # }
        
        # bedrock_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
        #     "bedrock-runtime", 
        #     model_id="cohere.command",
        #     request_body=json.dumps(request_body)
        # )
        # self.assertEqual(len(bedrock_runtime_attributes), 5)
        # self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        # self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], "cohere.command")
        # self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], 512)
        # self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], 0.5)
        # self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], 0.75)
        # response_body = {
        #     'finish_reason': 'COMPLETE'
        # }
        # json_bytes = json.dumps(response_body).encode('utf-8')
        # body_bytes = BytesIO(json_bytes)
        # streaming_body = StreamingBody(body_bytes, len(json_bytes))
        # bedrock_runtime_success_attributes: Dict[str, str] = _do_on_success_bedrock(
        #     "bedrock-runtime",
        #     model_id="cohere.command",
        #     streaming_body=streaming_body
        # )
        # self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.input_tokens"], 23)
        # self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.output_tokens"], 36)
        # self.assertEqual(bedrock_runtime_success_attributes["gen_ai.response.finish_reasons"], ["COMPLETE"])

        #BedrockRuntime - AI21 Jamba Models
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        request_body = {
            "max_tokens": 512,
            "temperature": 0.5,
            "top_p":0.9,
        }
        
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
            "bedrock-runtime", 
            model_id="ai21.jamba",
            request_body=json.dumps(request_body)
        )
        self.assertEqual(len(bedrock_runtime_attributes), 5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], "ai21.jamba")
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], 512)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], 0.5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], 0.9)
        response_body = {
            'choices':[{'finish_reason': "stop"}],
            'usage': {'prompt_tokens': 24,
                      'completion_tokens': 31,
                      'total_tokens': 55}
        }
        json_bytes = json.dumps(response_body).encode('utf-8')
        body_bytes = BytesIO(json_bytes)
        streaming_body = StreamingBody(body_bytes, len(json_bytes))
        bedrock_runtime_success_attributes: Dict[str, str] = _do_on_success_bedrock(
            "bedrock-runtime",
            model_id="ai21.jamba",
            streaming_body=streaming_body
        )
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.input_tokens"], 24)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.output_tokens"], 31)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.response.finish_reasons"], ["stop"])

        #BedrockRuntime - Meta LLama Models 
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        request_body = {
            "max_gen_len": 512,
            "temperature": 0.5,
            "top_p":0.9,
        }
        
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
            "bedrock-runtime", 
            model_id="meta.llama",
            request_body=json.dumps(request_body)
        )
        self.assertEqual(len(bedrock_runtime_attributes), 5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], "meta.llama")
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], 512)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], 0.5)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], 0.9)
        response_body = {
            'prompt_token_count': 31, 
            'generation_token_count': 36, 
            'stop_reason': 'stop'
        }
        json_bytes = json.dumps(response_body).encode('utf-8')
        body_bytes = BytesIO(json_bytes)
        streaming_body = StreamingBody(body_bytes, len(json_bytes))
        bedrock_runtime_success_attributes: Dict[str, str] = _do_on_success_bedrock(
            "bedrock-runtime",
            model_id="meta.llama",
            streaming_body=streaming_body
        )
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.input_tokens"], 31)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.usage.output_tokens"], 36)
        self.assertEqual(bedrock_runtime_success_attributes["gen_ai.response.finish_reasons"], ["stop"])


        # SecretsManager
        self.assertTrue("secretsmanager" in _KNOWN_EXTENSIONS)
        secretsmanager_attributes: Dict[str, str] = _do_extract_secretsmanager_attributes()
        self.assertTrue("aws.secretsmanager.secret.arn" in secretsmanager_attributes)
        self.assertEqual(secretsmanager_attributes["aws.secretsmanager.secret.arn"], _SECRET_ARN)
        secretsmanager_success_attributes: Dict[str, str] = _do_on_success_secretsmanager()
        self.assertTrue("aws.secretsmanager.secret.arn" in secretsmanager_success_attributes)
        self.assertEqual(secretsmanager_success_attributes["aws.secretsmanager.secret.arn"], _SECRET_ARN)

        # SNS
        self.assertTrue("sns" in _KNOWN_EXTENSIONS)
        sns_attributes: Dict[str, str] = _do_extract_sns_attributes()
        self.assertTrue("aws.sns.topic.arn" in sns_attributes)
        self.assertEqual(sns_attributes["aws.sns.topic.arn"], _TOPIC_ARN)

        # StepFunctions
        self.assertTrue("stepfunctions" in _KNOWN_EXTENSIONS)
        stepfunctions_attributes: Dict[str, str] = _do_extract_stepfunctions_attributes()
        self.assertTrue("aws.stepfunctions.state_machine.arn" in stepfunctions_attributes)
        self.assertEqual(stepfunctions_attributes["aws.stepfunctions.state_machine.arn"], _STATE_MACHINE_ARN)
        self.assertTrue("aws.stepfunctions.activity.arn" in stepfunctions_attributes)
        self.assertEqual(stepfunctions_attributes["aws.stepfunctions.activity.arn"], _ACTIVITY_ARN)

        # Lambda
        self.assertTrue("lambda" in _KNOWN_EXTENSIONS)
        lambda_attributes: Dict[str, str] = _do_extract_lambda_attributes()
        self.assertTrue("aws.lambda.function.name" in lambda_attributes)
        self.assertEqual(lambda_attributes["aws.lambda.function.name"], _LAMBDA_FUNCTION_NAME)
        self.assertTrue("aws.lambda.resource_mapping.id" in lambda_attributes)
        self.assertEqual(lambda_attributes["aws.lambda.resource_mapping.id"], _LAMBDA_SOURCE_MAPPING_ID)

    def _test_patched_gevent_os_ssl_instrumentation(self):
        # Only ssl and os module should have been patched since the environment variable was set to 'os, ssl'
        self.assertTrue(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("os"), "gevent os module has not been patched")
        # Rest should still be unpatched
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("time"), "gevent time module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("socket"), "gevent socket module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("select"), "gevent select module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("signal"), "gevent signal module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("queue"), "gevent queue module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    def _test_patched_gevent_instrumentation(self):
        self.assertTrue(gevent.monkey.is_module_patched("os"), "gevent os module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("time"), "gevent time module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("socket"), "gevent socket module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("select"), "gevent select module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("signal"), "gevent signal module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("queue"), "gevent queue module has not been patched")

        # Current version of gevent.monkey.patch_all() does not do anything to these modules despite being called
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has  been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module not been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    def _test_botocore_installed_flag(self):
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            get_distribution_patch: patch = self.method_patches[GET_DISTRIBUTION_PATCH]
            get_distribution_patch.side_effect = pkg_resources.DistributionNotFound
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = pkg_resources.VersionConflict("botocore==1.0.0", "botocore==0.0.1")
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = None
            get_distribution_patch.return_value = "CorrectDistributionObject"
            apply_instrumentation_patches()
            mock_apply_patches.assert_called()

    def _test_patched_bedrock_instrumentation(self):
        """For bedrock service, only on_success provides attributes, and we only expect to see guardrail"""
        bedrock_sucess_attributes: Dict[str, str] = _do_on_success_bedrock("bedrock")
        self.assertEqual(len(bedrock_sucess_attributes), 1)
        self.assertEqual(bedrock_sucess_attributes["aws.bedrock.guardrail.id"], _BEDROCK_GUARDRAIL_ID)

    def _test_patched_bedrock_agent_instrumentation(self):
        """For bedrock-agent service, both extract_attributes and on_success provides attributes,
        the attributes depend on the API being invoked."""
        self.assertTrue("bedrock-agent" in _KNOWN_EXTENSIONS)
        operation_to_expected_attribute = {
            "CreateAgentActionGroup": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "CreateAgentAlias": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "DeleteAgentActionGroup": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "DeleteAgentAlias": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "DeleteAgent": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "DeleteAgentVersion": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "GetAgentActionGroup": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "GetAgentAlias": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "GetAgent": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "GetAgentVersion": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "ListAgentActionGroups": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "ListAgentAliases": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "ListAgentKnowledgeBases": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "ListAgentVersions": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "PrepareAgent": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "UpdateAgentActionGroup": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "UpdateAgentAlias": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "UpdateAgent": ("aws.bedrock.agent.id", _BEDROCK_AGENT_ID),
            "AssociateAgentKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "CreateDataSource": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "DeleteKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "DisassociateAgentKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "GetAgentKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "GetKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "ListDataSources": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "UpdateAgentKnowledgeBase": ("aws.bedrock.knowledge_base.id", _BEDROCK_KNOWLEDGEBASE_ID),
            "DeleteDataSource": ("aws.bedrock.data_source.id", _BEDROCK_DATASOURCE_ID),
            "GetDataSource": ("aws.bedrock.data_source.id", _BEDROCK_DATASOURCE_ID),
            "UpdateDataSource": ("aws.bedrock.data_source.id", _BEDROCK_DATASOURCE_ID),
        }

        data_source_operations = ["DeleteDataSource", "GetDataSource", "UpdateDataSource"]

        for operation, attribute_tuple in operation_to_expected_attribute.items():
            bedrock_agent_extract_attributes: Dict[str, str] = _do_extract_attributes_bedrock(
                "bedrock-agent", operation
            )

            if operation in data_source_operations:
                self.assertEqual(len(bedrock_agent_extract_attributes), 2)
                self.assertEqual(bedrock_agent_extract_attributes[attribute_tuple[0]], attribute_tuple[1])
                self.assertEqual(
                    bedrock_agent_extract_attributes["aws.bedrock.knowledge_base.id"], _BEDROCK_KNOWLEDGEBASE_ID
                )
            else:
                self.assertEqual(len(bedrock_agent_extract_attributes), 1)
                self.assertEqual(bedrock_agent_extract_attributes[attribute_tuple[0]], attribute_tuple[1])

            bedrock_agent_success_attributes: Dict[str, str] = _do_on_success_bedrock("bedrock-agent", operation)
            self.assertEqual(len(bedrock_agent_success_attributes), 1)
            self.assertEqual(bedrock_agent_success_attributes[attribute_tuple[0]], attribute_tuple[1])

    def _reset_mocks(self):
        for method_patch in self.method_patches.values():
            method_patch.reset_mock()


def _do_extract_kinesis_attributes() -> Dict[str, str]:
    service_name: str = "kinesis"
    params: Dict[str, str] = {"StreamName": _STREAM_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_s3_attributes() -> Dict[str, str]:
    service_name: str = "s3"
    params: Dict[str, str] = {"Bucket": _BUCKET_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_sqs_attributes() -> Dict[str, str]:
    service_name: str = "sqs"
    params: Dict[str, str] = {"QueueUrl": _QUEUE_URL, "QueueName": _QUEUE_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_attributes_bedrock(service, operation=None, model_id=None, request_body=None) -> Dict[str, str]:
    params: Dict[str, Any] = {
        "agentId": _BEDROCK_AGENT_ID,
        "dataSourceId": _BEDROCK_DATASOURCE_ID,
        "knowledgeBaseId": _BEDROCK_KNOWLEDGEBASE_ID,
        "guardrailId": _BEDROCK_GUARDRAIL_ID,
        "modelId": model_id,
        "body": request_body,
    }
    return _do_extract_attributes(service, params, operation)


def _do_on_success_bedrock(service, operation=None, model_id=None, streaming_body=None) -> Dict[str, str]:
    result: Dict[str, Any] = {
        "agentId": _BEDROCK_AGENT_ID,
        "dataSourceId": _BEDROCK_DATASOURCE_ID,
        "knowledgeBaseId": _BEDROCK_KNOWLEDGEBASE_ID,
        "guardrailId": _BEDROCK_GUARDRAIL_ID,
        "body": streaming_body,
    }
    return _do_on_success(service, result, operation, params={"modelId": model_id})


def _do_extract_secretsmanager_attributes() -> Dict[str, str]:
    service_name: str = "secretsmanager"
    params: Dict[str, str] = {"SecretId": _SECRET_ARN}
    return _do_extract_attributes(service_name, params)


def _do_on_success_secretsmanager() -> Dict[str, str]:
    service_name: str = "secretsmanager"
    result: Dict[str, Any] = {"ARN": _SECRET_ARN}
    return _do_on_success(service_name, result)


def _do_extract_sns_attributes() -> Dict[str, str]:
    service_name: str = "sns"
    params: Dict[str, str] = {"TopicArn": _TOPIC_ARN}
    return _do_extract_attributes(service_name, params)


def _do_extract_stepfunctions_attributes() -> Dict[str, str]:
    service_name: str = "stepfunctions"
    params: Dict[str, str] = {"stateMachineArn": _STATE_MACHINE_ARN, "activityArn": _ACTIVITY_ARN}
    return _do_extract_attributes(service_name, params)


def _do_extract_lambda_attributes() -> Dict[str, str]:
    service_name: str = "lambda"
    params: Dict[str, str] = {"FunctionName": _LAMBDA_FUNCTION_NAME, "UUID": _LAMBDA_SOURCE_MAPPING_ID}
    return _do_extract_attributes(service_name, params)


def _do_extract_attributes(service_name: str, params: Dict[str, Any], operation: str = None) -> Dict[str, str]:
    mock_call_context: MagicMock = MagicMock()
    mock_call_context.params = params
    if operation:
        mock_call_context.operation = operation
    attributes: Dict[str, str] = {}
    sqs_extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    sqs_extension.extract_attributes(attributes)
    return attributes


def _do_on_success(
    service_name: str, result: Dict[str, Any], operation: str = None, params: Dict[str, Any] = None
) -> Dict[str, str]:
    span_mock: Span = MagicMock()
    mock_call_context = MagicMock()
    span_attributes: Dict[str, str] = {}

    def set_side_effect(set_key, set_value):
        span_attributes[set_key] = set_value

    span_mock.set_attribute.side_effect = set_side_effect

    if operation:
        mock_call_context.operation = operation

    if params:
        mock_call_context.params = params

    extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    extension.on_success(span_mock, result)

    return span_attributes
