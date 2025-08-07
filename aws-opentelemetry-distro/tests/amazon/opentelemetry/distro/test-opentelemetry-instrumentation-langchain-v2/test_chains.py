# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import ast
import os

import boto3
import pytest
from botocore.exceptions import ClientError, NoCredentialsError
from langchain.chains import LLMChain, SequentialChain
from langchain.prompts import PromptTemplate
from langchain_aws import BedrockLLM

from opentelemetry.trace import SpanKind


def has_aws_credentials():
    """Check if AWS credentials are available."""
    # Check for environment variables first
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True

    # Try to create a boto3 client and make a simple call
    try:
        # Using STS for a lightweight validation
        sts = boto3.client("sts")
        sts.get_caller_identity()
        return True
    except (NoCredentialsError, ClientError):
        return False


aws_credentials_required = pytest.mark.skipif(
    not has_aws_credentials(), reason="AWS credentials not available for testing"
)


def create_bedrock_llm(region="us-west-2"):
    """Create and return a BedrockLLM instance."""
    session = boto3.Session(region_name=region)
    bedrock_client = session.client(service_name="bedrock-runtime", region_name=region)
    return BedrockLLM(
        client=bedrock_client,
        model_id="anthropic.claude-v2",
        model_kwargs={"max_tokens_to_sample": 500, "temperature": 0.7},
    )


def create_chains(llm):
    """Create and return the sequential chain."""
    synopsis_prompt = PromptTemplate(
        input_variables=["title", "era"],
        template="""You are a playwright. Given the title of play and the era it is set in, it is your job to write a synopsis for that title.

    Title: {title}
    Era: {era}
    Playwright: This is a synopsis for the above play:""",  # noqa: E501
    )

    review_prompt = PromptTemplate(
        input_variables=["synopsis"],
        template="""You are a play critic from the New York Times. Given the synopsis of play, it is your job to write a review for that play.

    Play Synopsis:
    {synopsis}
    Review from a New York Times play critic of the above play:""",  # noqa: E501
    )

    return SequentialChain(
        chains=[
            LLMChain(llm=llm, prompt=synopsis_prompt, output_key="synopsis", name="synopsis"),
            LLMChain(llm=llm, prompt=review_prompt, output_key="review"),
        ],
        input_variables=["era", "title"],
        output_variables=["synopsis", "review"],
        verbose=True,
    )


@aws_credentials_required
@pytest.mark.vcr(filter_headers=["Authorization", "X-Amz-Date", "X-Amz-Security-Token"], record_mode="once")
def test_sequential_chain(instrument_langchain, span_exporter):
    span_exporter.clear()

    input_data = {"title": "Tragedy at sunset on the beach", "era": "Victorian England"}
    create_chains(create_bedrock_llm()).invoke(input_data)

    spans = span_exporter.get_finished_spans()
    synopsis_span = next(span for span in spans if span.name == "chain synopsis")
    review_span = next(span for span in spans if span.name == "chain LLMChain")
    overall_span = next(span for span in spans if span.name == "chain SequentialChain")

    assert ["chain synopsis", "chain LLMChain", "chain SequentialChain"] == [
        span.name for span in spans if span.name.startswith("chain ")
    ]

    for span in [synopsis_span, review_span, overall_span]:
        assert span.kind == SpanKind.INTERNAL
        assert "gen_ai.prompt" in span.attributes
        assert "gen_ai.completion" in span.attributes

    synopsis_data = (
        ast.literal_eval(synopsis_span.attributes["gen_ai.prompt"]),
        ast.literal_eval(synopsis_span.attributes["gen_ai.completion"]),
    )
    assert synopsis_data[0] == input_data
    assert "synopsis" in synopsis_data[1]

    review_data = (
        ast.literal_eval(review_span.attributes["gen_ai.prompt"]),
        ast.literal_eval(review_span.attributes["gen_ai.completion"]),
    )
    assert all(key in review_data[0] for key in ["title", "era", "synopsis"])
    assert "review" in review_data[1]

    overall_data = (
        ast.literal_eval(overall_span.attributes["gen_ai.prompt"]),
        ast.literal_eval(overall_span.attributes["gen_ai.completion"]),
    )
    assert overall_data[0] == input_data
    assert all(key in overall_data[1] for key in ["synopsis", "review"])
