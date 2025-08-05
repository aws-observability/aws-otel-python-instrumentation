# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import ast

import boto3
import pytest
from langchain.chains import LLMChain, SequentialChain
from langchain.prompts import PromptTemplate
from langchain_aws import BedrockLLM

from opentelemetry.trace import SpanKind


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
    """Create and return the synopsis chain, review chain, and overall chain."""

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

    overall_chain = SequentialChain(
        chains=[
            LLMChain(llm=llm, prompt=synopsis_prompt, output_key="synopsis", name="synopsis"),
            LLMChain(llm=llm, prompt=review_prompt, output_key="review"),
        ],
        input_variables=["era", "title"],
        output_variables=["synopsis", "review"],
        verbose=True,
    )

    return overall_chain


def validate_span(span, expected_kind, expected_attrs):
    """Validate a span against expected values."""
    assert span.kind == expected_kind
    for attr in expected_attrs:
        assert attr in span.attributes
    return ast.literal_eval(span.attributes["gen_ai.prompt"]), ast.literal_eval(span.attributes["gen_ai.completion"])


@pytest.mark.vcr(filter_headers=["Authorization", "X-Amz-Date", "X-Amz-Security-Token"], record_mode="once")
def test_sequential_chain(instrument_langchain, span_exporter):
    span_exporter.clear()

    llm = create_bedrock_llm()
    chain = create_chains(llm)
    input_data = {"title": "Tragedy at sunset on the beach", "era": "Victorian England"}
    chain.invoke(input_data)

    spans = span_exporter.get_finished_spans()
    langchain_spans = [span for span in spans if span.name.startswith("chain ")]

    assert [
        "chain synopsis",
        "chain LLMChain",
        "chain SequentialChain",
    ] == [span.name for span in langchain_spans]

    synopsis_span = next(span for span in spans if span.name == "chain synopsis")
    review_span = next(span for span in spans if span.name == "chain LLMChain")
    overall_span = next(span for span in spans if span.name == "chain SequentialChain")

    synopsis_prompt, synopsis_completion = validate_span(
        synopsis_span, SpanKind.INTERNAL, ["gen_ai.prompt", "gen_ai.completion"]
    )
    assert synopsis_prompt == input_data
    assert "synopsis" in synopsis_completion

    review_prompt, review_completion = validate_span(
        review_span, SpanKind.INTERNAL, ["gen_ai.prompt", "gen_ai.completion"]
    )
    assert "title" in review_prompt
    assert "era" in review_prompt
    assert "synopsis" in review_prompt
    assert "review" in review_completion

    overall_prompt, overall_completion = validate_span(
        overall_span, SpanKind.INTERNAL, ["gen_ai.prompt", "gen_ai.completion"]
    )
    assert overall_prompt == input_data
    assert "synopsis" in overall_completion
    assert "review" in overall_completion
