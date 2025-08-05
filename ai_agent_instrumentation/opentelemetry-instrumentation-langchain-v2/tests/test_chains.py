import ast
import os

import boto3
import pytest
from langchain.chains import LLMChain, SequentialChain
from langchain.prompts import PromptTemplate
from langchain_aws import BedrockLLM

from opentelemetry.trace import SpanKind


@pytest.mark.vcr(filter_headers=["Authorization", "X-Amz-Date", "X-Amz-Security-Token"], record_mode="once")
def test_sequential_chain(instrument_langchain, span_exporter):
    span_exporter.clear()

    session = boto3.Session(region_name="us-west-2")

    bedrock_client = session.client(service_name="bedrock-runtime", region_name="us-west-2")

    llm = BedrockLLM(
        client=bedrock_client,
        model_id="anthropic.claude-v2",
        model_kwargs={
            "max_tokens_to_sample": 500,
            "temperature": 0.7,
        },
    )
    synopsis_template = """You are a playwright. Given the title of play and the era it is set in, it is your job to write a synopsis for that title.

    Title: {title}
    Era: {era}
    Playwright: This is a synopsis for the above play:"""  # noqa: E501
    synopsis_prompt_template = PromptTemplate(input_variables=["title", "era"], template=synopsis_template)
    synopsis_chain = LLMChain(llm=llm, prompt=synopsis_prompt_template, output_key="synopsis", name="synopsis")

    template = """You are a play critic from the New York Times. Given the synopsis of play, it is your job to write a review for that play.

    Play Synopsis:
    {synopsis}
    Review from a New York Times play critic of the above play:"""  # noqa: E501
    prompt_template = PromptTemplate(input_variables=["synopsis"], template=template)
    review_chain = LLMChain(llm=llm, prompt=prompt_template, output_key="review")

    overall_chain = SequentialChain(
        chains=[synopsis_chain, review_chain],
        input_variables=["era", "title"],
        output_variables=["synopsis", "review"],
        verbose=True,
    )
    overall_chain.invoke({"title": "Tragedy at sunset on the beach", "era": "Victorian England"})

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

    assert synopsis_span.kind == SpanKind.INTERNAL
    assert "gen_ai.prompt" in synopsis_span.attributes
    assert "gen_ai.completion" in synopsis_span.attributes

    synopsis_prompt = ast.literal_eval(synopsis_span.attributes["gen_ai.prompt"])
    synopsis_completion = ast.literal_eval(synopsis_span.attributes["gen_ai.completion"])

    assert synopsis_prompt == {"title": "Tragedy at sunset on the beach", "era": "Victorian England"}
    assert "synopsis" in synopsis_completion

    assert review_span.kind == SpanKind.INTERNAL
    assert "gen_ai.prompt" in review_span.attributes
    assert "gen_ai.completion" in review_span.attributes
    print("Raw completion value:", repr(synopsis_span.attributes["gen_ai.completion"]))

    review_prompt = ast.literal_eval(review_span.attributes["gen_ai.prompt"])
    review_completion = ast.literal_eval(review_span.attributes["gen_ai.completion"])

    assert "title" in review_prompt
    assert "era" in review_prompt
    assert "synopsis" in review_prompt
    assert "review" in review_completion

    assert overall_span.kind == SpanKind.INTERNAL
    assert "gen_ai.prompt" in overall_span.attributes
    assert "gen_ai.completion" in overall_span.attributes

    overall_prompt = ast.literal_eval(overall_span.attributes["gen_ai.prompt"])
    overall_completion = ast.literal_eval(overall_span.attributes["gen_ai.completion"])

    assert overall_prompt == {"title": "Tragedy at sunset on the beach", "era": "Victorian England"}
    assert "synopsis" in overall_completion
    assert "review" in overall_completion
