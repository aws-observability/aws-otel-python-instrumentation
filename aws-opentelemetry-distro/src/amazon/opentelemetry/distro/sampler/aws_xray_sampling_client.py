# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from logging import getLogger

import requests

from amazon.opentelemetry.distro.sampler.sampling_rule import SamplingRule

_logger = getLogger(__name__)


class AwsXRaySamplingClient:
    def __init__(self, endpoint=None, log_level=None):
        # Override default log level
        if log_level is not None:
            _logger.setLevel(log_level)

        if endpoint is None:
            _logger.error("endpoint must be specified")
        self.__get_sampling_rules_endpoint = endpoint + "/GetSamplingRules"

    def get_sampling_rules(self):
        sampling_rules = []
        headers = {"content-type": "application/json"}

        try:
            xray_response = requests.post(url=self.__get_sampling_rules_endpoint, headers=headers, timeout=20)
            if xray_response is None:
                raise ValueError("GetSamplingRules response is None")
            sampling_rules_response = xray_response.json()
            if "SamplingRuleRecords" not in sampling_rules_response:
                raise ValueError(
                    f"SamplingRuleRecords is missing in getSamplingRules response:{sampling_rules_response}"
                )

            sampling_rules_records = sampling_rules_response["SamplingRuleRecords"]
            for record in sampling_rules_records:
                sampling_rules.append(SamplingRule(**record["SamplingRule"]))

        except requests.exceptions.RequestException as req_err:
            _logger.exception("Request error occurred: %s", req_err)
        except json.JSONDecodeError as json_err:
            _logger.exception("Error in decoding JSON response: %s", json_err)
        except ValueError as ex:
            _logger.exception("Exception occurred: %s", ex)

        return sampling_rules
