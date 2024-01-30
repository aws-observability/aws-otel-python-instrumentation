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
        self.__getSamplingRulesEndpoint = endpoint + "/GetSamplingRules"

    def get_sampling_rules(self):
        sampling_rules = []
        headers = {"content-type": "application/json"}

        try:
            r = requests.post(url=self.__getSamplingRulesEndpoint, headers=headers)
            if r is None:
                raise Exception("GetSamplingRules response is None")
            sampling_rules_response = r.json()
            if "SamplingRuleRecords" not in sampling_rules_response:
                raise Exception(
                    f"SamplingRuleRecords is missing in getSamplingRules response:{sampling_rules_response}"
                )

            sampling_rules_records = sampling_rules_response["SamplingRuleRecords"]
            for record in sampling_rules_records:
                sampling_rules.append(SamplingRule(**record["SamplingRule"]))

        except requests.exceptions.RequestException as req_err:
            _logger.exception(f"Request error occurred: {req_err}")
        except json.JSONDecodeError as json_err:
            _logger.exception(f"Error in decoding JSON response: {json_err}")
        except Exception as ex:
            _logger.exception(f"Exception occurred: {ex}")

        return sampling_rules
