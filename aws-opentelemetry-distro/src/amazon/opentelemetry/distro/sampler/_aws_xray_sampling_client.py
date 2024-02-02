# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from logging import getLogger

import requests

from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule

_logger = getLogger(__name__)


class _AwsXRaySamplingClient:
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
                _logger.error("GetSamplingRules response is None")
                return []
            sampling_rules_response = xray_response.json()
            if "SamplingRuleRecords" not in sampling_rules_response:
                _logger.error(
                    "SamplingRuleRecords is missing in getSamplingRules response: %s", sampling_rules_response
                )
                return []

            sampling_rules_records = sampling_rules_response["SamplingRuleRecords"]
            for record in sampling_rules_records:
                sampling_rules.append(_SamplingRule(**record["SamplingRule"]))

        except requests.exceptions.RequestException as req_err:
            _logger.error("Request error occurred: %s", req_err)
        except json.JSONDecodeError as json_err:
            _logger.error("Error in decoding JSON response: %s", json_err)

        return sampling_rules
