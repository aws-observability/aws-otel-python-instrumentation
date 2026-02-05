# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from logging import getLogger

import requests

from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from amazon.opentelemetry.distro.sampler._sampling_target import _SamplingTargetResponse

_logger = getLogger(__name__)


class _AwsXRaySamplingClient:
    def __init__(self, endpoint: str = None, log_level: str = None):
        # Override default log level
        if log_level is not None:
            _logger.setLevel(log_level)

        if endpoint is None:
            _logger.error("endpoint must be specified")
        self.__get_sampling_rules_endpoint = endpoint + "/GetSamplingRules"
        self.__get_sampling_targets_endpoint = endpoint + "/SamplingTargets"

        self.__session = requests.Session()

    def get_sampling_rules(self) -> [_SamplingRule]:
        sampling_rules = []
        headers = {"content-type": "application/json"}

        try:
            xray_response = self.__session.post(url=self.__get_sampling_rules_endpoint, headers=headers, timeout=20)
            if xray_response is None:
                _logger.error("GetSamplingRules response is None")
                return []
            sampling_rules_response = xray_response.json()
            if sampling_rules_response is None or "SamplingRuleRecords" not in sampling_rules_response:
                _logger.error(
                    "SamplingRuleRecords is missing in getSamplingRules response: %s", sampling_rules_response
                )
                return []
            sampling_rules_records = sampling_rules_response["SamplingRuleRecords"]
            for record in sampling_rules_records:
                if "SamplingRule" not in record:
                    _logger.error("SamplingRule is missing in SamplingRuleRecord")
                else:
                    sampling_rules.append(_SamplingRule(**record["SamplingRule"]))

        except requests.exceptions.RequestException as req_err:
            _logger.error("Request error occurred: %s", req_err)
        except json.JSONDecodeError as json_err:
            _logger.error("Error in decoding JSON response: %s", json_err)
        # pylint: disable=broad-exception-caught
        except Exception as err:
            _logger.error("Error occurred when attempting to fetch rules: %s", err)

        return sampling_rules

    def get_sampling_targets(self, statistics: [dict]) -> _SamplingTargetResponse:
        sampling_targets_response = _SamplingTargetResponse(
            LastRuleModification=None, SamplingTargetDocuments=None, UnprocessedStatistics=None
        )
        headers = {"content-type": "application/json"}
        try:
            xray_response = self.__session.post(
                url=self.__get_sampling_targets_endpoint,
                headers=headers,
                timeout=20,
                json={"SamplingStatisticsDocuments": statistics},
            )
            if xray_response is None:
                _logger.debug("GetSamplingTargets response is None. Unable to update targets.")
                return sampling_targets_response
            xray_response_json = xray_response.json()
            if (
                xray_response_json is None
                or "SamplingTargetDocuments" not in xray_response_json
                or "LastRuleModification" not in xray_response_json
            ):
                _logger.debug("getSamplingTargets response is invalid. Unable to update targets.")
                return sampling_targets_response

            sampling_targets_response = _SamplingTargetResponse(**xray_response_json)
        except requests.exceptions.RequestException as req_err:
            _logger.debug("Request error occurred: %s", req_err)
        except json.JSONDecodeError as json_err:
            _logger.debug("Error in decoding JSON response: %s", json_err)
        # pylint: disable=broad-exception-caught
        except Exception as err:
            _logger.debug("Error occurred when attempting to fetch targets: %s", err)

        return sampling_targets_response
