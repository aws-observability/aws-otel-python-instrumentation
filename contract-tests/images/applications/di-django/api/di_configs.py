# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: skip-file
"""DI breakpoint and probe configurations for the Django contract test app.

Targets live in `api.views`, so all CodeLocation entries set
CodeUnit/FilePath accordingly.
"""

from api.views import _CALCULATE_SUM_LINE

_CODE_UNIT = "api.views"
_FILE_PATH = "api/views.py"


BREAKPOINT_CONFIGS = [
    # Function-level breakpoint on process_data
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "process_data",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000001",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["value"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Line-level breakpoint on calculate_sum
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "calculate_sum",
                "FilePath": _FILE_PATH,
                "LineNumber": _CALCULATE_SUM_LINE,
            }
        },
        "LocationHash": "aabb000000000003",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureLocals": True,
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # Breakpoint with low hit limit on limited_function
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "limited_function",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000004",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["x"],
                "CaptureLimits": {"MaxStringLength": 255, "MaxHits": 3},
            }
        },
    },
    # Breakpoint on shared_function (coexists with PROBE)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "shared_function",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000005",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["data"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # String truncation limit validation (MaxStringLength=9999 -> clamped to 255)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "process_long_string",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000007",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["long_string"],
                "CaptureLimits": {"MaxStringLength": 9999},
            }
        },
    },
    # Collection width limit validation (MaxCollectionWidth=9999 -> clamped to 20)
    {
        "InstrumentationType": "BREAKPOINT",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "process_large_collection",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000008",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["large_list"],
                "CaptureLimits": {"MaxCollectionWidth": 9999},
            }
        },
    },
]

PROBE_CONFIGS = [
    # PROBE on compute_total
    {
        "InstrumentationType": "PROBE",
        "InstrumentationName": "compute-total-probe",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "compute_total",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000002",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["items"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
    # PROBE on shared_function (coexists with BREAKPOINT)
    {
        "InstrumentationType": "PROBE",
        "InstrumentationName": "shared-function-probe",
        "SignalType": "SNAPSHOT",
        "Location": {
            "CodeLocation": {
                "Language": "Python",
                "CodeUnit": _CODE_UNIT,
                "MethodName": "shared_function",
                "FilePath": _FILE_PATH,
            }
        },
        "LocationHash": "aabb000000000006",
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": True,
                "CaptureArguments": ["data"],
                "CaptureLimits": {"MaxStringLength": 255},
            }
        },
    },
]
