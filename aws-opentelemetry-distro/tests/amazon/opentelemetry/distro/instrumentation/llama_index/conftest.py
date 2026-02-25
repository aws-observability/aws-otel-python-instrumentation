# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import importlib.util

collect_ignore_glob = []

if importlib.util.find_spec("llama_index") is None:
    collect_ignore_glob.append("test_*.py")
