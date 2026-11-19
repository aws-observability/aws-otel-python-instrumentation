# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""GenAI layer, driven by the hooks in ``_auto_instrumentation``, which always import ``_settings`` and import the
step modules only when the layer is not off."""
