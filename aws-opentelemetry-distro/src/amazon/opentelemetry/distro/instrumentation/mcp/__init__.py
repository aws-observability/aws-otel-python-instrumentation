# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from .version import __version__
from .instrumentation import McpInstrumentor

__all__ = ["McpInstrumentor", "__version__"]
