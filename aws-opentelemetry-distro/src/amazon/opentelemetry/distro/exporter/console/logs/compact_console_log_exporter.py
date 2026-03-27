# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import Sequence

from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import ConsoleLogRecordExporter, LogRecordExportResult


class CompactConsoleLogRecordExporter(ConsoleLogRecordExporter):
    def export(self, batch: Sequence[ReadableLogRecord]):
        for data in batch:
            formatted_json = self.formatter(data.log_record)
            print(re.sub(r"\s*([{}[\]:,])\s*", r"\1", formatted_json), flush=True)

        return LogRecordExportResult.SUCCESS
