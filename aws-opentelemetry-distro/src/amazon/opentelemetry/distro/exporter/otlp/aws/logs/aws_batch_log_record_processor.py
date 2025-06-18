import logging
from typing import List, Mapping, Optional, Sequence, cast

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
from opentelemetry.context import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    attach,
    detach,
    set_value,
)
from opentelemetry.sdk._logs import LogData
from opentelemetry.sdk._logs._internal.export import BatchLogExportStrategy
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.util.types import AnyValue

_logger = logging.getLogger(__name__)


class AwsBatchLogRecordProcessor(BatchLogRecordProcessor):
    _BASE_LOG_BUFFER_BYTE_SIZE = 2000 # Buffer size in bytes to account for log metadata not included in the body size calculation
    _MAX_LOG_REQUEST_BYTE_SIZE = (
        1048576  # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    )
    _MAX_LOG_DEPTH = 3 # Maximum depth to traverse in the log body structures; deeper levels are ignored for size calculation

    def __init__(
        self,
        exporter: OTLPAwsLogExporter,
        schedule_delay_millis: Optional[float] = None,
        max_export_batch_size: Optional[int] = None,
        export_timeout_millis: Optional[float] = None,
        max_queue_size: Optional[int] = None,
    ):

        super().__init__(
            exporter=exporter,
            schedule_delay_millis=schedule_delay_millis,
            max_export_batch_size=max_export_batch_size,
            export_timeout_millis=export_timeout_millis,
            max_queue_size=max_queue_size,
        )

        self._exporter = exporter

    # https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-sdk/src/opentelemetry/sdk/_shared_internal/__init__.py#L143
    def _export(self, batch_strategy: BatchLogExportStrategy) -> None:
        """
        Preserves existing batching behavior but will intermediarly export small log batches if
        the size of the data in the batch is at orabove AWS CloudWatch's maximum request size limit of 1 MB.

        - Data size of exported batches will ALWAYS be <= 1 MB except for the case below:
        - If the data size of an exported batch is ever > 1 MB then the batch size is guaranteed to be 1
        """
        with self._export_lock:
            iteration = 0
            # We could see concurrent export calls from worker and force_flush. We call _should_export_batch
            # once the lock is obtained to see if we still need to make the requested export.
            while self._should_export_batch(batch_strategy, iteration):
                iteration += 1
                token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
                try:
                    batch_length = min(self._max_export_batch_size, len(self._queue))
                    batch_data_size = 0
                    batch = []

                    for _ in range(batch_length):
                        log_data: LogData = self._queue.pop()
                        log_size, paths = self._traverse_log_and_calculate_size(log_data.log_record.body)
                        log_size += self._BASE_LOG_BUFFER_BYTE_SIZE

                        if batch and (batch_data_size + log_size > self._MAX_LOG_REQUEST_BYTE_SIZE):
                            # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                            if batch_data_size > self._MAX_LOG_REQUEST_BYTE_SIZE:
                                self._exporter.set_llo_paths(paths)

                            self._exporter.export(batch)
                            batch_data_size = 0
                            batch = []

                        batch_data_size += log_size
                        batch.append(log_data)

                    if batch:
                        # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                        if batch_data_size > self._MAX_LOG_REQUEST_BYTE_SIZE:
                            _, paths = self._traverse_log_and_calculate_size(batch[0].log_record.body)
                            self._exporter.set_llo_paths(paths)

                        self._exporter.export(batch)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    _logger.exception("Exception while exporting logs.")
                detach(token)

    def _traverse_log_and_calculate_size(self, val: AnyValue) -> tuple[int, List[str]]:
        starting_path: str = "['body']"

        # Use a stack to prevent excessive recursive calls.
        paths: List[str] = []
        stack = [(val, starting_path, 0)]
        size: int = 0

        while stack:
            next_val, current_path, current_depth = stack.pop()

            if isinstance(next_val, str):
                size += len(next_val)
                paths.append(f"{current_path}['stringValue']")
                continue

            if isinstance(next_val, bytes):
                size += len(next_val)
                continue

            if isinstance(next_val, bool):
                size += 4 if next_val else 5
                continue

            if isinstance(next_val, (float, int)):
                size += len(str(next_val))
                continue

            if current_depth <= self._MAX_LOG_DEPTH:
                if isinstance(next_val, Sequence):
                    array_path = f"{current_path}['arrayValue']['values']"
                    for i, content in enumerate(next_val):
                        new_path = f"{array_path}[{i}]"
                        stack.append((cast(AnyValue, content), new_path, current_depth + 1))

                if isinstance(next_val, Mapping):
                    kv_path = f"{current_path}['kvlistValue']['values']"
                    for i, (key, content) in enumerate(next_val.items()):
                        entry_path = f"{kv_path}[{i}]"
                        new_path = f"{entry_path}['value']"
                        size += len(key)
                        stack.append((content, new_path, current_depth + 1))
            else:
                _logger.debug("Max log depth exceeded. Log data size will not be calculated.")
                return 0, []

        return size, paths
