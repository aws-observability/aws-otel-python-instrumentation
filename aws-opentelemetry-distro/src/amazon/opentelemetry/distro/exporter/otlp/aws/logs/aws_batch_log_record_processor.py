import logging
from typing import Mapping, Sequence

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
from opentelemetry.sdk._logs import LogData
from opentelemetry.sdk._logs._internal.export import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    BatchLogExportStrategy,
    attach,
    detach,
    set_value,
)
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.util.types import AnyValue

_logger = logging.getLogger(__name__)

BASE_LOG_BUFFER_BYTE_SIZE = 2000
MAX_LOG_REQUEST_BYTE_SIZE = (
    1048576  # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
)


class AwsBatchLogRecordProcessor(BatchLogRecordProcessor):

    def __init__(
        self,
        exporter: OTLPAwsLogExporter,
        schedule_delay_millis: float | None = None,
        max_export_batch_size: int | None = None,
        export_timeout_millis: float | None = None,
        max_queue_size: int | None = None,
    ):

        super().__init__(
            exporter=exporter,
            schedule_delay_millis=schedule_delay_millis,
            max_export_batch_size=max_export_batch_size,
            export_timeout_millis=export_timeout_millis,
            max_queue_size=max_queue_size,
        )

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

                        log_data = self._queue.pop()
                        log_size = self._get_size_of_log(log_data)

                        if batch and (batch_data_size + log_size > MAX_LOG_REQUEST_BYTE_SIZE):
                            # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                            if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE:
                                self._exporter.set_gen_ai_flag()

                            self._exporter.export(batch)
                            batch_data_size = 0
                            batch = []

                        batch_data_size += log_size
                        batch.append(log_data)

                    if batch:
                        # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                        if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE:
                            self._exporter.set_gen_ai_flag()

                        self._exporter.export(batch)
                except Exception:  # pylint: disable=broad-exception-caught
                    _logger.exception("Exception while exporting logs.")
                detach(token)

    @staticmethod
    def _get_size_of_log(log_data: LogData) -> int:
        """
        Estimates the size of a given LogData based on the size of the body + a buffer
        amount representing a rough guess of other data present in the log.
        """
        size = BASE_LOG_BUFFER_BYTE_SIZE
        body = log_data.log_record.body

        if body:
            size += AwsBatchLogRecordProcessor._get_size_of_any_value(body)

        return size

    @staticmethod
    def _get_size_of_any_value(val: AnyValue) -> int:
        """
        Calculates the size of an AnyValue type.
        If AnyValue is an instance of a Map or Array, calculation is truncated to one layer.
        """

        # Use a stack to prevent excessive recursive calls.
        stack = [val]
        size = 0
        depth = 0

        while stack:
            next = stack.pop()

            if isinstance(next, (str, bytes)):
                size += len(next)
                continue

            if isinstance(next, bool):
                size += 4 if next else 5
                continue

            if isinstance(next, (float, int)):
                size += len(str(next))
                continue

            if isinstance(next, Mapping):
                if depth < 1:
                    for key, content in next.items():
                        size += len(key)
                        stack.append(content)

                    depth += 1
                continue

            if isinstance(next, Sequence):
                if depth < 1:
                    for content in next:
                        stack.append(content)

                    depth += 1
                continue

        return size
