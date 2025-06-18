import logging
from typing import Mapping, Optional, Sequence, cast

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
    _BASE_LOG_BUFFER_BYTE_SIZE = (
        2000  # Buffer size in bytes to account for log metadata not included in the body size calculation
    )
    _MAX_LOG_REQUEST_BYTE_SIZE = (
        1048576  # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    )

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
                        log_size = self._BASE_LOG_BUFFER_BYTE_SIZE + self._get_any_value_size(log_data.log_record.body)

                        if batch and (batch_data_size + log_size > self._MAX_LOG_REQUEST_BYTE_SIZE):
                            # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                            if batch_data_size > self._MAX_LOG_REQUEST_BYTE_SIZE:
                                if self._is_gen_ai_log(batch[0]):
                                    self._exporter.set_gen_ai_log_flag()

                            self._exporter.export(batch)
                            batch_data_size = 0
                            batch = []

                        batch_data_size += log_size
                        batch.append(log_data)

                    if batch:
                        # if batch_data_size > MAX_LOG_REQUEST_BYTE_SIZE then len(batch) == 1
                        if batch_data_size > self._MAX_LOG_REQUEST_BYTE_SIZE:
                            if self._is_gen_ai_log(batch[0]):
                                self._exporter.set_gen_ai_log_flag()

                        self._exporter.export(batch)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    _logger.exception("Exception while exporting logs: " + str(e))
                detach(token)

    def _get_any_value_size(self, val: AnyValue, depth: int = 3) -> int:
        """
        Only used to indicate whether we should export a batch log size of 1 or not.
        Calculates the size in bytes of an AnyValue object.
        Will processs complex AnyValue structures up to the specified depth limit.
        If the depth limit of the AnyValue structure is exceeded, returns 0.

        Args:
            val: The AnyValue object to calculate size for
            depth: Maximum depth to traverse in nested structures (default: 3)

        Returns:
            int: Total size of the AnyValue object in bytes
        """
        # Use a stack to prevent excessive recursive calls.
        stack = [(val, 0)]
        size: int = 0

        while stack:
            # small optimization. We can stop calculating the size once it reaches the 1 MB limit.
            if size >= self._MAX_LOG_REQUEST_BYTE_SIZE:
                return size

            next_val, current_depth = stack.pop()

            if isinstance(next_val, (str, bytes)):
                size += len(next_val)
                continue

            if isinstance(next_val, bool):
                size += 4 if next_val else 5
                continue

            if isinstance(next_val, (float, int)):
                size += len(str(next_val))
                continue

            if current_depth <= depth:
                if isinstance(next_val, Sequence):
                    for content in next_val:
                        stack.append((cast(AnyValue, content), current_depth + 1))

                if isinstance(next_val, Mapping):
                    for key, content in next_val.items():
                        size += len(key)
                        stack.append((content, current_depth + 1))
            else:
                _logger.debug("Max log depth exceeded. Log data size will not be accurately calculated.")
                return 0

        return size

    @staticmethod
    def _is_gen_ai_log(log_data: LogData) -> bool:
        """
        Is the log a Gen AI log event?
        """
        gen_ai_instrumentations = {
            "openinference.instrumentation.langchain",
            "openinference.instrumentation.crewai",
            "opentelemetry.instrumentation.langchain",
            "crewai.telemetry",
            "openlit.otel.tracing",
        }

        return log_data.instrumentation_scope.name in gen_ai_instrumentations
