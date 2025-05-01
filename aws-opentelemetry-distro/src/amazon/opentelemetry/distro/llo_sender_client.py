import logging

from typing import Dict

class LLOSenderClient:
    """Skeleton client for handling Large Language Objects (LLO)"""

    def __init__(self):
        self._bucket_name = "genai-llo-bucket"
        self._logger = logging.getLogger(__name__)
        self._logger.info("Initialized mock LLO sender client")

    def upload(self, llo_content: str, metadata: Dict[str, str]) -> str:
        """Mock upload that returns a dummy S3 pointer

        Args:
            llo_content: For now we assume this will be a str that contains the LLM input/output
            metadata: Metadata associated with the LLO content, such as trace_id, span_id

        Returns:
            str: S3 pointer to the uploaded LLO content 
        """
        attribute_name = metadata.get("attribute_name", "unknown")
        self._logger.debug(f"LLO content: {llo_content}")
        self._logger.debug(f"Mock upload of LLO attribute: {attribute_name}")
        return f"s3://{self._bucket_name}/{metadata.get('trace_id', 'trace')}/{metadata.get('span_id', 'span')}/{attribute_name}"
