from logging import getLogger

from opentelemetry.instrumentation.distro import BaseDistro

logger = getLogger(__name__)


class AWSDistro(BaseDistro):
    def _configure(self, **kwargs):
        super(AWSDistro, self)._configure()
