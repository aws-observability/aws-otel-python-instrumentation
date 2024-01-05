from unittest import TestCase

from pkg_resources import DistributionNotFound, require


class TestAWSDistro(TestCase):
    def test_package_available(self):
        try:
            require(["opentelemetry-distro-aws"])
        except DistributionNotFound:
            self.fail("opentelemetry-distro-aws not installed")
