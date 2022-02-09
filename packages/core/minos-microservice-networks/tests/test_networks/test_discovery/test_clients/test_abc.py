import unittest

from minos.common import (
    Object,
)
from minos.networks import (
    DiscoveryClient,
)


class TestDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DiscoveryClient, Object))


if __name__ == "__main__":
    unittest.main()
