import unittest

from minos.common import (
    MinosException,
)
from minos.networks import (
    MinosNetworkException,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosNetworkException, MinosException))


if __name__ == "__main__":
    unittest.main()
