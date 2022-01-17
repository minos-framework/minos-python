import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerPublisher,
)


class TestBrokerPublisher(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisher, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"send"}, BrokerPublisher.__abstractmethods__,
        )


if __name__ == "__main__":
    unittest.main()
