import unittest
from abc import (
    ABC,
)

from minos.networks import (
    BrokerPublisherRepository,
    BrokerRepository,
)


class TestBrokerPublisherRepository(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherRepository, (ABC, BrokerRepository)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerPublisherRepository.__abstractmethods__,
        )


if __name__ == "__main__":
    unittest.main()
