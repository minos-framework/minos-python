import unittest
from abc import (
    ABC,
)

from minos.networks import (
    BrokerPublisherQueue,
    BrokerRepository,
)


class TestBrokerPublisherQueue(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherQueue, (ABC, BrokerRepository)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerPublisherQueue.__abstractmethods__,
        )


if __name__ == "__main__":
    unittest.main()
