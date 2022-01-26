import unittest
from abc import (
    ABC,
)

from minos.networks import (
    BrokerPublisherQueue,
    BrokerQueue,
)


class TestBrokerPublisherQueue(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherQueue, (ABC, BrokerQueue)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerPublisherQueue.__abstractmethods__,
        )


if __name__ == "__main__":
    unittest.main()
