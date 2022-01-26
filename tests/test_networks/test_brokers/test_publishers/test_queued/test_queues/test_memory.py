import unittest

from minos.networks import (
    BrokerPublisherQueue,
    InMemoryBrokerPublisherQueue,
    InMemoryBrokerQueue,
)


class TestInMemoryBrokerPublisherQueue(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerPublisherQueue, (InMemoryBrokerQueue, BrokerPublisherQueue)))


if __name__ == "__main__":
    unittest.main()
