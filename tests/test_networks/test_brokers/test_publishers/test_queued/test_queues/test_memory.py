import unittest

from minos.networks import (
    BrokerPublisherQueue,
    InMemoryBrokerPublisherQueue,
    InMemoryBrokerRepository,
)


class TestInMemoryBrokerPublisherQueue(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerPublisherQueue, (InMemoryBrokerRepository, BrokerPublisherQueue)))


if __name__ == "__main__":
    unittest.main()
