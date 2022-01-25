import unittest

from minos.networks import (
    BrokerSubscriberQueue,
    InMemoryBrokerQueue,
    InMemoryBrokerSubscriberQueue,
)


class TestInMemoryBrokerSubscriberQueue(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(
            issubclass(InMemoryBrokerSubscriberQueue, (InMemoryBrokerQueue, BrokerSubscriberQueue))
        )


if __name__ == "__main__":
    unittest.main()
