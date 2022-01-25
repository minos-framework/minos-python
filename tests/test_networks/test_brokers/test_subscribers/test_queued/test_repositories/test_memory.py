import unittest

from minos.networks import (
    BrokerSubscriberRepository,
    InMemoryBrokerRepository,
    InMemoryBrokerSubscriberRepository,
)


class TestInMemoryBrokerSubscriberRepository(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(
            issubclass(InMemoryBrokerSubscriberRepository, (InMemoryBrokerRepository, BrokerSubscriberRepository))
        )


if __name__ == "__main__":
    unittest.main()
