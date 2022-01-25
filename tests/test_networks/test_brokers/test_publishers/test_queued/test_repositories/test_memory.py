import unittest

from minos.networks import (
    BrokerPublisherRepository,
    InMemoryBrokerPublisherRepository,
    InMemoryBrokerRepository,
)


class TestInMemoryBrokerPublisherRepository(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(
            issubclass(InMemoryBrokerPublisherRepository, (InMemoryBrokerRepository, BrokerPublisherRepository))
        )


if __name__ == "__main__":
    unittest.main()
