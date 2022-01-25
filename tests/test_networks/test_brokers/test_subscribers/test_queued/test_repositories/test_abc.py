import unittest
from abc import (
    ABC,
)

from minos.networks import (
    BrokerMessage,
    BrokerRepository,
    BrokerSubscriberRepository,
)


class _BrokerSubscriberRepository(BrokerSubscriberRepository):
    """For testing purposes."""

    async def _enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def _dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerSubscriberRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}

    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberRepository, (ABC, BrokerRepository)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerSubscriberRepository.__abstractmethods__,
        )

    def test_topics(self):
        repository = _BrokerSubscriberRepository(self.topics)
        self.assertEqual(self.topics, repository.topics)

    def test_topics_raises(self):
        with self.assertRaises(ValueError):
            _BrokerSubscriberRepository([])


if __name__ == "__main__":
    unittest.main()
