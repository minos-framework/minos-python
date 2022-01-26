import unittest
from abc import (
    ABC,
)

from minos.networks import (
    BrokerMessage,
    BrokerQueue,
    BrokerSubscriberQueue,
)


class _BrokerSubscriberQueue(BrokerSubscriberQueue):
    """For testing purposes."""

    async def _enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def _dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerSubscriberQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}

    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberQueue, (ABC, BrokerQueue)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_enqueue", "_dequeue"}, BrokerSubscriberQueue.__abstractmethods__,
        )

    def test_topics(self):
        queue = _BrokerSubscriberQueue(self.topics)
        self.assertEqual(self.topics, queue.topics)

    def test_topics_raises(self):
        with self.assertRaises(ValueError):
            _BrokerSubscriberQueue([])


if __name__ == "__main__":
    unittest.main()
