import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerQueue,
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)


class _BrokerSubscriberQueue(BrokerSubscriberQueue):
    """For testing purposes."""

    async def _enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def _dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class _BrokerSubscriberQueueBuilder(BrokerSubscriberQueueBuilder):
    """For testing purposes."""

    def build(self) -> BrokerSubscriberQueue:
        """For testing purposes."""
        return _BrokerSubscriberQueue(**self.kwargs)


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


class TestBrokerSubscriberBuilder(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberQueueBuilder, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"build"}, BrokerSubscriberQueueBuilder.__abstractmethods__,
        )

    def test_with_topics(self):
        builder = _BrokerSubscriberQueueBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_build(self):
        builder = _BrokerSubscriberQueueBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        subscriber = builder.build()
        self.assertIsInstance(subscriber, _BrokerSubscriberQueue)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()
