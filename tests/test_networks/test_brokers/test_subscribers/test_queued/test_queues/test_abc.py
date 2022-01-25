import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerQueue,
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
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

    def test_new(self):
        builder = _BrokerSubscriberQueueBuilder.new()
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        builder = _BrokerSubscriberQueueBuilder.new().with_topics({"one", "two"}).copy()
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_with_kwargs(self):
        builder = _BrokerSubscriberQueueBuilder().with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        config = MinosConfig(CONFIG_FILE_PATH)
        builder = _BrokerSubscriberQueueBuilder().with_config(config)
        self.assertIsInstance(builder, _BrokerSubscriberQueueBuilder)
        self.assertEqual(dict(), builder.kwargs)

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
