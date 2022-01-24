import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberRepository,
    BrokerSubscriberRepositoryBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _BrokerSubscriberRepository(BrokerSubscriberRepository):
    """For testing purposes."""

    async def enqueue(self, message: BrokerMessage) -> None:
        """For testing purposes."""

    async def dequeue(self) -> BrokerMessage:
        """For testing purposes."""


class _BrokerSubscriberRepositoryBuilder(BrokerSubscriberRepositoryBuilder):
    """For testing purposes."""

    def build(self) -> BrokerSubscriberRepository:
        """For testing purposes."""
        return _BrokerSubscriberRepository(**self.kwargs)


class TestBrokerSubscriberRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}

    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberRepository, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"enqueue", "dequeue"}, BrokerSubscriberRepository.__abstractmethods__,
        )

    def test_topics(self):
        repository = _BrokerSubscriberRepository(self.topics)
        self.assertEqual(self.topics, repository.topics)

    def test_topics_raises(self):
        with self.assertRaises(ValueError):
            _BrokerSubscriberRepository([])

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        async with _BrokerSubscriberRepository(self.topics) as repository:
            repository.dequeue = dequeue_mock
            observed = await repository.__aiter__().__anext__()

        self.assertEqual(messages[0], observed)
        self.assertEqual(1, dequeue_mock.call_count)

    async def test_iter_raises(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        dequeue_mock = AsyncMock(side_effect=messages)

        repository = _BrokerSubscriberRepository(self.topics)
        repository.dequeue = dequeue_mock
        with self.assertRaises(StopAsyncIteration):
            await repository.__aiter__().__anext__()


class TestBrokerSubscriberBuilder(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberRepositoryBuilder, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"build"}, BrokerSubscriberRepositoryBuilder.__abstractmethods__,
        )

    def test_new(self):
        builder = _BrokerSubscriberRepositoryBuilder.new()
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        builder = _BrokerSubscriberRepositoryBuilder.new().with_topics({"one", "two"}).copy()
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_with_kwargs(self):
        builder = _BrokerSubscriberRepositoryBuilder().with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        builder = _BrokerSubscriberRepositoryBuilder().with_config(CONFIG_FILE_PATH)
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        self.assertEqual(dict(), builder.kwargs)

    def test_with_topics(self):
        builder = _BrokerSubscriberRepositoryBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_build(self):
        builder = _BrokerSubscriberRepositoryBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberRepositoryBuilder)
        subscriber = builder.build()
        self.assertIsInstance(subscriber, _BrokerSubscriberRepository)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()
