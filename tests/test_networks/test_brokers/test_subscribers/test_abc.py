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
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _BrokerSubscriberBuilder(BrokerSubscriberBuilder):
    def build(self) -> BrokerSubscriber:
        """For testing purposes."""
        return _BrokerSubscriber(**self.kwargs)


class _BrokerSubscriber(BrokerSubscriber):
    async def _receive(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriber, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_receive"}, BrokerSubscriber.__abstractmethods__,
        )

    def test_topics(self):
        subscriber = _BrokerSubscriber(["foo", "bar", "bar"])
        self.assertEqual({"foo", "bar"}, subscriber.topics)

    async def test_receive(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        mock = AsyncMock(return_value=message)
        subscriber = _BrokerSubscriber(list())
        subscriber._receive = mock

        observed = await subscriber.receive()
        self.assertEqual(message, observed)
        self.assertEqual(1, mock.call_count)

    async def test_aiter(self):
        expected = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        mock = AsyncMock(side_effect=expected)
        subscriber = _BrokerSubscriber(list())
        subscriber.receive = mock

        await subscriber.setup()
        observed = list()
        async for message in subscriber:
            observed.append(message)
            if len(observed) == len(expected):
                await subscriber.destroy()

        self.assertEqual(expected, observed)


class TestBrokerSubscriberBuilder(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriberBuilder, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"build"}, BrokerSubscriberBuilder.__abstractmethods__,
        )

    def test_new(self):
        builder = _BrokerSubscriberBuilder.new()
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        builder = _BrokerSubscriberBuilder.new().with_topics({"one", "two"}).copy()
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_with_kwargs(self):
        builder = _BrokerSubscriberBuilder().with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        builder = _BrokerSubscriberBuilder().with_config(CONFIG_FILE_PATH)
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual(dict(), builder.kwargs)

    def test_with_group_id(self):
        builder = _BrokerSubscriberBuilder().with_group_id("foobar")
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual({"group_id": "foobar"}, builder.kwargs)

    def test_with_remove_topics_on_destroy(self):
        builder = _BrokerSubscriberBuilder().with_remove_topics_on_destroy(False)
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual({"remove_topics_on_destroy": False}, builder.kwargs)

    def test_with_topics(self):
        builder = _BrokerSubscriberBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_build(self):
        builder = _BrokerSubscriberBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, _BrokerSubscriberBuilder)
        subscriber = builder.build()
        self.assertIsInstance(subscriber, _BrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()
