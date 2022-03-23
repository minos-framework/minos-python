import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from minos.common import (
    Builder,
    Config,
    MinosConfigException,
    SetupMixin,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    BrokerSubscriberBuilder,
    IdempotentBrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberDuplicateDetector,
    InMemoryBrokerSubscriberQueue,
    InMemoryBrokerSubscriberQueueBuilder,
    QueuedBrokerSubscriber,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _BrokerSubscriber(BrokerSubscriber):
    """For testing purposes."""

    async def _receive(self) -> BrokerMessage:
        """For testing purposes."""


class TestBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerSubscriber, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_receive"}, BrokerSubscriber.__abstractmethods__)

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
    def test_constructor(self):
        builder = BrokerSubscriberBuilder()
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual(None, builder.duplicate_detector_builder)
        self.assertEqual(QueuedBrokerSubscriber, builder.queued_cls)

    def test_with_queued_cls(self):
        # noinspection PyTypeChecker
        builder = BrokerSubscriberBuilder().with_queued_cls(int)
        self.assertEqual(int, builder.queued_cls)

    def test_with_idempotent_cls(self):
        # noinspection PyTypeChecker
        builder = BrokerSubscriberBuilder().with_idempotent_cls(int)
        self.assertEqual(int, builder.idempotent_cls)

    def test_constructor_with_queue_builder(self):
        queue_builder = InMemoryBrokerSubscriberQueueBuilder()
        builder = BrokerSubscriberBuilder(queue_builder=queue_builder)
        self.assertEqual(queue_builder, builder.queue_builder)
        self.assertEqual(QueuedBrokerSubscriber, builder.queued_cls)

    def test_constructor_with_duplicate_detector(self):
        idempotent_builder = Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector)
        builder = BrokerSubscriberBuilder(idempotent_builder=idempotent_builder)
        self.assertEqual(idempotent_builder, builder.duplicate_detector_builder)
        self.assertEqual(QueuedBrokerSubscriber, builder.queued_cls)

    def test_with_config_none(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(side_effect=MinosConfigException(""))
        config.get_interface_by_name = mock

        builder = BrokerSubscriberBuilder().with_config(config)
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({}, builder.kwargs)

    def test_with_config_empty(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(return_value={"subscriber": {}})
        config.get_interface_by_name = mock

        builder = BrokerSubscriberBuilder().with_config(config)
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({}, builder.kwargs)

    def test_with_config(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(
            return_value={
                "subscriber": {
                    "queue": InMemoryBrokerSubscriberQueue,
                    "idempotent": InMemoryBrokerSubscriberDuplicateDetector,
                }
            }
        )
        config.get_interface_by_name = mock

        builder = BrokerSubscriberBuilder().with_config(config)
        self.assertEqual(InMemoryBrokerSubscriberQueueBuilder(), builder.queue_builder)
        self.assertEqual(
            Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector), builder.duplicate_detector_builder
        )
        self.assertEqual({}, builder.kwargs)

    def test_with_queue_with_config(self):
        config = Config(CONFIG_FILE_PATH)

        builder = BrokerSubscriberBuilder().with_queue(InMemoryBrokerSubscriberQueue).with_config(config)
        self.assertEqual({}, builder.kwargs)
        self.assertEqual(InMemoryBrokerSubscriberQueueBuilder(), builder.queue_builder)

    def test_with_duplicate_with_config(self):
        config = Config(CONFIG_FILE_PATH)

        builder = (
            BrokerSubscriberBuilder()
            .with_duplicate_detector(InMemoryBrokerSubscriberDuplicateDetector)
            .with_config(config)
        )
        self.assertEqual({}, builder.kwargs)
        self.assertEqual(
            Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector), builder.duplicate_detector_builder
        )

    def test_with_kwargs(self):
        builder = BrokerSubscriberBuilder().with_kwargs({"foo": "bar"})
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_queue_with_kwargs(self):
        builder = BrokerSubscriberBuilder().with_queue(InMemoryBrokerSubscriberQueue).with_kwargs({"foo": "bar"})
        self.assertEqual(InMemoryBrokerSubscriberQueueBuilder().with_kwargs({"foo": "bar"}), builder.queue_builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_duplicate_detector_with_kwargs(self):
        builder = (
            BrokerSubscriberBuilder()
            .with_duplicate_detector(InMemoryBrokerSubscriberDuplicateDetector)
            .with_kwargs({"foo": "bar"})
        )
        self.assertEqual(
            Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector).with_kwargs({"foo": "bar"}),
            builder.duplicate_detector_builder,
        )
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_queue_cls(self):
        queue_builder = InMemoryBrokerSubscriberQueueBuilder()
        builder = BrokerSubscriberBuilder().with_queue(InMemoryBrokerSubscriberQueue)
        self.assertEqual(queue_builder, builder.queue_builder)

    def test_with_queue_builder(self):
        queue_builder = InMemoryBrokerSubscriberQueueBuilder()
        builder = BrokerSubscriberBuilder().with_queue(queue_builder)
        self.assertEqual(queue_builder, builder.queue_builder)

    def test_with_duplicate_detector_cls(self):
        duplicate_detector_builder = Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector)
        builder = BrokerSubscriberBuilder().with_duplicate_detector(InMemoryBrokerSubscriberDuplicateDetector)
        self.assertEqual(duplicate_detector_builder, builder.duplicate_detector_builder)

    def test_with_duplicate_detector_builder(self):
        duplicate_detector_builder = Builder().with_cls(InMemoryBrokerSubscriberDuplicateDetector)
        builder = BrokerSubscriberBuilder().with_duplicate_detector(duplicate_detector_builder)
        self.assertEqual(duplicate_detector_builder, builder.duplicate_detector_builder)

    def test_build(self):
        subscriber = BrokerSubscriberBuilder().with_topics({"one", "two"}).with_cls(InMemoryBrokerSubscriber).build()

        self.assertIsInstance(subscriber, InMemoryBrokerSubscriber)

    def test_build_with_queue(self):
        subscriber = (
            BrokerSubscriberBuilder()
            .with_cls(InMemoryBrokerSubscriber)
            .with_queue(InMemoryBrokerSubscriberQueue)
            .with_topics({"one", "two"})
            .build()
        )

        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, InMemoryBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, InMemoryBrokerSubscriberQueue)

    def test_build_with_duplicate_detector(self):
        subscriber = (
            BrokerSubscriberBuilder()
            .with_cls(InMemoryBrokerSubscriber)
            .with_duplicate_detector(InMemoryBrokerSubscriberDuplicateDetector)
            .with_topics({"one", "two"})
            .build()
        )

        self.assertIsInstance(subscriber, IdempotentBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, InMemoryBrokerSubscriber)
        self.assertIsInstance(subscriber.duplicate_detector, InMemoryBrokerSubscriberDuplicateDetector)

    def test_build_with_duplicate_detector_with_queue(self):
        subscriber = (
            BrokerSubscriberBuilder()
            .with_cls(InMemoryBrokerSubscriber)
            .with_duplicate_detector(InMemoryBrokerSubscriberDuplicateDetector)
            .with_queue(InMemoryBrokerSubscriberQueue)
            .with_topics({"one", "two"})
            .build()
        )
        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, InMemoryBrokerSubscriberQueue)

        self.assertIsInstance(subscriber.impl, IdempotentBrokerSubscriber)
        self.assertIsInstance(subscriber.impl.impl, InMemoryBrokerSubscriber)
        self.assertIsInstance(subscriber.impl.duplicate_detector, InMemoryBrokerSubscriberDuplicateDetector)

    def test_with_group_id(self):
        builder = BrokerSubscriberBuilder().with_group_id("foobar")
        self.assertIsInstance(builder, BrokerSubscriberBuilder)
        self.assertEqual({"group_id": "foobar"}, builder.kwargs)

    def test_with_remove_topics_on_destroy(self):
        builder = BrokerSubscriberBuilder().with_remove_topics_on_destroy(False)
        self.assertIsInstance(builder, BrokerSubscriberBuilder)
        self.assertEqual({"remove_topics_on_destroy": False}, builder.kwargs)

    def test_with_topics(self):
        builder = BrokerSubscriberBuilder().with_topics({"one", "two"})
        self.assertIsInstance(builder, BrokerSubscriberBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

    def test_with_topics_with_queue(self):
        builder = BrokerSubscriberBuilder().with_queue(InMemoryBrokerSubscriberQueue).with_topics({"one", "two"})
        self.assertIsInstance(builder, BrokerSubscriberBuilder)
        self.assertEqual(InMemoryBrokerSubscriberQueueBuilder().with_topics({"one", "two"}), builder.queue_builder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)


if __name__ == "__main__":
    unittest.main()
