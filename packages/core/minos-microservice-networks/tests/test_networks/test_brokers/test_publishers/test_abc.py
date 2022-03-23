import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    call, MagicMock,
)

from minos.common import (
    Builder,
    SetupMixin,
    Config, MinosConfigException,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    BrokerPublisherBuilder,
    InMemoryBrokerPublisherQueue,
    QueuedBrokerPublisher,
    InMemoryBrokerPublisher,
)
from tests.utils import CONFIG_FILE_PATH


class _BrokerPublisher(BrokerPublisher):
    """For testing purposes."""

    async def _send(self, message: BrokerMessage) -> None:
        """For testing purposes."""


class TestBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisher, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_send"}, BrokerPublisher.__abstractmethods__)

    async def test_send(self):
        publisher = _BrokerPublisher()
        mock = AsyncMock()
        publisher._send = mock

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        await publisher.send(message)

        self.assertEqual([call(message)], mock.call_args_list)


class TestBrokerPublisherBuilder(unittest.TestCase):
    def test_constructor(self):
        builder = BrokerPublisherBuilder()
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual(QueuedBrokerPublisher, builder.queued_cls)

    def test_with_queued_cls(self):
        # noinspection PyTypeChecker
        builder = BrokerPublisherBuilder().with_queued_cls(int)
        self.assertEqual(int, builder.queued_cls)

    def test_constructor_with_queue_builder(self):
        queue_builder = Builder().with_cls(InMemoryBrokerPublisherQueue)
        builder = BrokerPublisherBuilder(queue_builder=queue_builder)
        self.assertEqual(queue_builder, builder.queue_builder)
        self.assertEqual(QueuedBrokerPublisher, builder.queued_cls)

    def test_with_config_none(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(side_effect=MinosConfigException(""))
        config.get_interface_by_name = mock

        builder = BrokerPublisherBuilder().with_config(config)
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({}, builder.kwargs)

    def test_with_config_empty(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(return_value={"publisher": {}})
        config.get_interface_by_name = mock

        builder = BrokerPublisherBuilder().with_config(config)
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({}, builder.kwargs)

    def test_with_config(self):
        config = Config(CONFIG_FILE_PATH)

        mock = MagicMock(return_value={"publisher": {"queue": InMemoryBrokerPublisherQueue}})
        config.get_interface_by_name = mock

        builder = BrokerPublisherBuilder().with_config(config)
        self.assertEqual(Builder().with_cls(InMemoryBrokerPublisherQueue), builder.queue_builder)
        self.assertEqual({}, builder.kwargs)

    def test_with_queue_with_config(self):
        config = Config(CONFIG_FILE_PATH)

        builder = BrokerPublisherBuilder().with_queue(InMemoryBrokerPublisherQueue).with_config(config)
        self.assertEqual({}, builder.kwargs)
        self.assertEqual(Builder().with_cls(InMemoryBrokerPublisherQueue), builder.queue_builder)

    def test_with_kwargs(self):
        builder = BrokerPublisherBuilder().with_kwargs({"foo": "bar"})
        self.assertEqual(None, builder.queue_builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_queue_with_kwargs(self):
        builder = BrokerPublisherBuilder().with_queue(InMemoryBrokerPublisherQueue).with_kwargs({"foo": "bar"})
        self.assertEqual(
            Builder().with_cls(InMemoryBrokerPublisherQueue).with_kwargs({"foo": "bar"}), builder.queue_builder
        )
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_queue_cls(self):
        queue_builder = Builder().with_cls(InMemoryBrokerPublisherQueue)
        builder = BrokerPublisherBuilder().with_queue(InMemoryBrokerPublisherQueue)
        self.assertEqual(queue_builder, builder.queue_builder)

    def test_with_queue_builder(self):
        queue_builder = Builder().with_cls(InMemoryBrokerPublisherQueue)
        builder = BrokerPublisherBuilder().with_queue(queue_builder)
        self.assertEqual(queue_builder, builder.queue_builder)

    def test_build(self):
        publisher = BrokerPublisherBuilder().with_cls(InMemoryBrokerPublisher).build()

        self.assertIsInstance(publisher, InMemoryBrokerPublisher)

    def test_build_with_queue(self):
        publisher = (
            BrokerPublisherBuilder().with_cls(InMemoryBrokerPublisher).with_queue(InMemoryBrokerPublisherQueue).build()
        )

        self.assertIsInstance(publisher, QueuedBrokerPublisher)
        self.assertIsInstance(publisher.impl, InMemoryBrokerPublisher)
        self.assertIsInstance(publisher.queue, InMemoryBrokerPublisherQueue)


if __name__ == "__main__":
    unittest.main()
