import unittest
from asyncio import (
    sleep,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberBuilder,
    InMemoryBrokerSubscriberQueue,
    InMemoryBrokerSubscriberQueueBuilder,
    QueuedBrokerSubscriber,
    QueuedBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestQueuedBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        self.topics = {"foo", "bar"}
        self.impl = InMemoryBrokerSubscriber(self.topics)
        self.queue = InMemoryBrokerSubscriberQueue(self.topics)

    def test_is_subclass(self):
        self.assertTrue(issubclass(QueuedBrokerSubscriber, BrokerSubscriber))

    def test_impl(self):
        subscriber = QueuedBrokerSubscriber(self.impl, self.queue)
        self.assertEqual(self.impl, subscriber.impl)

    def test_queue(self):
        subscriber = QueuedBrokerSubscriber(self.impl, self.queue)
        self.assertEqual(self.queue, subscriber.queue)

    def test_init_raises(self):
        impl = InMemoryBrokerSubscriber({"foo"})
        queue = InMemoryBrokerSubscriberQueue({"bar"})

        with self.assertRaises(ValueError):
            QueuedBrokerSubscriber(impl, queue)

    async def test_setup_destroy(self):
        impl_setup_mock = AsyncMock()
        impl_destroy_mock = AsyncMock()
        queue_setup_mock = AsyncMock()
        queue_destroy_mock = AsyncMock()

        self.impl.setup = impl_setup_mock
        self.impl.destroy = impl_destroy_mock
        self.queue.setup = queue_setup_mock
        self.queue.destroy = queue_destroy_mock

        async with QueuedBrokerSubscriber(self.impl, self.queue):
            self.assertEqual(1, impl_setup_mock.call_count)
            self.assertEqual(0, impl_destroy_mock.call_count)
            self.assertEqual(1, queue_setup_mock.call_count)
            self.assertEqual(0, queue_destroy_mock.call_count)

            impl_setup_mock.reset_mock()
            impl_destroy_mock.reset_mock()
            queue_setup_mock.reset_mock()
            queue_destroy_mock.reset_mock()

        self.assertEqual(0, impl_setup_mock.call_count)
        self.assertEqual(1, impl_destroy_mock.call_count)
        self.assertEqual(0, queue_setup_mock.call_count)
        self.assertEqual(1, queue_destroy_mock.call_count)

    async def test_receive(self):
        dequeue_mock = AsyncMock(side_effect=self.messages)
        self.queue.dequeue = dequeue_mock
        async with QueuedBrokerSubscriber(self.impl, self.queue) as receive:
            self.assertEqual(self.messages[0], await receive.receive())
            self.assertEqual(self.messages[1], await receive.receive())

    async def test_run(self):
        receive_mock = AsyncMock(side_effect=self.messages)
        self.impl.receive = receive_mock

        enqueue_mock = AsyncMock()
        self.queue.enqueue = enqueue_mock

        async with QueuedBrokerSubscriber(self.impl, self.queue):
            await sleep(0.5)  # To give time to consume the message

        self.assertEqual([call(self.messages[0]), call(self.messages[1])], enqueue_mock.call_args_list)


class TestQueuedBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)
        self.impl_builder = InMemoryBrokerSubscriberBuilder()
        self.queue_builder = InMemoryBrokerSubscriberQueueBuilder()
        self._kwargs = {"impl_builder": self.impl_builder, "queue_builder": self.queue_builder}

    def test_with_kwargs(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.with_kwargs)
        queue_mock = MagicMock(side_effect=self.impl_builder.with_kwargs)
        self.impl_builder.with_kwargs = impl_mock
        self.queue_builder.with_kwargs = queue_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)

        self.assertEqual([call({"foo": "bar"})], impl_mock.call_args_list)
        self.assertEqual([call({"foo": "bar"})], queue_mock.call_args_list)

    def test_with_config(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.with_config)
        queue_mock = MagicMock(side_effect=self.impl_builder.with_config)
        self.impl_builder.with_config = impl_mock
        self.queue_builder.with_config = queue_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_config(self.config)
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)

        self.assertEqual([call(self.config)], impl_mock.call_args_list)
        self.assertEqual([call(self.config)], queue_mock.call_args_list)

    def test_with_group_id(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.with_group_id)
        self.impl_builder.with_group_id = impl_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_group_id("foobar")
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)
        self.assertEqual({"group_id": "foobar"}, builder.kwargs)

        self.assertEqual([call("foobar")], impl_mock.call_args_list)

    def test_with_remove_topics_on_destroy(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.with_remove_topics_on_destroy)
        self.impl_builder.with_remove_topics_on_destroy = impl_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_remove_topics_on_destroy(False)
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)
        self.assertEqual({"remove_topics_on_destroy": False}, builder.kwargs)

        self.assertEqual([call(False)], impl_mock.call_args_list)

    def test_with_topics(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.with_topics)
        queue_mock = MagicMock(side_effect=self.impl_builder.with_topics)
        self.impl_builder.with_topics = impl_mock
        self.queue_builder.with_topics = queue_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_topics({"one", "two"})
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)
        self.assertEqual({"topics": {"one", "two"}}, builder.kwargs)

        self.assertEqual([call({"one", "two"})], impl_mock.call_args_list)
        self.assertEqual([call({"one", "two"})], queue_mock.call_args_list)

    def test_build(self):
        impl_mock = MagicMock(side_effect=self.impl_builder.build)
        queue_mock = MagicMock(side_effect=self.impl_builder.build)
        self.impl_builder.build = impl_mock
        self.queue_builder.build = queue_mock

        builder = QueuedBrokerSubscriberBuilder(**self._kwargs).with_topics({"one", "two"})
        self.assertIsInstance(builder, QueuedBrokerSubscriberBuilder)
        subscriber = builder.build()
        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)

        self.assertEqual([call()], impl_mock.call_args_list)
        self.assertEqual([call()], queue_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
