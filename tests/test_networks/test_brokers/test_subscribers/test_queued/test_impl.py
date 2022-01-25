import unittest
from asyncio import (
    sleep,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberQueue,
    QueuedBrokerSubscriber,
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


if __name__ == "__main__":
    unittest.main()
