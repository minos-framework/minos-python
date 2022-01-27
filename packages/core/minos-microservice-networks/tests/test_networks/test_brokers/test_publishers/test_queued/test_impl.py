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
    BrokerPublisher,
    InMemoryBrokerPublisher,
    InMemoryBrokerPublisherQueue,
    QueuedBrokerPublisher,
)


class TestQueuedBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.impl = InMemoryBrokerPublisher()
        self.queue = InMemoryBrokerPublisherQueue()

    def test_is_subclass(self):
        self.assertTrue(issubclass(QueuedBrokerPublisher, BrokerPublisher))

    def test_impl(self):
        publisher = QueuedBrokerPublisher(self.impl, self.queue)
        self.assertEqual(self.impl, publisher.impl)

    def test_queue(self):
        publisher = QueuedBrokerPublisher(self.impl, self.queue)
        self.assertEqual(self.queue, publisher.queue)

    async def test_setup_destroy(self):
        impl_setup_mock = AsyncMock()
        impl_destroy_mock = AsyncMock()
        queue_setup_mock = AsyncMock()
        queue_destroy_mock = AsyncMock()

        self.impl.setup = impl_setup_mock
        self.impl.destroy = impl_destroy_mock
        self.queue.setup = queue_setup_mock
        self.queue.destroy = queue_destroy_mock

        async with QueuedBrokerPublisher(self.impl, self.queue):
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

    async def test_send(self):
        queue_enqueue_mock = AsyncMock()
        self.queue.enqueue = queue_enqueue_mock

        publisher = QueuedBrokerPublisher(self.impl, self.queue)
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        await publisher.send(message)

        self.assertEqual([call(message)], queue_enqueue_mock.call_args_list)

    async def test_run(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        impl_send_mock = AsyncMock()
        self.impl.send = impl_send_mock

        async with QueuedBrokerPublisher(self.impl, self.queue) as publisher:
            await publisher.send(messages[0])
            await publisher.send(messages[1])

            await sleep(0.5)  # To give time to consume the message

        self.assertEqual([call(messages[0]), call(messages[1])], impl_send_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
