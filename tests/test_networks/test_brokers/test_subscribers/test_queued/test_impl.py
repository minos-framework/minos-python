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
    InMemoryBrokerSubscriberRepository,
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
        self.repository = InMemoryBrokerSubscriberRepository(self.topics)

    def test_is_subclass(self):
        self.assertTrue(issubclass(QueuedBrokerSubscriber, BrokerSubscriber))

    def test_impl(self):
        subscriber = QueuedBrokerSubscriber(self.impl, self.repository)
        self.assertEqual(self.impl, subscriber.impl)

    def test_repository(self):
        subscriber = QueuedBrokerSubscriber(self.impl, self.repository)
        self.assertEqual(self.repository, subscriber.repository)

    def test_init_raises(self):
        impl = InMemoryBrokerSubscriber({"foo"})
        repository = InMemoryBrokerSubscriberRepository({"bar"})

        with self.assertRaises(ValueError):
            QueuedBrokerSubscriber(impl, repository)

    async def test_setup_destroy(self):
        impl_setup_mock = AsyncMock()
        impl_destroy_mock = AsyncMock()
        repository_setup_mock = AsyncMock()
        repository_destroy_mock = AsyncMock()

        self.impl.setup = impl_setup_mock
        self.impl.destroy = impl_destroy_mock
        self.repository.setup = repository_setup_mock
        self.repository.destroy = repository_destroy_mock

        async with QueuedBrokerSubscriber(self.impl, self.repository):
            self.assertEqual(1, impl_setup_mock.call_count)
            self.assertEqual(0, impl_destroy_mock.call_count)
            self.assertEqual(1, repository_setup_mock.call_count)
            self.assertEqual(0, repository_destroy_mock.call_count)

            impl_setup_mock.reset_mock()
            impl_destroy_mock.reset_mock()
            repository_setup_mock.reset_mock()
            repository_destroy_mock.reset_mock()

        self.assertEqual(0, impl_setup_mock.call_count)
        self.assertEqual(1, impl_destroy_mock.call_count)
        self.assertEqual(0, repository_setup_mock.call_count)
        self.assertEqual(1, repository_destroy_mock.call_count)

    async def test_receive(self):
        dequeue_mock = AsyncMock(side_effect=self.messages)
        self.repository.dequeue = dequeue_mock
        async with QueuedBrokerSubscriber(self.impl, self.repository) as receive:
            self.assertEqual(self.messages[0], await receive.receive())
            self.assertEqual(self.messages[1], await receive.receive())

    async def test_run(self):
        receive_mock = AsyncMock(side_effect=self.messages)
        self.impl.receive = receive_mock

        enqueue_mock = AsyncMock()
        self.repository.enqueue = enqueue_mock

        async with QueuedBrokerSubscriber(self.impl, self.repository):
            await sleep(0.5)  # To give time to consume the message

        self.assertEqual([call(self.messages[0]), call(self.messages[1])], enqueue_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
