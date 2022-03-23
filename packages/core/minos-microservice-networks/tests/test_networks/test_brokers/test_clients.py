import unittest
from asyncio import (
    gather,
    sleep,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    NotProvidedException,
    SetupMixin,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerClient,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
    MinosHandlerNotFoundEnoughEntriesException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestBrokerClient(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.topic = "fooReply"
        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.subscriber_builder = InMemoryBrokerSubscriberBuilder()
        self.broker = BrokerClient.from_config(
            self.config, topic=self.topic, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.broker.setup()

    async def asyncTearDown(self):
        await self.broker.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            BrokerClient.from_config(self.config)
        with self.assertRaises(NotProvidedException):
            BrokerClient.from_config(self.config, publisher=self.publisher)

    async def test_setup_destroy(self):
        broker = BrokerClient.from_config(
            self.config, topic=self.topic, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )
        self.assertFalse(broker.already_setup)
        async with broker:
            self.assertTrue(broker.already_setup)
        self.assertTrue(broker.already_destroyed)

    def test_base_classes(self):
        self.assertIsInstance(self.broker, SetupMixin)

    async def test_send(self):
        mock = AsyncMock()
        self.publisher.send = mock
        message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        await self.broker.send(message)

        expected = BrokerMessageV1(
            "AddFoo", BrokerMessageV1Payload(56), reply_topic=self.topic, identifier=message.identifier
        )
        self.assertEqual([call(expected)], mock.call_args_list)

    async def test_receive(self):
        expected = FakeModel("test1")
        # noinspection PyUnresolvedReferences
        self.broker.subscriber.add_message(FakeModel("test1"))
        # noinspection PyUnresolvedReferences
        self.broker.subscriber.add_message(FakeModel("test2"))

        observed = await self.broker.receive()

        self.assertEqual(expected, observed)

    async def test_receive_many(self):
        expected = [
            FakeModel("test1"),
            FakeModel("test2"),
            FakeModel("test3"),
            FakeModel("test4"),
        ]

        async def _fn1():
            messages = list()
            async for message in self.broker.receive_many(count=4, max_wait=0.1):
                messages.append(message)
            return messages

        async def _fn2():
            # noinspection PyUnresolvedReferences
            self.broker.subscriber.add_message(FakeModel("test1"))
            # noinspection PyUnresolvedReferences
            self.broker.subscriber.add_message(FakeModel("test2"))
            await sleep(0.5)
            # noinspection PyUnresolvedReferences
            self.broker.subscriber.add_message(FakeModel("test3"))
            # noinspection PyUnresolvedReferences
            self.broker.subscriber.add_message(FakeModel("test4"))

        observed, _ = await gather(_fn1(), _fn2())

        self.assertEqual(expected, observed)

    async def test_receive_many_raises(self):
        with self.assertRaises(MinosHandlerNotFoundEnoughEntriesException):
            async for _ in self.broker.receive_many(count=3, timeout=0.1):
                pass


if __name__ == "__main__":
    unittest.main()
