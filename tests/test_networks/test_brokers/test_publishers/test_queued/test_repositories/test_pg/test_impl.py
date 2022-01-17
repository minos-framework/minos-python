import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherRepository,
    PostgreSqlBrokerPublisherRepository,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeAsyncIterator,
)


class TestPostgreSqlBrokerPublisherRepository(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def setUp(self) -> None:
        super().setUp()
        self.repository = PostgreSqlBrokerPublisherRepository.from_config(self.config)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.repository.setup()

    async def asyncTearDown(self) -> None:
        await self.repository.destroy()
        await super().asyncTearDown()

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerPublisherRepository, BrokerPublisherRepository))

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        await self.repository.enqueue(message)

    async def test_dequeue_all(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        get_count_mock = AsyncMock(side_effect=[2, InterruptedError])
        self.repository._get_count = get_count_mock

        await self.repository.enqueue(messages[0])
        await self.repository.enqueue(messages[1])

        with self.assertRaises(InterruptedError):
            observed = list()
            async for message in self.repository.dequeue_all():
                observed.append(message)

        self.assertEqual(messages, observed)

    async def test_dequeue_all_with_exception(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        get_count_mock = AsyncMock(side_effect=[2, 2, InterruptedError])
        self.repository._get_count = get_count_mock

        dispatch_one_mock = MagicMock(side_effect=[messages[0], ValueError, messages[1]])
        self.repository._dispatch_one = dispatch_one_mock

        await self.repository.enqueue(messages[0])
        await self.repository.enqueue(messages[1])

        with self.assertRaises(InterruptedError):
            observed = list()
            async for message in self.repository.dequeue_all():
                observed.append(message)

        self.assertEqual(messages, observed)

    async def test_dequeue_all_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        get_count = AsyncMock(side_effect=[2, 0, InterruptedError])
        self.repository._get_count = get_count

        await self.repository.enqueue(messages[0])

        with self.assertRaises(InterruptedError):
            observed = list()
            async for message in self.repository.dequeue_all():
                await self.repository.enqueue(messages[1])
                observed.append(message)

        self.assertEqual(messages, observed)

    async def test_dequeue_all_with_count(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        dequeue_batch = MagicMock(side_effect=[FakeAsyncIterator(messages), InterruptedError])
        self.repository._dequeue_batch = dequeue_batch

        await self.repository.enqueue(messages[0])

        with self.assertRaises(InterruptedError):
            observed = list()
            async for message in self.repository.dequeue_all():
                observed.append(message)

        self.assertEqual(messages, observed)


if __name__ == "__main__":
    unittest.main()
