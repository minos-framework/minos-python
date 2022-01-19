import unittest
from asyncio import (
    sleep,
)
from unittest.mock import (
    AsyncMock,
    call,
    patch,
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
)


class TestPostgreSqlBrokerPublisherRepository(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerPublisherRepository, BrokerPublisherRepository))

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:
            await repository.enqueue(message)

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:
            await repository.enqueue(messages[0])
            await repository.enqueue(messages[1])

            observed = list()
            async for message in repository:
                observed.append(message)
                if len(messages) == len(observed):
                    break

        self.assertEqual(messages, observed)

    async def test_run_with_count(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        put_mock = AsyncMock()

        with patch(
            "aiopg.Cursor.fetchall",
            return_value=[
                [None, None, messages[0].avro_bytes],
                [None, None, bytes()],
                [None, None, messages[1].avro_bytes],
            ],
        ):
            repository = PostgreSqlBrokerPublisherRepository.from_config(self.config)
            repository._queue.put = put_mock
            repository._get_count = AsyncMock(side_effect=[3, 0])

            async with repository:
                await sleep(0.5)

        self.assertEqual([call(messages[0]), call(messages[1])], put_mock.call_args_list)

    async def test_run_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        put_mock = AsyncMock()
        async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:
            repository._queue.put = put_mock

            await repository.enqueue(messages[0])
            await repository.enqueue(messages[1])

            await sleep(0.5)

        self.assertEqual([call(messages[0]), call(messages[1])], put_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
