import unittest
from asyncio import (
    sleep,
)
from unittest.mock import (
    AsyncMock,
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
            await sleep(0.5)  # To give time to consume the message from db.

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        repository = PostgreSqlBrokerPublisherRepository.from_config(self.config)
        await repository.setup()
        await repository.enqueue(messages[0])
        await repository.enqueue(messages[1])

        observed = list()
        async for message in repository:
            observed.append(message)
            if len(messages) == len(observed):
                await repository.destroy()

        self.assertEqual(messages, observed)

    async def test_run_with_count(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        with patch(
            "aiopg.Cursor.fetchall",
            return_value=[[1, messages[0].avro_bytes], [2, bytes()], [3, messages[1].avro_bytes]],
        ):
            async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:
                repository._get_count = AsyncMock(side_effect=[3, 0])

                async with repository:
                    observed = [await repository.dequeue(), await repository.dequeue()]

        self.assertEqual(messages, observed)

    async def test_run_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:
            await repository.enqueue(messages[0])
            await repository.enqueue(messages[1])

            observed = [await repository.dequeue(), await repository.dequeue()]

        self.assertEqual(messages, observed)

    async def test_run_with_order(self):
        unsorted = [
            BrokerMessageV1("foo", BrokerMessageV1Payload(4)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(2)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(3)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(1)),
        ]

        async with PostgreSqlBrokerPublisherRepository.from_config(self.config) as repository:

            for message in unsorted:
                await repository.enqueue(message)

            await sleep(0.5)
            observed = list()
            for _ in range(len(unsorted)):
                observed.append(await repository.dequeue())

        expected = [unsorted[3], unsorted[1], unsorted[2], unsorted[0]]

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
