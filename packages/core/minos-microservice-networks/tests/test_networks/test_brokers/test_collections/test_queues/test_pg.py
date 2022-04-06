import unittest
from asyncio import (
    sleep,
)
from unittest.mock import (
    AsyncMock,
    patch,
)

from minos.common import (
    AiopgDatabaseClient,
    DatabaseMixin,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    AiopgBrokerQueueDatabaseOperationFactory,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerQueue,
    DatabaseBrokerQueue,
)
from tests.utils import (
    FakeAsyncIterator,
    NetworksTestCase,
)


class _Aiopg_BrokerQueueDatabaseOperationFactory(AiopgBrokerQueueDatabaseOperationFactory):
    def build_table_name(self) -> str:
        """For testing purposes."""
        return "test_table"


class TestPostgreSqlBrokerQueue(NetworksTestCase, PostgresAsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.operation_factory = _Aiopg_BrokerQueueDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerQueue, (BrokerQueue, DatabaseMixin)))

    def test_constructor(self):
        queue = DatabaseBrokerQueue(operation_factory=self.operation_factory)
        self.assertEqual(self.pool_factory.get_pool("database"), queue.pool)
        self.assertEqual(self.operation_factory, queue.operation_factory)
        self.assertEqual(2, queue.retry)
        self.assertEqual(1000, queue.records)

    async def test_operation_factory(self):
        queue = DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory)

        self.assertEqual(self.operation_factory, queue.operation_factory)

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory) as queue:
            await queue.enqueue(message)
            await sleep(0.5)  # To give time to consume the message from db.

    async def test_aiter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        queue = DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory)
        await queue.setup()
        await queue.enqueue(messages[0])
        await queue.enqueue(messages[1])

        observed = list()
        async for message in queue:
            observed.append(message)
            if len(messages) == len(observed):
                await queue.destroy()

        self.assertEqual(messages, observed)

    async def test_dequeue_with_count(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        with patch.object(
            AiopgDatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator([[1, messages[0].avro_bytes], [2, bytes()], [3, messages[1].avro_bytes]]),
        ):
            async with DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory) as queue:
                queue._get_count = AsyncMock(side_effect=[3, 0])

                async with queue:
                    observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_dequeue_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        async with DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory) as queue:
            await queue.enqueue(messages[0])
            await queue.enqueue(messages[1])

            observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_dequeue_ordered(self):
        unsorted = [
            BrokerMessageV1("foo", BrokerMessageV1Payload(4)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(2)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(3)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(1)),
        ]

        async with DatabaseBrokerQueue.from_config(self.config, operation_factory=self.operation_factory) as queue:

            for message in unsorted:
                await queue.enqueue(message)

            await sleep(0.5)
            observed = list()
            for _ in range(len(unsorted)):
                observed.append(await queue.dequeue())

        expected = [unsorted[3], unsorted[1], unsorted[2], unsorted[0]]

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
