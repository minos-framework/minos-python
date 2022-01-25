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
    BrokerSubscriberQueue,
    PostgreSqlBrokerSubscriberQueue,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlBrokerSubscriberQueue(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerSubscriberQueue, BrokerSubscriberQueue))

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(message)
            await sleep(0.5)  # To give time to consume the message from db.

    async def test_iter(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        queue = PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"})
        await queue.setup()
        await queue.enqueue(messages[0])
        await queue.enqueue(messages[1])

        observed = list()
        async for message in queue:
            observed.append(message)
            if len(messages) == len(observed):
                await queue.destroy()

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
            async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
                queue._get_count = AsyncMock(side_effect=[3, 0])

                async with queue:
                    observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_run_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(messages[0])
            await queue.enqueue(messages[1])

            observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_run_with_order(self):
        unsorted = [
            BrokerMessageV1("foo", BrokerMessageV1Payload(4)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(2)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(3)),
            BrokerMessageV1("foo", BrokerMessageV1Payload(1)),
        ]

        async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:

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
