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
    PostgreSqlBrokerQueue,
    PostgreSqlBrokerSubscriberQueue,
    PostgreSqlBrokerSubscriberQueueQueryFactory,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlBrokerSubscriberQueue(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerSubscriberQueue, (PostgreSqlBrokerQueue, BrokerSubscriberQueue)))

    async def test_query_factory(self):
        queue = PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"})

        self.assertIsInstance(queue.query_factory, PostgreSqlBrokerSubscriberQueueQueryFactory)

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(message)
            await sleep(0.5)  # To give time to consume the message from db.

    async def test_dequeue_with_count(self):
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

    async def test_dequeue_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        async with PostgreSqlBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(messages[0])
            await queue.enqueue(messages[1])

            observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)


class TestPostgreSqlBrokerSubscriberQueueQueryFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = PostgreSqlBrokerSubscriberQueueQueryFactory()

    def test_build_table_name(self):
        self.assertEqual("broker_subscriber_queue", self.factory.build_table_name())


if __name__ == "__main__":
    unittest.main()
