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
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    AiopgBrokerSubscriberQueueDatabaseOperationFactory,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberQueue,
    DatabaseBrokerQueue,
    DatabaseBrokerSubscriberQueue,
    DatabaseBrokerSubscriberQueueBuilder,
)
from tests.utils import (
    FakeAsyncIterator,
    NetworksTestCase,
)


class TestPostgreSqlBrokerSubscriberQueue(NetworksTestCase, DatabaseMinosTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerSubscriberQueue, (DatabaseBrokerQueue, BrokerSubscriberQueue)))

    async def test_operation_factory(self):
        queue = DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"})

        self.assertIsInstance(queue.operation_factory, AiopgBrokerSubscriberQueueDatabaseOperationFactory)

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(message)
            await sleep(0.5)  # To give time to consume the message from db.

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
            async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
                queue._get_count = AsyncMock(side_effect=[3, 0])

                async with queue:
                    observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_dequeue_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
            await queue.enqueue(messages[0])
            await queue.enqueue(messages[1])

            observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)


class TestPostgreSqlBrokerSubscriberQueueQueryFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerSubscriberQueueDatabaseOperationFactory()

    def test_build_table_name(self):
        self.assertEqual("broker_subscriber_queue", self.factory.build_table_name())


class TestPostgreSqlBrokerSubscriberQueueBuilder(NetworksTestCase, DatabaseMinosTestCase):
    def test_build(self):
        builder = DatabaseBrokerSubscriberQueueBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, DatabaseBrokerSubscriberQueue)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()
