import unittest
from asyncio import (
    sleep,
)
from itertools import (
    chain,
    cycle,
)
from unittest.mock import (
    patch,
)

from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriberQueue,
    BrokerSubscriberQueueDatabaseOperationFactory,
    DatabaseBrokerQueue,
    DatabaseBrokerSubscriberQueue,
    DatabaseBrokerSubscriberQueueBuilder,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeDatabaseClient,
    NetworksTestCase,
)


class TestDatabaseBrokerSubscriberQueue(NetworksTestCase, DatabaseMinosTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerSubscriberQueue, (DatabaseBrokerQueue, BrokerSubscriberQueue)))

    async def test_operation_factory(self):
        queue = DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"})

        self.assertIsInstance(queue.operation_factory, BrokerSubscriberQueueDatabaseOperationFactory)

    async def test_enqueue(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        with patch.object(
            FakeDatabaseClient,
            "fetch_all",
            side_effect=chain(
                [FakeAsyncIterator([(0,)]), FakeAsyncIterator([(1, message.avro_bytes)])],
                cycle([FakeAsyncIterator([(0,)])]),
            ),
        ):
            async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
                await queue.enqueue(message)
                await sleep(0.5)  # To give time to consume the message from db.

    async def test_dequeue_with_count(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        with patch.object(
            FakeDatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator(
                    [
                        [2],
                    ]
                ),
                FakeAsyncIterator([[1, messages[0].avro_bytes], [2, bytes()], [3, messages[1].avro_bytes]]),
                FakeAsyncIterator([(0,)]),
            ],
        ):
            async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
                async with queue:
                    observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)

    async def test_dequeue_with_notify(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]
        with patch.object(
            FakeDatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([(0,)]),
                FakeAsyncIterator([(1, messages[0].avro_bytes), (3, messages[1].avro_bytes)]),
                FakeAsyncIterator([(0,)]),
            ],
        ):
            async with DatabaseBrokerSubscriberQueue.from_config(self.config, topics={"foo", "bar"}) as queue:
                await queue.enqueue(messages[0])
                await queue.enqueue(messages[1])

                observed = [await queue.dequeue(), await queue.dequeue()]

        self.assertEqual(messages, observed)


class TestDatabaseBrokerSubscriberQueueBuilder(NetworksTestCase, DatabaseMinosTestCase):
    def test_build(self):
        builder = DatabaseBrokerSubscriberQueueBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, DatabaseBrokerSubscriberQueue)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()
