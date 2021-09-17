"""tests.test_networks.tests_handlers.test_consumers module."""

import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from psycopg2.sql import (
    SQL,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    Consumer,
)
from tests.utils import (
    BASE_PATH,
    FakeConsumer,
    Message,
)


class TestConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.client = FakeConsumer([Message(topic="AddOrder", partition=0, value=b"test")])
        self.consumer = Consumer(
            topics={f"{self.config.service.name}Reply"},
            broker=self.config.broker,
            client=self.client,
            **self.config.broker.queue._asdict(),
        )

    def test_from_config(self):
        expected_topics = {
            "AddOrder",
            "DeleteOrder",
            "GetOrder",
            "OrderReply",
            "TicketAdded",
            "TicketDeleted",
            "UpdateOrder",
        }

        consumer = Consumer.from_config(config=self.config)
        self.assertIsInstance(consumer, Consumer)
        self.assertEqual(expected_topics, consumer.topics)
        self.assertEqual(self.config.broker, consumer._broker)
        self.assertEqual(self.config.broker.queue.host, consumer.host)
        self.assertEqual(self.config.broker.queue.port, consumer.port)
        self.assertEqual(self.config.broker.queue.database, consumer.database)
        self.assertEqual(self.config.broker.queue.user, consumer.user)
        self.assertEqual(self.config.broker.queue.password, consumer.password)

    def test_empty_topics(self):
        consumer = Consumer(broker=self.config.broker, client=self.client, **self.config.broker.queue._asdict())
        self.assertEqual(set(), consumer.topics)

    def test_topics(self):
        self.assertEqual({"OrderReply"}, self.consumer.topics)

    async def test_add_topic(self):
        mock = MagicMock()
        self.consumer.client.subscribe = mock
        await self.consumer.add_topic("foo")
        self.assertEqual({"foo", "OrderReply"}, self.consumer.topics)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(topics=list(self.consumer.topics)), mock.call_args)

    async def test_remove_topic(self):
        mock = MagicMock()
        self.consumer.client.subscribe = mock

        await self.consumer.add_topic("AddOrder")
        mock.reset_mock()

        await self.consumer.remove_topic("AddOrder")

        self.assertEqual({"OrderReply"}, self.consumer.topics)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(topics=list(self.consumer.topics)), mock.call_args)

    async def test_remove_all_topics(self):
        mock = MagicMock()
        self.consumer.client.unsubscribe = mock

        await self.consumer.remove_topic("OrderReply")
        self.assertEqual(set(), self.consumer.topics)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(), mock.call_args)

    async def test_dispatch(self):
        mock = MagicMock(side_effect=self.consumer.handle_message)
        self.consumer.handle_message = mock
        async with self.consumer:
            await self.consumer.dispatch()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.consumer.client), mock.call_args)

    async def tests_handle_message(self):
        mock = MagicMock(side_effect=self.consumer.handle_single_message)

        self.consumer.handle_single_message = mock
        async with self.consumer:
            await self.consumer.dispatch()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.consumer.client.messages[0]), mock.call_args)

    async def test_handle_single_message(self):
        mock = MagicMock(side_effect=self.consumer.enqueue)

        self.consumer.enqueue = mock
        async with self.consumer:
            await self.consumer.handle_single_message(Message(topic="AddOrder", partition=0, value=b"test"))

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("AddOrder", 0, b"test"), mock.call_args)

    async def test_enqueue(self):
        query = SQL(
            "INSERT INTO consumer_queue (topic, partition_id, binary_data) " "VALUES (%s, %s, %s) " "RETURNING id"
        )

        mock = MagicMock(side_effect=self.consumer.submit_query_and_fetchone)

        self.consumer.submit_query_and_fetchone = mock
        async with self.consumer:
            await self.consumer.enqueue("AddOrder", 0, b"test")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(query, ("AddOrder", 0, b"test")), mock.call_args)


if __name__ == "__main__":
    unittest.main()
