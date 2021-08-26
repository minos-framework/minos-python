"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from psycopg2.sql import (
    SQL,
    Identifier,
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


class _FakeConsumer(Consumer):
    TABLE_NAME = "fake"


class TestConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        consumer = FakeConsumer([Message(topic="AddOrder", partition=0, value=b"test")])
        self.consumer = _FakeConsumer(
            topics=[f"{item.name}Reply" for item in self.config.saga.items],
            broker=self.config.broker,
            consumer=consumer,
            **self.config.broker.queue._asdict(),
        )

    def test_topics(self):
        self.assertEqual({"AddOrderReply", "DeleteOrderReply"}, self.consumer.topics)

    def test_add_topic(self):
        mock = MagicMock()
        self.consumer._consumer.subscribe = mock
        self.consumer.add_topic("foo")
        self.assertEqual({"foo", "AddOrderReply", "DeleteOrderReply"}, self.consumer.topics)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(topics=list(self.consumer.topics)), mock.call_args)

    def test_remove_topic(self):
        mock = MagicMock()
        self.consumer._consumer.subscribe = mock

        self.consumer.remove_topic("AddOrderReply")

        self.assertEqual({"DeleteOrderReply"}, self.consumer.topics)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(topics=list(self.consumer.topics)), mock.call_args)

    def test_remove_all_topics(self):
        mock = MagicMock()
        self.consumer._consumer.unsubscribe = mock

        self.consumer.remove_topic("AddOrderReply")
        self.consumer.remove_topic("DeleteOrderReply")
        self.assertEqual(set(), self.consumer.topics)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(), mock.call_args)

    async def test_dispatch(self):
        mock = MagicMock(side_effect=self.consumer.handle_message)
        self.consumer.handle_message = mock
        async with self.consumer:
            await self.consumer.dispatch()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.consumer._consumer), mock.call_args)

    async def tests_handle_message(self):
        mock = MagicMock(side_effect=self.consumer.handle_single_message)

        self.consumer.handle_single_message = mock
        async with self.consumer:
            await self.consumer.dispatch()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.consumer._consumer.messages[0]), mock.call_args)

    async def test_handle_single_message(self):
        mock = MagicMock(side_effect=self.consumer.queue_add)

        self.consumer.queue_add = mock
        async with self.consumer:
            await self.consumer.handle_single_message(Message(topic="AddOrder", partition=0, value=b"test"))

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("AddOrder", 0, b"test"), mock.call_args)

    async def test_queue_add(self):
        query = SQL(
            "INSERT INTO {} (topic, partition_id, binary_data, creation_date) VALUES (%s, %s, %s, NOW()) RETURNING id"
        ).format(Identifier("fake"))

        mock = MagicMock(side_effect=self.consumer.submit_query_and_fetchone)

        self.consumer.submit_query_and_fetchone = mock
        async with self.consumer:
            await self.consumer.queue_add("AddOrder", 0, b"test")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(query, ("AddOrder", 0, b"test")), mock.call_args)


if __name__ == "__main__":
    unittest.main()
