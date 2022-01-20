import unittest
from collections import namedtuple
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)

from psycopg2.sql import SQL

from minos.common.testing import PostgresAsyncTestCase
from tests.utils import BASE_PATH

_ConsumerMessage = namedtuple("Message", ["topic", "partition", "value"])


class _ConsumerClient:
    """For testing purposes."""

    def __init__(self, messages=None):
        if messages is None:
            messages = [_ConsumerMessage(topic="TicketAdded", partition=0, value=bytes())]
        self.messages = messages

    async def start(self):
        """For testing purposes."""

    async def stop(self):
        """For testing purposes."""

    def subscribe(self, *args, **kwargs):
        """For testing purposes."""

    def unsubscribe(self):
        """For testing purposes."""

    async def getmany(self, *args, **kwargs):
        """For testing purposes."""
        return dict(enumerate(self.messages))

    async def __aiter__(self):
        for message in self.messages:
            yield message

    async def commit(self, *args, **kwargs):
        """For testing purposes."""


@unittest.skip("FIXME!")
class TestConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.client = _ConsumerClient([_ConsumerMessage("AddOrder", 0, b"test")])
        # noinspection PyTypeChecker
        self.consumer = BrokerConsumer(  # noqa
            topics={f"{self.config.service.name}Reply"},
            broker=self.config.broker,
            client=self.client,
            **self.config.broker.queue._asdict(),
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.consumer.setup()

    async def asyncTearDown(self):
        await self.consumer.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        expected_topics = {
            "AddOrder",
            "DeleteOrder",
            "GetOrder",
            "TicketAdded",
            "TicketDeleted",
            "UpdateOrder",
        }

        consumer = BrokerConsumer.from_config(config=self.config)  # noqa
        self.assertIsInstance(consumer, BrokerConsumer)  # noqa
        self.assertEqual(expected_topics, consumer.topics)
        self.assertEqual(self.config.broker, consumer._broker)
        self.assertEqual(self.config.broker.queue.host, consumer.host)
        self.assertEqual(self.config.broker.queue.port, consumer.port)
        self.assertEqual(self.config.broker.queue.database, consumer.database)
        self.assertEqual(self.config.broker.queue.user, consumer.user)
        self.assertEqual(self.config.broker.queue.password, consumer.password)

    def test_empty_topics(self):
        # noinspection PyTypeChecker
        consumer = BrokerConsumer(  # noqa
            broker=self.config.broker, client=self.client, **self.config.broker.queue._asdict()
        )
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
        await self.consumer.dispatch()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.consumer.client), mock.call_args)

    async def tests_handle_message(self):
        handle_single_message_mock = MagicMock(side_effect=self.consumer.handle_single_message)
        commit_mock = AsyncMock()

        self.client.commit = commit_mock
        self.consumer.handle_single_message = handle_single_message_mock
        await self.consumer.dispatch()

        self.assertEqual([call()], commit_mock.call_args_list)
        self.assertEqual([call(self.client.messages[0])], handle_single_message_mock.call_args_list)

    async def test_handle_single_message(self):
        mock = MagicMock(side_effect=self.consumer.enqueue)

        self.consumer.enqueue = mock
        await self.consumer.handle_single_message(_ConsumerMessage("AddOrder", 0, b"test"))

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("AddOrder", 0, b"test"), mock.call_args)

    async def test_enqueue(self):
        query = SQL("INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id")

        mock = MagicMock(side_effect=self.consumer.submit_query_and_fetchone)

        self.consumer.submit_query_and_fetchone = mock
        await self.consumer.enqueue("AddOrder", 0, b"test")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(query, ("AddOrder", 0, b"test")), mock.call_args)


if __name__ == "__main__":
    unittest.main()
