"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    MagicMock,
)

import aiopg
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    CommandReply,
    MinosConfig,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyBroker,
    Producer,
)
from tests.utils import (
    BASE_PATH,
    Foo,
)


class TestCommandReplyBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_with_args(self):
        broker = CommandReplyBroker.from_config("CommandBroker", saga_uuid="9347839473kfslf", config=self.config)
        self.assertIsInstance(broker, CommandReplyBroker)

    def test_from_config_default(self):
        broker = CommandReplyBroker.from_config(config=self.config)
        self.assertIsInstance(broker, CommandReplyBroker)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandReplyBroker.from_config()

    async def test_send_one(self):
        saga_uuid = "9347839473kfslf"
        topic = "CommandBroker"
        query = SQL(
            "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "RETURNING id"
        )
        item = Foo("test")

        async def _fn(*args, **kwargs):
            return (56,)

        mock = MagicMock(side_effect=_fn)

        async with CommandReplyBroker.from_config(config=self.config) as broker:
            broker.submit_query_and_fetchone = mock
            identifier = await broker.send_one(item, saga_uuid=saga_uuid, topic=topic)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(query, args[0])
        self.assertEqual("CommandBrokerReply", args[1][0])
        self.assertEqual(
            CommandReply(topic=f"{topic}Reply", items=[item], saga_uuid=saga_uuid),
            CommandReply.from_avro_bytes(args[1][1]),
        )
        self.assertEqual(0, args[1][2])
        self.assertEqual("commandReply", args[1][3])
        self.assertIsInstance(args[1][4], datetime)
        self.assertIsInstance(args[1][5], datetime)

    async def test_if_commands_was_deleted(self):
        item = Foo("test")

        async with CommandReplyBroker.from_config(
            "TestDeleteReply", config=self.config, saga_uuid="9347839473kfslf"
        ) as broker:
            queue_id_1 = await broker.send_one(item)
            queue_id_2 = await broker.send_one(item)

        await Producer.from_config(config=self.config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteReply")
                records = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_commands_retry_was_incremented(self):
        item = Foo("test")

        async with CommandReplyBroker.from_config(
            "TestDeleteOrder", config=self.config, saga_uuid="9347839473kfslf"
        ) as broker:
            queue_id_1 = await broker.send_one(item)
            queue_id_2 = await broker.send_one(item)

        config = MinosConfig(
            path=BASE_PATH / "wrong_test_config.yml",
            events_queue_database=self.config.events.queue.database,
            events_queue_user=self.config.events.queue.user,
        )
        await Producer.from_config(config=config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteOrderReply")
                records = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_1)
                retry_1 = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_2)
                retry_2 = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 2
        assert retry_1[0] > 0
        assert retry_2[0] > 0


if __name__ == "__main__":
    unittest.main()
