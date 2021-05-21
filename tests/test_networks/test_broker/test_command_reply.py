"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

import aiopg

from minos.common import (
    MinosConfig,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyBroker,
    ProducerDispatcher,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestMinosCommandReplyBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        broker = CommandReplyBroker.from_config(
            "CommandBroker", saga_id="9347839473kfslf", task_id="92839283hjijh232", config=self.config
        )
        self.assertIsInstance(broker, CommandReplyBroker)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandReplyBroker.from_config()

    async def test_commands_broker_insertion(self):
        broker = CommandReplyBroker.from_config(
            "CommandBroker", config=self.config, saga_id="9347839473kfslf", task_id="92839283hjijh232"
        )
        await broker.setup()

        item = NaiveAggregate(test_id=1, test=2, id=1, version=1)

        queue_id = await broker.send_one(item)
        assert queue_id > 0

    async def test_if_commands_was_deleted(self):
        broker = CommandReplyBroker.from_config(
            "CommandReplyBroker-Delete", config=self.config, saga_id="9347839473kfslf", task_id="92839283hjijh232"
        )
        await broker.setup()

        item = NaiveAggregate(test_id=1, test=2, id=1, version=1)

        queue_id_1 = await broker.send_one(item)
        queue_id_2 = await broker.send_one(item)

        await ProducerDispatcher.from_config(config=self.config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "CommandReplyBroker-Delete"
                )
                records = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_commands_retry_was_incremented(self):
        broker = CommandReplyBroker.from_config(
            "CommandReplyBroker-Delete", config=self.config, saga_id="9347839473kfslf", task_id="92839283hjijh232",
        )
        await broker.setup()

        item = NaiveAggregate(test_id=1, test=2, id=1, version=1)

        queue_id_1 = await broker.send_one(item)
        queue_id_2 = await broker.send_one(item)

        config = MinosConfig(
            path=BASE_PATH / "wrong_test_config.yml",
            events_queue_database=self.config.events.queue.database,
            events_queue_user=self.config.events.queue.user,
        )
        await ProducerDispatcher.from_config(config=config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "CommandReplyBroker-Delete"
                )
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
