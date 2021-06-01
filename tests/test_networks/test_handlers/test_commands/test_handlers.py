import asyncio
import datetime
import unittest
from collections import (
    namedtuple,
)

import aiopg

from minos.common import (
    Command,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandHandler,
    MinosNetworkException,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    NaiveAggregate,
)


class TestCommandHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandHandler)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandHandler.from_config()

    async def test_if_queue_table_exists(self):
        async with CommandHandler.from_config(config=self.config):
            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 "
                        "FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = 'command_queue';"
                    )
                    ret = []
                    async for row in cur:
                        ret.append(row)

            assert ret == [(1,)]

    async def test_get_event_handler(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Command(
            topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        m = CommandHandler.from_config(config=self.config)

        cls = m.get_event_handler(topic=event_instance.topic)
        result = await cls(topic=event_instance.topic, command=event_instance)

        assert result == "add_order"

    async def test_non_implemented_action(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(
            topic="NotExisting", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="UpdateTicket",
        )
        m = CommandHandler.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = m.get_event_handler(topic=instance.topic)
            await cls(topic=instance.topic, command=instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_event_dispatch(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(
            topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij", reply_topic="UpdateTicket",
        )

        broker = FakeBroker()

        async with CommandHandler.from_config(config=self.config, broker=broker) as handler:
            queue_id = await self._insert_one(instance)
            await handler.dispatch()
            self.assertTrue(await self._is_processed(queue_id))

        self.assertEqual(1, broker.call_count)
        self.assertEqual("add_order", broker.items)
        self.assertEqual("UpdateTicket", broker.topic)
        self.assertEqual("43434jhij", broker.saga_uuid)
        self.assertEqual(None, broker.reply_topic)

    async def test_event_dispatch_without_reply(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij",)

        broker = FakeBroker()

        async with CommandHandler.from_config(config=self.config, broker=broker) as handler:
            queue_id = await self._insert_one(instance)
            await handler.dispatch()
            self.assertTrue(await self._is_processed(queue_id))

        self.assertEqual(0, broker.call_count)

    async def test_event_dispatch_wrong_event(self):
        instance = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))

        async with CommandHandler.from_config(config=self.config) as handler:
            queue_id = await self._insert_one(instance)
            await handler.dispatch()
            self.assertFalse(await self._is_processed(queue_id))

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id;",
                    (instance.topic, 0, instance.avro_bytes, datetime.datetime.now(),),
                )
                return (await cur.fetchone())[0]

    async def _is_processed(self, queue_id):
        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM command_queue WHERE id=%d" % (queue_id,))
                return (await cur.fetchone())[0] == 0

    async def test_command_handler_dispatch_wrong_event(self):
        async with CommandHandler.from_config(config=self.config) as handler:
            bin_data = bytes(b"Test")

            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        ("AddOrder", 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()

            assert queue_id[0] > 0

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            assert records[0] == 1

            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT * FROM command_queue WHERE id=%d" % (queue_id))
                    pending_row = await cur.fetchone()

            # Retry attempts
            assert pending_row[4] == 1

    async def test_concurrency_dispatcher(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(
            topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij", reply_topic="UpdateTicket",
        )
        bin_data = instance.avro_bytes

        broker = FakeBroker()
        bin_data_wrong = bytes(b"Test")

        async with CommandHandler.from_config(config=self.config, broker=broker) as handler:
            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    for x in range(0, 25):
                        await cur.execute(
                            "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data, datetime.datetime.now(),),
                        )
                        await cur.execute(
                            "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data_wrong, datetime.datetime.now(),),
                        )

            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_queue")
                    records = await cur.fetchone()

            assert records[0] == 50

            slow_task_1 = asyncio.create_task(handler.dispatch())
            slow_task_2 = asyncio.create_task(handler.dispatch())
            slow_task_3 = asyncio.create_task(handler.dispatch())
            slow_task_4 = asyncio.create_task(handler.dispatch())
            slow_task_5 = asyncio.create_task(handler.dispatch())
            slow_task_6 = asyncio.create_task(handler.dispatch())

            await slow_task_1
            await slow_task_4
            await slow_task_2
            await slow_task_6
            await slow_task_3
            await slow_task_5

            async with aiopg.connect(**self.commands_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_queue")
                    records = await cur.fetchone()

            assert records[0] == 25


if __name__ == "__main__":
    unittest.main()
