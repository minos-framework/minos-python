import asyncio
import datetime
import unittest

import aiopg

from minos.common import (
    CommandReply,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyHandler,
    MinosNetworkException,
)
from tests.utils import (
    BASE_PATH,
    FakeSagaManager,
    Foo,
)


class TestCommandReplyHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyHandler)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandReplyHandler.from_config()

    async def test_if_queue_table_exists(self):
        async with CommandReplyHandler.from_config(config=self.config):
            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 "
                        "FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = 'command_reply_queue';"
                    )
                    ret = []
                    async for row in cur:
                        ret.append(row)

            self.assertEqual(ret, [(1,)])

    async def test_get_action(self):
        model = Foo("test")
        event_instance = CommandReply(
            topic="AddOrderReply", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        handler = CommandReplyHandler.from_config(config=self.config)

        cls = handler.get_action(topic=event_instance.topic)
        result = await cls(topic=event_instance.topic, command=event_instance)

        self.assertEqual(result, "add_order_saga")

    async def test_non_implemented_action(self):
        model = Foo("test")
        instance = CommandReply(
            topic="NotExisting", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        handler = CommandReplyHandler.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = handler.get_action(topic=instance.topic)
            await cls(topic=instance.topic, command=instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_event_dispatch(self):
        model = Foo("test")
        instance = CommandReply(
            topic="AddOrderReply", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        bin_data = instance.avro_bytes
        saga_manager = FakeSagaManager()

        async with CommandReplyHandler.from_config(config=self.config, saga_manager=saga_manager) as handler:
            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO command_reply_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        (instance.topic, 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()

            self.assertGreater(queue_id[0], 0)

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_reply_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            self.assertEqual(records[0], 0)

            self.assertEqual(None, saga_manager.name)
            self.assertEqual(instance, saga_manager.reply)

    async def test_command_reply_dispatch_wrong_event(self):
        async with CommandReplyHandler.from_config(config=self.config) as handler:
            bin_data = bytes(b"Test")

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO command_reply_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        ("AddOrder", 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()

            self.assertGreater(queue_id[0], 0)

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_reply_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            self.assertEqual(records[0], 1)

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT * FROM command_reply_queue WHERE id=%d" % (queue_id))
                    pending_row = await cur.fetchone()

            # Retry attempts
            self.assertEqual(pending_row[4], 1)

    async def test_concurrency_dispatcher(self):
        # Correct instance
        model = Foo("test")
        instance = CommandReply(
            topic="AddOrderReply", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        bin_data = instance.avro_bytes
        saga_manager = FakeSagaManager()

        # Wrong instance
        bin_data_wrong = bytes(b"Test")

        async with CommandReplyHandler.from_config(config=self.config, saga_manager=saga_manager) as handler:
            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    for x in range(0, 25):
                        await cur.execute(
                            "INSERT INTO command_reply_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data, datetime.datetime.now(),),
                        )
                        await cur.execute(
                            "INSERT INTO command_reply_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data_wrong, datetime.datetime.now(),),
                        )

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_reply_queue")
                    records = await cur.fetchone()

            self.assertEqual(records[0], 50)

            await asyncio.gather(*[handler.dispatch() for _ in range(0, 6)])

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM command_reply_queue")
                    records = await cur.fetchone()

            self.assertEqual(records[0], 25)


if __name__ == "__main__":
    unittest.main()
