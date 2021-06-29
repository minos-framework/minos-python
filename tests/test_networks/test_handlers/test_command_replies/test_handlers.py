import asyncio
import datetime
import unittest

import aiopg

from minos.common import (
    CommandReply,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyHandler,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    FakeSagaManager,
)


class TestCommandReplyHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyHandler)

    async def test_dispatch(self):
        model = FakeModel("foo")
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

    async def test_dispatch_wrong(self):
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

    async def test_dispatch_concurrent(self):
        # Correct instance
        model = FakeModel("foo")
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
