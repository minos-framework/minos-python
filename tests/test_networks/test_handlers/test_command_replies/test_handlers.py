import datetime

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
    NaiveAggregate,
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
        handler = CommandReplyHandler.from_config(config=self.config)
        await handler.setup()

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

        assert ret == [(1,)]

    async def test_get_event_handler(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = CommandReply(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        m = CommandReplyHandler.from_config(config=self.config)

        cls = m.get_event_handler(topic=event_instance.topic)
        result = await cls(topic=event_instance.topic, command=event_instance)

        assert result == "add_order_saga"

    async def test_non_implemented_action(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = CommandReply(
            topic="NotExisting",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        m = CommandReplyHandler.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = m.get_event_handler(topic=instance.topic)
            await cls(topic=instance.topic, command=instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_event_dispatch(self):
        handler = CommandReplyHandler.from_config(config=self.config)
        await handler.setup()

        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = CommandReply(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        bin_data = instance.avro_bytes
        CommandReply.from_avro_bytes(bin_data)

        async with aiopg.connect(**self.saga_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO command_reply_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id;",
                    (instance.topic, 0, bin_data, datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()

        assert queue_id[0] > 0

        # Must get the record, call on_reply function and delete the record from DB
        await handler.dispatch()

        async with aiopg.connect(**self.saga_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM command_reply_queue WHERE id=%d" % (queue_id))
                records = await cur.fetchone()

        assert records[0] == 0

    async def test_command_reply_dispatch_wrong_event(self):
        handler = CommandReplyHandler.from_config(config=self.config)
        await handler.setup()

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

        assert queue_id[0] > 0

        # Must get the record, call on_reply function and delete the record from DB
        await handler.dispatch()

        async with aiopg.connect(**self.saga_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM command_reply_queue WHERE id=%d" % (queue_id))
                records = await cur.fetchone()

        assert records[0] == 1
