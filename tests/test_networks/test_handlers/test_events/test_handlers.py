import asyncio
import datetime
import unittest

import aiopg

from minos.common import (
    Event,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventHandler,
    MinosNetworkException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestEventHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = EventHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, EventHandler)

    async def test_get_action(self):
        model = FakeModel("foo")
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        handler = EventHandler.from_config(config=self.config)

        cls = handler.get_action(topic="TicketAdded")
        result = await cls(topic="TicketAdded", event=event_instance)

        self.assertEqual("request_added", result)

    async def test_non_implemented_action(self):
        model = FakeModel("foo")
        event_instance = Event(topic="NotExisting", model=model.classname, items=[])
        handler = EventHandler.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = handler.get_action(topic=event_instance.topic)
            await cls(topic=event_instance.topic, event=event_instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_event_dispatch(self):
        async with EventHandler.from_config(config=self.config) as handler:
            model = FakeModel("foo")
            event_instance = Event(topic="TicketAdded", model=model.classname, items=[])
            bin_data = event_instance.avro_bytes
            Event.from_avro_bytes(bin_data)

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        (event_instance.topic, 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()

            self.assertGreater(queue_id[0], 0)

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            self.assertEqual(0, records[0])

    async def test_event_dispatch_wrong_event(self):
        async with EventHandler.from_config(config=self.config) as handler:
            bin_data = bytes(b"Test")

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        ("TicketAdded", 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()
            self.assertGreater(queue_id[0], 0)

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id = %s", (queue_id,))
                    records = await cur.fetchone()

            self.assertEqual(1, records[0])

            async with aiopg.connect(**self.saga_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT * FROM event_queue WHERE id=%s", (queue_id,))
                    pending_row = await cur.fetchone()

            # Retry attempts
            self.assertEqual(1, pending_row[4])

    async def test_concurrency_dispatcher(self):
        # Correct instance
        model = FakeModel("foo")
        instance = Event(topic="TicketAdded", model=model.classname, items=[])
        bin_data = instance.avro_bytes

        # Wrong instance
        bin_data_wrong = bytes(b"Test")

        async with EventHandler.from_config(config=self.config) as handler:
            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    for x in range(0, 25):
                        await cur.execute(
                            "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data, datetime.datetime.now(),),
                        )
                        await cur.execute(
                            "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "RETURNING id;",
                            (instance.topic, 0, bin_data_wrong, datetime.datetime.now(),),
                        )

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue")
                    records = await cur.fetchone()

            self.assertEqual(50, records[0])

            await asyncio.gather(*[handler.dispatch() for i in range(0, 6)])

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue")
                    records = await cur.fetchone()

            self.assertEqual(25, records[0])


if __name__ == "__main__":
    unittest.main()
