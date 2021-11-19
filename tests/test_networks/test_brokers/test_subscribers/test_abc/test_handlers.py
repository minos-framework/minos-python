import unittest
from asyncio import (
    TimeoutError,
    gather,
    sleep,
    wait_for,
)
from collections import (
    namedtuple,
)
from inspect import (
    isawaitable,
)
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

import aiopg

from minos.common import (
    DataTransferObject,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerHandler,
    BrokerHandlerEntry,
    BrokerMessage,
    BrokerResponse,
    EnrouteBuilder,
    MinosActionNotFoundException,
)
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
    FakeRequest,
)


class _FakeBrokerHandler(BrokerHandler):
    ENTRY_MODEL_CLS = DataTransferObject

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.call_count = 0
        self.call_args = None

    async def dispatch_one(self, entry: BrokerHandlerEntry) -> None:
        """For testing purposes."""
        self.call_count += 1
        self.call_args = (entry,)
        if entry.topic == "DeleteOrder":
            raise ValueError()
        result = entry.callback(entry.data)
        if isawaitable(result):
            await result


class TestHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        handlers = self._get_handlers()
        handlers["empty"] = None
        self.handler = _FakeBrokerHandler(handlers=handlers, **self.config.broker.queue._asdict())

    def _get_handlers(self):
        decorators = EnrouteBuilder(*self.config.services).get_broker_command_query()
        handlers = {decorator.topic: fn for decorator, fn in decorators.items()}
        return handlers

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.handler.setup()

    async def asyncTearDown(self):
        await self.handler.destroy()
        await super().asyncTearDown()

    async def test_get_action(self):
        action = self.handler.get_action(topic="AddOrder")
        self.assertEqual(BrokerResponse("add_order"), await action(FakeRequest("test")))

    async def test_get_action_none(self):
        action = self.handler.get_action(topic="empty")
        self.assertIsNone(None, action)

    async def test_get_action_raises(self):
        with self.assertRaises(MinosActionNotFoundException) as context:
            self.handler.get_action(topic="NotExisting")

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_dispatch_forever(self):
        mock = AsyncMock(side_effect=ValueError)
        self.handler.dispatch = mock
        try:
            await gather(self.handler.dispatch_forever(), self._notify("empty"))
        except ValueError:
            pass
        self.assertEqual(1, mock.call_count)

    async def test_dispatch_forever_without_notify(self):
        mock_dispatch = AsyncMock(side_effect=[None, ValueError])
        mock_count = AsyncMock(side_effect=[1, 0, 1])
        self.handler.dispatch = mock_dispatch
        self.handler._get_count = mock_count
        try:
            await self.handler.dispatch_forever(max_wait=0.01)
        except ValueError:
            pass
        self.assertEqual(2, mock_dispatch.call_count)
        self.assertEqual(3, mock_count.call_count)

    async def test_dispatch_forever_without_topics(self):
        handler = _FakeBrokerHandler(handlers=dict(), **self.config.broker.queue._asdict())
        mock = AsyncMock()
        async with handler:
            handler.dispatch = mock
            try:
                await wait_for(handler.dispatch_forever(max_wait=0.1), 0.5)
            except TimeoutError:
                pass
        self.assertEqual(0, mock.call_count)

    async def test_dispatch(self):
        instance = BrokerMessage("AddOrder", FAKE_AGGREGATE_DIFF)

        queue_id = await self._insert_one(instance)
        await self.handler.dispatch()
        self.assertTrue(await self._is_processed(queue_id))

        self.assertEqual(1, self.handler.call_count)

    async def test_dispatch_wrong(self):
        instance_1 = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))
        instance_2 = BrokerMessage("DeleteOrder", FAKE_AGGREGATE_DIFF)
        instance_3 = BrokerMessage("NoActionTopic", FAKE_AGGREGATE_DIFF)

        queue_id_1 = await self._insert_one(instance_1)
        queue_id_2 = await self._insert_one(instance_2)
        queue_id_3 = await self._insert_one(instance_3)
        await self.handler.dispatch()
        self.assertFalse(await self._is_processed(queue_id_1))
        self.assertFalse(await self._is_processed(queue_id_2))
        self.assertFalse(await self._is_processed(queue_id_3))

    async def test_dispatch_concurrent(self):
        from tests.utils import (
            FakeModel,
        )

        saga = uuid4()

        instance = BrokerMessage("AddOrder", [FakeModel("foo")], saga=saga, reply_topic="UpdateTicket")
        instance_wrong = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))

        for _ in range(10):
            await self._insert_one(instance)
            await self._insert_one(instance_wrong)

        self.assertEqual(20, await self._count())

        await gather(*(self.handler.dispatch() for _ in range(2)))
        self.assertEqual(10, await self._count())

    async def _notify(self, name):
        await sleep(0.2)
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(f"NOTIFY {name!s};")

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id;",
                    (instance.topic, 0, instance.avro_bytes),
                )
                return (await cur.fetchone())[0]

    async def _count(self):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM consumer_queue")
                return (await cur.fetchone())[0]

    async def _is_processed(self, queue_id):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM consumer_queue WHERE id=%d" % (queue_id,))
                return (await cur.fetchone())[0] == 0


if __name__ == "__main__":
    unittest.main()
