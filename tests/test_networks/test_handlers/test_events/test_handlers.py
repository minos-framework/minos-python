import unittest
from collections import (
    defaultdict,
)
from random import (
    shuffle,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

import aiopg

from minos.common import (
    Action,
    AggregateDiff,
    Event,
    FieldDiffContainer,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventHandler,
    HandlerEntry,
    HandlerRequest,
    HandlerResponseException,
    MinosActionNotFoundException,
    Request,
)
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request):
        await request.content()

    @staticmethod
    async def _fn_raises_response(request: Request):
        raise HandlerResponseException("")

    @staticmethod
    async def _fn_raises_minos(request: Request):
        raise MinosActionNotFoundException("")

    @staticmethod
    async def _fn_raises_exception(request: Request):
        raise ValueError


class TestEventHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.handler = EventHandler.from_config(config=self.config)
        self.event = Event("TicketAdded", FAKE_AGGREGATE_DIFF)

    def test_from_config(self):
        self.assertIsInstance(self.handler, EventHandler)

        self.assertEqual({"TicketAdded", "TicketDeleted"}, set(self.handler.handlers.keys()))

        self.assertEqual(self.config.broker.queue.records, self.handler._records)
        self.assertEqual(self.config.broker.queue.retry, self.handler._retry)
        self.assertEqual(self.config.broker.queue.host, self.handler.host)
        self.assertEqual(self.config.broker.queue.port, self.handler.port)
        self.assertEqual(self.config.broker.queue.database, self.handler.database)
        self.assertEqual(self.config.broker.queue.user, self.handler.user)
        self.assertEqual(self.config.broker.queue.password, self.handler.password)

    def test_entry_model_cls(self):
        self.assertEqual(Event, EventHandler.ENTRY_MODEL_CLS)

    async def test_dispatch_one(self):
        callback_mock = AsyncMock()
        lookup_mock = MagicMock(return_value=callback_mock)

        topic = "TicketAdded"
        event = Event(topic, FAKE_AGGREGATE_DIFF)
        entry = HandlerEntry(1, topic, 0, event.avro_bytes, 1, callback_lookup=lookup_mock)

        async with self.handler:
            await self.handler.dispatch_one(entry)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("TicketAdded"), lookup_mock.call_args)

        self.assertEqual(1, callback_mock.call_count)
        self.assertEqual(call(HandlerRequest(event)), callback_mock.call_args)

    async def test_get_callback(self):
        fn = self.handler.get_callback(_Cls._fn)
        await fn(self.event)

    async def test_get_callback_raises_response(self):
        fn = self.handler.get_callback(_Cls._fn_raises_response)
        await fn(self.event)

    async def test_get_callback_raises_minos(self):
        fn = self.handler.get_callback(_Cls._fn_raises_minos)
        await fn(self.event)

    async def test_get_callback_raises_exception(self):
        fn = self.handler.get_callback(_Cls._fn_raises_exception)
        await fn(self.event)

    async def test_dispatch_concurrent(self):
        observed = defaultdict(list)

        async def _fn2(request):
            diff = await request.content()
            observed[diff.uuid].append(diff.version)

        self.handler.get_action = MagicMock(return_value=_fn2)

        uuid1, uuid2 = uuid4(), uuid4()

        events = list()
        for i in range(1, 6):
            events.extend(
                [
                    Event("AddOrder", AggregateDiff(uuid1, "Foo", i, Action.CREATE, FieldDiffContainer.empty())),
                    Event("AddOrder", AggregateDiff(uuid2, "Foo", i, Action.CREATE, FieldDiffContainer.empty())),
                ]
            )
        shuffle(events)

        async with self.handler:
            for event in events:
                await self._insert_one(event)

            await self.handler.dispatch()

        expected = {uuid1: list(range(1, 6)), uuid2: list(range(1, 6))}
        self.assertEqual(expected, observed)

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, NOW()) "
                    "RETURNING id;",
                    (instance.topic, 0, instance.avro_bytes),
                )
                return (await cur.fetchone())[0]


if __name__ == "__main__":
    unittest.main()
