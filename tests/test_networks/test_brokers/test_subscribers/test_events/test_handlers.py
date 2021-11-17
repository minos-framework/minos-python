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

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandHandler,
    CommandReplyBroker,
    Event,
    Handler,
    HandlerEntry,
    HandlerRequest,
    HandlerResponseException,
    Request,
)
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
    FakeModel,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request):
        await request.content()

    @staticmethod
    async def _fn_raises_response(request: Request):
        raise HandlerResponseException("")

    @staticmethod
    async def _fn_raises_exception(request: Request):
        raise ValueError


class TestEventHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.command_reply_broker = CommandReplyBroker.from_config(self.config)
        self.handler = CommandHandler.from_config(config=self.config, command_reply_broker=self.command_reply_broker)
        self.event = Event("TicketAdded", FAKE_AGGREGATE_DIFF)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.command_reply_broker.setup()
        await self.handler.setup()

    async def asyncTearDown(self):
        await self.handler.destroy()
        await self.command_reply_broker.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        self.assertIsInstance(self.handler, Handler)

        self.assertEqual(
            {"AddOrder", "DeleteOrder", "GetOrder", "TicketAdded", "TicketDeleted", "UpdateOrder"},
            set(self.handler.handlers.keys()),
        )

        self.assertEqual(self.config.broker.queue.records, self.handler._records)
        self.assertEqual(self.config.broker.queue.retry, self.handler._retry)
        self.assertEqual(self.config.broker.queue.host, self.handler.host)
        self.assertEqual(self.config.broker.queue.port, self.handler.port)
        self.assertEqual(self.config.broker.queue.database, self.handler.database)
        self.assertEqual(self.config.broker.queue.user, self.handler.user)
        self.assertEqual(self.config.broker.queue.password, self.handler.password)

    async def test_handlers(self):
        self.assertEqual(
            {"query_service_ticket_added", "command_service_ticket_added"},
            set(await self.handler.handlers["TicketAdded"](None)),
        )
        self.assertEqual("ticket_deleted", await self.handler.handlers["TicketDeleted"](None))

    async def test_dispatch_one(self):
        callback_mock = AsyncMock()
        lookup_mock = MagicMock(return_value=callback_mock)

        topic = "TicketAdded"
        event = Event(topic, FAKE_AGGREGATE_DIFF)
        entry = HandlerEntry(1, topic, 0, event.avro_bytes, 1, callback_lookup=lookup_mock)

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

    async def test_get_callback_raises_exception(self):
        fn = self.handler.get_callback(_Cls._fn_raises_exception)
        await fn(self.event)

    async def test_dispatch_without_sorting(self):
        observed = list()

        async def _fn2(request):
            content = await request.content()
            observed.append(content)

        self.handler.get_action = MagicMock(return_value=_fn2)

        events = [Event("TicketAdded", FakeModel("uuid1")), Event("TicketAdded", FakeModel("uuid2"))]

        for event in events:
            await self._insert_one(event)

        await self.handler.dispatch()

        expected = [FakeModel("uuid1"), FakeModel("uuid2")]
        self.assertEqual(expected, observed)

    async def test_dispatch_concurrent(self):
        observed = defaultdict(list)

        async def _fn2(request):
            content = await request.content()
            observed[content[0]].append(content[1])

        self.handler.get_action = MagicMock(return_value=_fn2)

        events = list()
        for i in range(1, 6):
            events.extend([Event("TicketAdded", ["uuid1", i]), Event("TicketAdded", ["uuid2", i])])
        shuffle(events)

        for event in events:
            await self._insert_one(event)

        await self.handler.dispatch()

        expected = {"uuid1": list(range(1, 6)), "uuid2": list(range(1, 6))}
        self.assertEqual(expected, observed)

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id;",
                    (instance.topic, 0, instance.avro_bytes),
                )
                return (await cur.fetchone())[0]


if __name__ == "__main__":
    unittest.main()
