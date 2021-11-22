import unittest
from asyncio import (
    Queue,
    TimeoutError,
    gather,
    wait_for,
)
from collections import (
    defaultdict,
    namedtuple,
)
from random import (
    shuffle,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    PropertyMock,
    call,
    patch,
)
from uuid import (
    uuid4,
)

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    USER_CONTEXT_VAR,
    BrokerHandler,
    BrokerHandlerEntry,
    BrokerMessage,
    BrokerMessageStatus,
    BrokerPublisher,
    BrokerRequest,
    BrokerResponse,
    BrokerResponseException,
    MinosActionNotFoundException,
    Request,
    Response,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    FakeRequest,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return BrokerResponse(await request.content())

    @staticmethod
    async def _fn_none(request: Request):
        await request.content()

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise BrokerResponseException("foo")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.publisher = BrokerPublisher.from_config(self.config)
        self.handler = BrokerHandler.from_config(self.config, publisher=self.publisher)

        self.saga = uuid4()
        self.user = uuid4()
        self.service_name = self.config.service.name

        self.message = BrokerMessage(
            "AddOrder", FakeModel("foo"), self.service_name, saga=self.saga, user=self.user, reply_topic="UpdateTicket",
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.handler.setup()

    async def asyncTearDown(self):
        await self.handler.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        self.assertIsInstance(self.handler, BrokerHandler)

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
        self.assertEqual(self.publisher, self.handler.publisher)

    async def test_get_action(self):
        action = self.handler.get_action(topic="AddOrder")
        self.assertEqual(BrokerResponse("add_order"), await action(FakeRequest("test")))

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
        queue = Queue()
        queue.put_nowait(None)
        with patch("aiopg.Connection.notifies", new_callable=PropertyMock, return_value=queue):
            with self.assertRaises(ValueError):
                await self.handler.dispatch_forever()

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
        handler = BrokerHandler(handlers=dict(), **self.config.broker.queue._asdict())
        mock = AsyncMock()
        async with handler:
            handler.dispatch = mock
            try:
                await wait_for(handler.dispatch_forever(max_wait=0.1), 0.5)
            except TimeoutError:
                pass
        self.assertEqual(0, mock.call_count)

    async def test_dispatch(self):
        callback_mock = AsyncMock(return_value=Response("add_order"))
        lookup_mock = MagicMock(return_value=callback_mock)
        entry = BrokerHandlerEntry(1, "AddOrder", 0, self.message.avro_bytes, 1, callback_lookup=lookup_mock)

        send_mock = AsyncMock()
        self.publisher.send = send_mock

        await self.handler.dispatch_one(entry)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("AddOrder"), lookup_mock.call_args)

        self.assertEqual(
            [
                call(
                    "add_order",
                    topic="UpdateTicket",
                    saga=self.message.saga,
                    status=BrokerMessageStatus.SUCCESS,
                    user=self.user,
                )
            ],
            send_mock.call_args_list,
        )

        self.assertEqual(1, callback_mock.call_count)
        observed = callback_mock.call_args[0][0]
        self.assertIsInstance(observed, BrokerRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())

    async def test_dispatch_wrong(self):
        instance_1 = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))
        instance_2 = BrokerMessage("NoActionTopic", FakeModel("Foo"), self.service_name)

        queue_id_1 = await self._insert_one(instance_1)
        queue_id_2 = await self._insert_one(instance_2)
        await self.handler.dispatch()
        self.assertFalse(await self._is_processed(queue_id_1))
        self.assertFalse(await self._is_processed(queue_id_2))

    async def test_dispatch_concurrent(self):
        from tests.utils import (
            FakeModel,
        )

        saga = uuid4()

        instance = BrokerMessage(
            "AddOrder", [FakeModel("foo")], self.service_name, saga=saga, reply_topic="UpdateTicket"
        )
        instance_wrong = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))

        for _ in range(10):
            await self._insert_one(instance)
            await self._insert_one(instance_wrong)

        self.assertEqual(20, await self._count())

        await gather(*(self.handler.dispatch() for _ in range(2)))
        self.assertEqual(10, await self._count())

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
        event = BrokerMessage(topic, FakeModel("Foo"), self.service_name)
        entry = BrokerHandlerEntry(1, topic, 0, event.avro_bytes, 1, callback_lookup=lookup_mock)

        await self.handler.dispatch_one(entry)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("TicketAdded"), lookup_mock.call_args)

        self.assertEqual(1, callback_mock.call_count)
        self.assertEqual(call(BrokerRequest(event)), callback_mock.call_args)

    async def test_get_callback(self):
        fn = self.handler.get_callback(_Cls._fn)
        self.assertEqual((FakeModel("foo"), BrokerMessageStatus.SUCCESS), await fn(self.message))

    async def test_get_callback_none(self):
        fn = self.handler.get_callback(_Cls._fn_none)
        self.assertEqual((None, BrokerMessageStatus.SUCCESS), await fn(self.message))

    async def test_get_callback_raises_response(self):
        fn = self.handler.get_callback(_Cls._fn_raises_response)
        expected = (repr(BrokerResponseException("foo")), BrokerMessageStatus.ERROR)
        self.assertEqual(expected, await fn(self.message))

    async def test_get_callback_raises_exception(self):
        fn = self.handler.get_callback(_Cls._fn_raises_exception)
        expected = (repr(ValueError()), BrokerMessageStatus.SYSTEM_ERROR)
        self.assertEqual(expected, await fn(self.message))

    async def test_get_callback_with_user(self):
        async def _fn(request) -> None:
            self.assertEqual(self.user, request.user)
            self.assertEqual(self.user, USER_CONTEXT_VAR.get())

        mock = AsyncMock(side_effect=_fn)

        handler = self.handler.get_callback(mock)
        await handler(self.message)

        self.assertEqual(1, mock.call_count)

    async def test_dispatch_without_sorting(self):
        observed = list()

        async def _fn2(request):
            content = await request.content()
            observed.append(content)

        self.handler.get_action = MagicMock(return_value=_fn2)

        events = [
            BrokerMessage("TicketAdded", FakeModel("uuid1"), self.service_name),
            BrokerMessage("TicketAdded", FakeModel("uuid2"), self.service_name),
        ]

        for event in events:
            await self._insert_one(event)

        await self.handler.dispatch()

        expected = [FakeModel("uuid1"), FakeModel("uuid2")]
        self.assertEqual(expected, observed)

    async def test_dispatch_with_order(self):
        observed = defaultdict(list)

        async def _fn2(request):
            content = await request.content()
            observed[content[0]].append(content[1])

        self.handler.get_action = MagicMock(return_value=_fn2)

        events = list()
        for i in range(1, 6):
            events.extend(
                [
                    BrokerMessage("TicketAdded", ["uuid1", i], self.service_name),
                    BrokerMessage("TicketAdded", ["uuid2", i], self.service_name),
                ]
            )
        shuffle(events)

        for event in events:
            await self._insert_one(event)

        await self.handler.dispatch()

        expected = {"uuid1": list(range(1, 6)), "uuid2": list(range(1, 6))}
        self.assertEqual(expected, observed)

    async def _notify(self, name):
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
