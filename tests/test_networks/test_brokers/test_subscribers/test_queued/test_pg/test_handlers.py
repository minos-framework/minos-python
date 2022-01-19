import unittest
from asyncio import (
    Queue,
    TimeoutError,
    gather,
    sleep,
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

from minos.common import (
    NotProvidedException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerDispatcher,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    InMemoryBrokerPublisher,
)
from minos.networks.brokers.subscribers.queued.repositories.pg.entries import (
    BrokerHandlerEntry,
)
from minos.networks.brokers.subscribers.queued.repositories.pg.handlers import (
    BrokerHandler,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


@unittest.skip("FIXME!")
class TestBrokerHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.dispatcher = BrokerDispatcher.from_config(self.config, publisher=self.publisher)
        self.handler = BrokerHandler.from_config(self.config, dispatcher=self.dispatcher)

        self.identifier = uuid4()

        self.message = BrokerMessageV1(
            topic="AddOrder",
            payload=BrokerMessageV1Payload(content=FakeModel("foo"), headers={"foo": "bar"}),
            identifier=self.identifier,
            reply_topic="UpdateTicket",
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.dispatcher.setup()
        await self.handler.setup()

    async def asyncTearDown(self):
        await self.handler.destroy()
        await self.dispatcher.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        self.assertIsInstance(self.handler, BrokerHandler)
        self.assertEqual(self.dispatcher, self.handler.dispatcher)

        self.assertEqual(
            {"AddOrder", "DeleteOrder", "GetOrder", "TicketAdded", "TicketDeleted", "UpdateOrder"},
            set(self.handler.topics),
        )

        self.assertEqual(self.config.broker.queue.records, self.handler._records)
        self.assertEqual(self.config.broker.queue.retry, self.handler._retry)
        self.assertEqual(self.config.broker.queue.host, self.handler.host)
        self.assertEqual(self.config.broker.queue.port, self.handler.port)
        self.assertEqual(self.config.broker.queue.database, self.handler.database)
        self.assertEqual(self.config.broker.queue.user, self.handler.user)
        self.assertEqual(self.config.broker.queue.password, self.handler.password)

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            BrokerHandler.from_config(self.config)

    async def test_consumers(self):
        mock = AsyncMock()
        consumer_concurrency = 3

        async def _fn_no_wait(*args, **kwargs):
            return

        async def _fn(*args, **kwargs):
            await sleep(60)

        self.dispatcher.get_action = MagicMock(side_effect=[_fn_no_wait, _fn, _fn, _fn, _fn_no_wait])

        async with BrokerHandler.from_config(
            self.config, dispatcher=self.dispatcher, consumer_concurrency=consumer_concurrency
        ) as handler:
            self.assertEqual(consumer_concurrency, len(handler.consumers))
            handler.submit_query = mock

            for _ in range(consumer_concurrency + 2):
                entry = BrokerHandlerEntry(1, "AddOrder", 0, self.message.avro_bytes, 1)
                await handler._queue.put(entry)
            await sleep(0.5)

        self.assertEqual(0, len(handler.consumers))
        self.assertEqual(
            [
                call(handler._queries["delete_processed"], (1,)),
                call(handler._queries["update_not_processed"], (1,)),
                call(handler._queries["update_not_processed"], (1,)),
                call(handler._queries["update_not_processed"], (1,)),
                call(handler._queries["update_not_processed"], (1,)),
            ],
            mock.call_args_list,
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
        handler = BrokerHandler.from_config(self.config, handlers=dict(), publisher=self.publisher)
        mock = AsyncMock()
        async with handler:
            handler.dispatch = mock
            try:
                await wait_for(handler.dispatch_forever(max_wait=0.1), 0.5)
            except TimeoutError:
                pass
        self.assertEqual(0, mock.call_count)

    async def test_dispatch_wrong(self):
        instance_1 = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))
        instance_2 = BrokerMessageV1("NoActionTopic", BrokerMessageV1Payload(FakeModel("Foo")))

        queue_id_1 = await self._insert_one(instance_1)
        queue_id_2 = await self._insert_one(instance_2)
        await self.handler.dispatch()
        self.assertFalse(await self._is_processed(queue_id_1))
        self.assertFalse(await self._is_processed(queue_id_2))

    async def test_dispatch_concurrent(self):
        instance_wrong = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))

        for _ in range(10):
            await self._insert_one(self.message)
            await self._insert_one(instance_wrong)

        self.assertEqual(20, await self._count())

        await gather(*(self.handler.dispatch() for _ in range(2)))
        self.assertEqual(10, await self._count())

    async def test_dispatch_without_ordering(self):
        observed = list()

        async def _fn2(request):
            content = await request.content()
            observed.append(content)

        self.dispatcher.get_action = MagicMock(return_value=_fn2)

        messages = [
            BrokerMessageV1("TicketAdded", BrokerMessageV1Payload(FakeModel("one"))),
            BrokerMessageV1("TicketAdded", BrokerMessageV1Payload(FakeModel(1))),
        ]

        for message in messages:
            await self._insert_one(message)

        await self.handler.dispatch()

        expected = [FakeModel("one"), FakeModel(1)]
        self.assertEqual(expected, observed)

    async def test_dispatch_with_ordering(self):
        observed = defaultdict(list)

        async def _fn2(request):
            content = await request.content()
            observed[content[0]].append(content[1])

        self.dispatcher.get_action = MagicMock(return_value=_fn2)

        messages = list()
        for i in range(1, 6):
            messages.extend(
                [
                    BrokerMessageV1("TicketAdded", BrokerMessageV1Payload(["uuid1", i])),
                    BrokerMessageV1("TicketAdded", BrokerMessageV1Payload(["uuid2", i])),
                ]
            )
        shuffle(messages)

        for event in messages:
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
