import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    Event,
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

        self.assertEqual(self.config.events.queue.records, self.handler._records)
        self.assertEqual(self.config.events.queue.retry, self.handler._retry)
        self.assertEqual(self.config.events.queue.host, self.handler.host)
        self.assertEqual(self.config.events.queue.port, self.handler.port)
        self.assertEqual(self.config.events.queue.database, self.handler.database)
        self.assertEqual(self.config.events.queue.user, self.handler.user)
        self.assertEqual(self.config.events.queue.password, self.handler.password)

    def test_entry_model_cls(self):
        self.assertEqual(Event, EventHandler.ENTRY_MODEL_CLS)

    async def test_dispatch_one(self):
        mock = AsyncMock()
        topic = "TicketAdded"
        event = Event(topic, FAKE_AGGREGATE_DIFF)
        entry = HandlerEntry(1, topic, mock, 0, event, 1, datetime.now())

        async with self.handler:
            await self.handler.dispatch_one(entry)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(HandlerRequest(event)), mock.call_args)

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


if __name__ == "__main__":
    unittest.main()
