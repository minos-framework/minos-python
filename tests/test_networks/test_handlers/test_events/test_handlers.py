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
)
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
)


class TestEventHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        handler = EventHandler.from_config(config=self.config)
        self.assertIsInstance(handler, EventHandler)
        handlers = {
            "TicketAdded": {"action": "ticket_added", "controller": "tests.services.CqrsTestService.CqrsService"},
            "TicketDeleted": {"action": "ticket_deleted", "controller": "tests.services.CqrsTestService.CqrsService"},
        }
        self.assertEqual(handlers, handler.handlers)
        self.assertEqual(self.config.events.queue.records, handler._records)
        self.assertEqual(self.config.events.queue.retry, handler._retry)
        self.assertEqual(self.config.events.queue.host, handler.host)
        self.assertEqual(self.config.events.queue.port, handler.port)
        self.assertEqual(self.config.events.queue.database, handler.database)
        self.assertEqual(self.config.events.queue.user, handler.user)
        self.assertEqual(self.config.events.queue.password, handler.password)

    def test_entry_model_cls(self):
        self.assertEqual(Event, EventHandler.ENTRY_MODEL_CLS)

    async def test_dispatch_one(self):
        mock = AsyncMock()
        topic = "TicketAdded"
        event = Event(topic, FAKE_AGGREGATE_DIFF)
        entry = HandlerEntry(1, topic, mock, 0, event, 1, datetime.now())

        async with EventHandler.from_config(config=self.config) as handler:
            await handler.dispatch_one(entry)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(topic, event), mock.call_args)


if __name__ == "__main__":
    unittest.main()
