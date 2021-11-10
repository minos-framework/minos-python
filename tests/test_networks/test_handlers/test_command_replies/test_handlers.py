import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyHandler,
    HandlerEntry,
    PublishResponse,
    PublishResponseStatus,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    FakeSagaManager,
)


class TestCommandReplyHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        saga_manager = FakeSagaManager()
        handler = CommandReplyHandler.from_config(config=self.config, saga_manager=saga_manager)
        self.assertIsInstance(handler, CommandReplyHandler)
        handlers = {"OrderReply": None}
        self.assertEqual(handlers, handler.handlers)
        self.assertEqual(self.config.broker.queue.records, handler._records)
        self.assertEqual(self.config.broker.queue.retry, handler._retry)
        self.assertEqual(self.config.broker.queue.host, handler.host)
        self.assertEqual(self.config.broker.queue.port, handler.port)
        self.assertEqual(self.config.broker.queue.database, handler.database)
        self.assertEqual(self.config.broker.queue.user, handler.user)
        self.assertEqual(self.config.broker.queue.password, handler.password)
        self.assertEqual(saga_manager, handler.saga_manager)

    async def test_dispatch(self):
        saga_manager = FakeSagaManager()
        mock = AsyncMock()
        saga_manager._load_and_run = mock

        saga = uuid4()
        command = PublishResponse(
            "TicketAdded", [FakeModel("foo")], saga, PublishResponseStatus.SUCCESS, self.config.service.name
        )
        entry = HandlerEntry(1, "TicketAdded", 0, command.avro_bytes, 1)

        async with CommandReplyHandler.from_config(config=self.config, saga_manager=saga_manager) as handler:
            await handler.dispatch_one(entry)

        self.assertEqual(1, mock.call_count)
        expected = call(command, pause_on_disk=True, raise_on_error=False, return_execution=False)
        self.assertEqual(expected, mock.call_args)


if __name__ == "__main__":
    unittest.main()
