import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Command,
    CommandStatus,
    Request,
    Response,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandHandler,
    CommandRequest,
    CommandResponse,
    CommandResponseException,
    HandlerEntry,
    MinosActionNotFoundException,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeModel,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return CommandResponse(await request.content())

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise CommandResponseException("")

    @staticmethod
    async def _fn_raises_minos(request: Request) -> Response:
        raise MinosActionNotFoundException("")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestCommandHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker = FakeBroker()
        self.handler = CommandHandler.from_config(config=self.config, broker=self.broker)
        self.command = Command("AddOrder", FakeModel("foo"), uuid4(), "UpdateTicket")

    def test_from_config(self):
        broker = FakeBroker()
        dispatcher = CommandHandler.from_config(config=self.config, broker=broker)
        self.assertIsInstance(dispatcher, CommandHandler)
        self.assertEqual(
            {
                "AddOrder": {"action": "add_order", "controller": "tests.services.CommandTestService.CommandService"},
                "DeleteOrder": {
                    "action": "delete_order",
                    "controller": "tests.services.CommandTestService.CommandService",
                },
                "GetOrder": {"action": "get_order", "controller": "tests.service.CommandTestService.CommandService"},
                "UpdateOrder": {
                    "action": "update_order",
                    "controller": "tests.services.CommandTestService.CommandService",
                },
            },
            dispatcher._handlers,
        )
        self.assertEqual(self.config.commands.queue.records, dispatcher._records)
        self.assertEqual(self.config.commands.queue.retry, dispatcher._retry)
        self.assertEqual(self.config.commands.queue.host, dispatcher.host)
        self.assertEqual(self.config.commands.queue.port, dispatcher.port)
        self.assertEqual(self.config.commands.queue.database, dispatcher.database)
        self.assertEqual(self.config.commands.queue.user, dispatcher.user)
        self.assertEqual(self.config.commands.queue.password, dispatcher.password)
        self.assertEqual(broker, dispatcher.broker)

    def test_entry_model_cls(self):
        self.assertEqual(Command, CommandHandler.ENTRY_MODEL_CLS)

    async def test_dispatch(self):
        mock = AsyncMock(return_value=Response("add_order"))

        entry = HandlerEntry(1, "AddOrder", mock, 0, self.command, 1, datetime.now())

        async with self.handler:
            await self.handler.dispatch_one(entry)

        self.assertEqual(1, self.broker.call_count)
        self.assertEqual("add_order", self.broker.items)
        self.assertEqual("UpdateTicket", self.broker.topic)
        self.assertEqual(self.command.saga, self.broker.saga_uuid)
        self.assertEqual(None, self.broker.reply_topic)
        self.assertEqual(CommandStatus.SUCCESS, self.broker.status)

        self.assertEqual(1, mock.call_count)
        observed = mock.call_args[0][0]
        self.assertIsInstance(observed, CommandRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())

    async def test_get_callback(self):
        fn = self.handler.get_callback(_Cls._fn)
        self.assertEqual((FakeModel("foo"), CommandStatus.SUCCESS), await fn(self.command))

    async def test_get_callback_raises_response(self):
        fn = self.handler.get_callback(_Cls._fn_raises_response)
        self.assertEqual((None, CommandStatus.ERROR), await fn(self.command))

    async def test_get_callback_raises_minos(self):
        fn = self.handler.get_callback(_Cls._fn_raises_minos)
        self.assertEqual((None, CommandStatus.SYSTEM_ERROR), await fn(self.command))

    async def test_get_callback_raises_exception(self):
        fn = self.handler.get_callback(_Cls._fn_raises_exception)
        self.assertEqual((None, CommandStatus.SYSTEM_ERROR), await fn(self.command))


if __name__ == "__main__":
    unittest.main()
