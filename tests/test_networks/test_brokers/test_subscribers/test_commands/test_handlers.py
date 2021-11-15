import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    USER_CONTEXT_VAR,
    Command,
    CommandHandler,
    CommandStatus,
    HandlerEntry,
    HandlerRequest,
    HandlerResponse,
    HandlerResponseException,
    Request,
    Response,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeModel,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return HandlerResponse(await request.content())

    @staticmethod
    async def _fn_none(request: Request):
        return

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise HandlerResponseException("foo")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestCommandHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.command_reply_broker = FakeBroker()
        self.handler = CommandHandler.from_config(config=self.config, command_reply_broker=self.command_reply_broker)
        self.saga = uuid4()
        self.user = uuid4()
        self.command = Command("AddOrder", FakeModel("foo"), saga=self.saga, user=self.user, reply_topic="UpdateTicket")

    def test_from_config(self):
        broker = FakeBroker()
        handler = CommandHandler.from_config(config=self.config, command_reply_broker=broker)
        self.assertIsInstance(handler, CommandHandler)

        self.assertEqual({"GetOrder", "AddOrder", "DeleteOrder", "UpdateOrder"}, set(handler.handlers.keys()))

        self.assertEqual(self.config.broker.queue.retry, handler._retry)
        self.assertEqual(self.config.broker.queue.host, handler.host)
        self.assertEqual(self.config.broker.queue.port, handler.port)
        self.assertEqual(self.config.broker.queue.database, handler.database)
        self.assertEqual(self.config.broker.queue.user, handler.user)
        self.assertEqual(self.config.broker.queue.password, handler.password)
        self.assertEqual(broker, handler.command_reply_broker)

    async def test_dispatch(self):
        callback_mock = AsyncMock(return_value=Response("add_order"))
        lookup_mock = MagicMock(return_value=callback_mock)
        entry = HandlerEntry(1, "AddOrder", 0, self.command.avro_bytes, 1, callback_lookup=lookup_mock)

        async with self.handler:
            await self.handler.dispatch_one(entry)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("AddOrder"), lookup_mock.call_args)

        self.assertEqual(1, self.command_reply_broker.call_count)
        self.assertEqual("add_order", self.command_reply_broker.items)
        self.assertEqual("UpdateTicket", self.command_reply_broker.topic)
        self.assertEqual(self.command.saga, self.command_reply_broker.saga)
        self.assertEqual(None, self.command_reply_broker.reply_topic)
        self.assertEqual(CommandStatus.SUCCESS, self.command_reply_broker.status)

        self.assertEqual(1, callback_mock.call_count)
        observed = callback_mock.call_args[0][0]
        self.assertIsInstance(observed, HandlerRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())

    async def test_get_callback(self):
        fn = self.handler.get_callback(_Cls._fn)
        self.assertEqual((FakeModel("foo"), CommandStatus.SUCCESS), await fn(self.command))

    async def test_get_callback_none(self):
        fn = self.handler.get_callback(_Cls._fn_none)
        self.assertEqual((None, CommandStatus.SUCCESS), await fn(self.command))

    async def test_get_callback_raises_response(self):
        fn = self.handler.get_callback(_Cls._fn_raises_response)
        expected = (repr(HandlerResponseException("foo")), CommandStatus.ERROR)
        self.assertEqual(expected, await fn(self.command))

    async def test_get_callback_raises_exception(self):
        fn = self.handler.get_callback(_Cls._fn_raises_exception)
        expected = (repr(ValueError()), CommandStatus.SYSTEM_ERROR)
        self.assertEqual(expected, await fn(self.command))

    async def test_get_callback_with_user(self):
        async def _fn(request) -> None:
            self.assertEqual(self.user, request.user)
            self.assertEqual(self.user, USER_CONTEXT_VAR.get())

        mock = AsyncMock(side_effect=_fn)

        handler = self.handler.get_callback(mock)
        await handler(self.command)

        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
