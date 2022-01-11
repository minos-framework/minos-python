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
    REQUEST_HEADERS_CONTEXT_VAR,
    REQUEST_USER_CONTEXT_VAR,
    BrokerDispatcher,
    BrokerMessage,
    BrokerMessageStatus,
    BrokerPublisher,
    BrokerRequest,
    BrokerResponse,
    BrokerResponseException,
    InMemoryRequest,
    MinosActionNotFoundException,
    Request,
    Response,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
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


class TestBrokerDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.publisher = BrokerPublisher.from_config(self.config)
        self.dispatcher = BrokerDispatcher.from_config(self.config, publisher=self.publisher)

        self.identifier = uuid4()
        self.user = uuid4()

        self.message = BrokerMessage(
            "AddOrder",
            FakeModel("foo"),
            identifier=self.identifier,
            user=self.user,
            reply_topic="UpdateTicket",
            headers={"foo": "bar"},
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.dispatcher.setup()

    async def asyncTearDown(self):
        await self.dispatcher.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        self.assertIsInstance(self.dispatcher, BrokerDispatcher)

        self.assertEqual(
            {"AddOrder", "DeleteOrder", "GetOrder", "TicketAdded", "TicketDeleted", "UpdateOrder"},
            set(self.dispatcher.actions.keys()),
        )

        self.assertEqual(self.publisher, self.dispatcher.publisher)

    async def test_actions(self):
        self.assertEqual(
            {"query_service_ticket_added", "command_service_ticket_added"},
            set(await self.dispatcher.actions["TicketAdded"](None)),
        )
        self.assertEqual("ticket_deleted", await self.dispatcher.actions["TicketDeleted"](None))

    async def test_get_action(self):
        action = self.dispatcher.get_action("AddOrder")
        self.assertEqual(BrokerResponse("add_order"), await action(InMemoryRequest("test")))

    async def test_get_action_raises(self):
        with self.assertRaises(MinosActionNotFoundException) as context:
            self.dispatcher.get_action("NotExisting")

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_get_callback(self):
        fn = self.dispatcher.get_callback(_Cls._fn)
        self.assertEqual((FakeModel("foo"), BrokerMessageStatus.SUCCESS, {"foo": "bar"}), await fn(self.message))

    async def test_get_callback_none(self):
        fn = self.dispatcher.get_callback(_Cls._fn_none)
        self.assertEqual((None, BrokerMessageStatus.SUCCESS, {"foo": "bar"}), await fn(self.message))

    async def test_get_callback_raises_response(self):
        fn = self.dispatcher.get_callback(_Cls._fn_raises_response)
        expected = (repr(BrokerResponseException("foo")), BrokerMessageStatus.ERROR, {"foo": "bar"})
        self.assertEqual(expected, await fn(self.message))

    async def test_get_callback_raises_exception(self):
        fn = self.dispatcher.get_callback(_Cls._fn_raises_exception)
        expected = (repr(ValueError()), BrokerMessageStatus.SYSTEM_ERROR, {"foo": "bar"})
        self.assertEqual(expected, await fn(self.message))

    async def test_get_callback_with_user(self):
        async def _fn(request) -> None:
            self.assertEqual(self.user, request.user)
            self.assertEqual(self.user, REQUEST_USER_CONTEXT_VAR.get())

        mock = AsyncMock(side_effect=_fn)

        action = self.dispatcher.get_callback(mock)
        await action(self.message)

        self.assertEqual(1, mock.call_count)

    async def test_get_callback_with_headers(self):
        async def _fn(request) -> None:
            self.assertEqual({"foo": "bar"}, request.raw.headers)
            REQUEST_HEADERS_CONTEXT_VAR.get()["bar"] = "foo"

        mock = AsyncMock(side_effect=_fn)

        action = self.dispatcher.get_callback(mock)
        _, _, observed = await action(self.message)

        self.assertEqual({"foo": "bar", "bar": "foo"}, observed)

    async def test_dispatch_with_response(self):
        callback_mock = AsyncMock(return_value=Response("add_order"))
        lookup_mock = MagicMock(return_value=callback_mock)
        self.dispatcher.get_action = lookup_mock

        send_mock = AsyncMock()
        self.publisher.send = send_mock

        await self.dispatcher.dispatch(self.message)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("AddOrder"), lookup_mock.call_args)

        self.assertEqual(
            [
                call(
                    "add_order",
                    topic="UpdateTicket",
                    identifier=self.message.identifier,
                    status=BrokerMessageStatus.SUCCESS,
                    user=self.user,
                    headers={"foo": "bar"},
                )
            ],
            send_mock.call_args_list,
        )

        self.assertEqual(1, callback_mock.call_count)
        observed = callback_mock.call_args[0][0]
        self.assertIsInstance(observed, BrokerRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())

    async def test_dispatch_without_response(self):
        callback_mock = AsyncMock()
        lookup_mock = MagicMock(return_value=callback_mock)

        self.dispatcher.get_action = lookup_mock

        topic = "TicketAdded"
        message = BrokerMessage(topic, FakeModel("Foo"))

        await self.dispatcher.dispatch(message)

        self.assertEqual(1, lookup_mock.call_count)
        self.assertEqual(call("TicketAdded"), lookup_mock.call_args)

        self.assertEqual(1, callback_mock.call_count)
        self.assertEqual(call(BrokerRequest(message)), callback_mock.call_args)


if __name__ == "__main__":
    unittest.main()
