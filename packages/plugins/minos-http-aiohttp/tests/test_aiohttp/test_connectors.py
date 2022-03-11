import unittest
from socket import (
    socket,
)
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from aiohttp import (
    web,
    web_runner,
)
from aiohttp.test_utils import (
    AioHTTPTestCase,
)
from aiohttp.web_exceptions import (
    HTTPInternalServerError,
)
from orjson import (
    orjson,
)

from minos.networks import (
    REQUEST_USER_CONTEXT_VAR,
    Request,
    Response,
)
from minos.plugins.aiohttp import (
    AioHttpConnector,
    AioHttpResponse,
    AioHttpResponseException,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    json_mocked_request,
    mocked_request,
)

APPLICATION_JSON = "application/json"


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return AioHttpResponse(await request.content())

    @staticmethod
    async def _fn_status(request: Request) -> Response:
        return AioHttpResponse(status=await request.content())

    @staticmethod
    async def _fn_none(request: Request):
        return

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise AioHttpResponseException("")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestAioHttpConnector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.connector = AioHttpConnector.from_config(CONFIG_FILE_PATH)

    def test_shutdown_timeout(self):
        self.assertEqual(6, self.connector.shutdown_timeout)

    def test_runner(self):
        self.assertEqual(None, self.connector.runner)

    def test_socket(self):
        self.assertEqual(None, self.connector.socket)

    def test_site(self):
        self.assertEqual(None, self.connector.site)

    def test_application(self):
        self.assertIsInstance(self.connector.application, web.Application)

    async def test_get_callback(self):
        handler = self.connector._adapt_callback(_Cls._fn)
        response = await handler(json_mocked_request({"foo": "bar"}))
        self.assertIsInstance(response, web.Response)
        self.assertEqual(orjson.dumps({"foo": "bar"}), response.body)
        self.assertEqual(APPLICATION_JSON, response.content_type)

    async def test_get_callback_status(self):
        handler = self.connector._adapt_callback(_Cls._fn_status)
        response = await handler(json_mocked_request(203))
        self.assertIsInstance(response, web.Response)
        self.assertEqual(None, response.body)
        self.assertEqual(APPLICATION_JSON, response.content_type)
        self.assertEqual(203, response.status)

    async def test_get_callback_none(self):
        handler = self.connector._adapt_callback(_Cls._fn_none)
        response = await handler(mocked_request())
        self.assertIsInstance(response, web.Response)
        self.assertEqual(None, response.text)
        self.assertEqual(APPLICATION_JSON, response.content_type)

    async def test_get_callback_raises_response(self):
        handler = self.connector._adapt_callback(_Cls._fn_raises_response)
        response = await handler(json_mocked_request({"foo": "bar"}))
        self.assertEqual(400, response.status)

    async def test_get_callback_raises_exception(self):
        handler = self.connector._adapt_callback(_Cls._fn_raises_exception)
        with self.assertRaises(HTTPInternalServerError):
            await handler(json_mocked_request({"foo": "bar"}))

    async def test_get_callback_with_user(self):
        user = uuid4()

        async def _fn(request) -> None:
            self.assertEqual(user, request.user)
            self.assertEqual(user, REQUEST_USER_CONTEXT_VAR.get())

        mock = AsyncMock(side_effect=_fn)

        handler = self.connector._adapt_callback(mock)
        await handler(json_mocked_request({"foo": "bar"}, user=user))

        self.assertEqual(1, mock.call_count)

    async def test_setup_destroy(self):
        self.assertIsNone(self.connector.socket)
        self.assertIsNone(self.connector.runner)
        self.assertIsNone(self.connector.site)

        async with self.connector:
            self.assertIsInstance(self.connector.socket, socket)
            self.assertIsInstance(self.connector.runner, web_runner.AppRunner)
            self.assertIsInstance(self.connector.site, web_runner.SockSite)

        self.assertIsNone(self.connector.socket)
        self.assertIsNone(self.connector.runner)
        self.assertIsNone(self.connector.site)


class TestAioHttpConnectorApplication(AioHTTPTestCase):
    async def get_application(self) -> web.Application:
        connector = AioHttpConnector.from_config(CONFIG_FILE_PATH)
        connector.mount_routes()
        return connector.application

    async def test_setup_destroy(self):
        url = "/order"
        resp = await self.client.request("GET", url)
        assert resp.status == 200
        text = await resp.text()
        assert "get_order" in text

        url = "/ticket"
        resp = await self.client.request("POST", url)
        assert resp.status == 200
        text = await resp.text()
        assert "ticket_added" in text


if __name__ == "__main__":
    unittest.main()
