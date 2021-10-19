import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from aiohttp import (
    web,
)
from aiohttp.web_exceptions import (
    HTTPBadRequest,
    HTTPInternalServerError,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    USER_CONTEXT_VAR,
    Request,
    Response,
    RestHandler,
    RestResponse,
    RestResponseException,
)
from tests.test_networks.test_rest.utils import (
    MockedRequest,
)
from tests.utils import (
    BASE_PATH,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return RestResponse(await request.content())

    @staticmethod
    async def _fn_none(request: Request):
        return

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise RestResponseException("")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestRestHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.handler = RestHandler.from_config(config=self.config)

    def test_from_config(self):
        self.assertIsInstance(self.handler, RestHandler)
        self.assertEqual({("/order", "GET"), ("/ticket", "POST")}, set(self.handler.endpoints.keys()))

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestHandler.from_config()

    def test_get_app(self):
        self.assertIsInstance(self.handler.get_app(), web.Application)

    async def test_get_callback(self):
        handler = self.handler.get_callback(_Cls._fn)
        response = await handler(MockedRequest({"foo": "bar"}))
        self.assertIsInstance(response, web.Response)
        self.assertEqual('{"foo": "bar"}', response.text)
        self.assertEqual("application/json", response.content_type)

    async def test_get_callback_none(self):
        handler = self.handler.get_callback(_Cls._fn_none)
        response = await handler(MockedRequest())
        self.assertIsInstance(response, web.Response)
        self.assertEqual(None, response.text)
        self.assertEqual("application/json", response.content_type)

    async def test_get_callback_raises_response(self):
        handler = self.handler.get_callback(_Cls._fn_raises_response)
        with self.assertRaises(HTTPBadRequest):
            await handler(MockedRequest({"foo": "bar"}))

    async def test_get_callback_raises_exception(self):
        handler = self.handler.get_callback(_Cls._fn_raises_exception)
        with self.assertRaises(HTTPInternalServerError):
            await handler(MockedRequest({"foo": "bar"}))

    async def test_get_callback_with_user(self):
        user = uuid4()

        async def _fn(request) -> None:
            self.assertEqual(user, request.user)
            self.assertEqual(user, USER_CONTEXT_VAR.get())

        mock = AsyncMock(side_effect=_fn)

        handler = self.handler.get_callback(mock)
        await handler(MockedRequest({"foo": "bar"}, user=user))

        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
