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
    HTTPInternalServerError,
)
from orjson import (
    orjson,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
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
    BASE_PATH,
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


@unittest.skip
class TestAioHttpConnector(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.connector = AioHttpConnector.from_config(config=self.config)

    def test_get_app(self):
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


if __name__ == "__main__":
    unittest.main()
