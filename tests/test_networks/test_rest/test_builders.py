import unittest

from aiohttp import (
    web,
)
from aiohttp.web_exceptions import (
    HTTPBadRequest,
    HTTPInternalServerError,
)
from yarl import (
    URL,
)

from minos.common import (
    ModelType,
    Request,
    Response,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    HttpRequest,
    HttpResponse,
    HttpResponseException,
    MinosActionNotFoundException,
    RestBuilder,
)
from tests.utils import (
    BASE_PATH,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return HttpResponse(await request.content())

    @staticmethod
    async def _fn_none(request: Request):
        return

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise HttpResponseException("")

    @staticmethod
    async def _fn_raises_minos(request: Request) -> Response:
        raise MinosActionNotFoundException("")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class MockedRequest:
    def __init__(self, data=None):
        self.data = data
        self.remote = "127.0.0.1"
        self.rel_url = URL("localhost")
        self.match_info = dict()

    async def json(self):
        return self.data


class TestRestBuilder(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.dispatcher = RestBuilder.from_config(config=self.config)

    def test_from_config(self):
        self.assertIsInstance(self.dispatcher, RestBuilder)

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestBuilder.from_config()

    def test_get_app(self):
        self.assertIsInstance(self.dispatcher.get_app(), web.Application)

    async def test_get_callback(self):
        handler = self.dispatcher.get_callback(_Cls._fn)
        response = await handler(MockedRequest({"foo": "bar"}))
        self.assertIsInstance(response, web.Response)
        self.assertEqual('{"foo": "bar"}', response.text)
        self.assertEqual("application/json", response.content_type)

    async def test_get_callback_none(self):
        handler = self.dispatcher.get_callback(_Cls._fn_none)
        response = await handler(MockedRequest())
        self.assertIsInstance(response, web.Response)
        self.assertEqual(None, response.text)
        self.assertEqual("application/json", response.content_type)

    async def test_get_callback_raises_response(self):
        handler = self.dispatcher.get_callback(_Cls._fn_raises_response)
        with self.assertRaises(HTTPBadRequest):
            await handler(MockedRequest({"foo": "bar"}))

    async def test_get_callback_raises_minos(self):
        handler = self.dispatcher.get_callback(_Cls._fn_raises_minos)
        with self.assertRaises(HTTPInternalServerError):
            await handler(MockedRequest({"foo": "bar"}))

    async def test_get_callback_raises_exception(self):
        handler = self.dispatcher.get_callback(_Cls._fn_raises_exception)
        with self.assertRaises(HTTPInternalServerError):
            await handler(MockedRequest({"foo": "bar"}))


if __name__ == "__main__":
    unittest.main()
