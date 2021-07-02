import unittest

from aiohttp import (
    web,
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
    RestBuilder,
)
from tests.utils import (
    BASE_PATH,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return HttpResponse(await request.content())


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

    def test_from_config(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        self.assertIsInstance(dispatcher, RestBuilder)

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestBuilder.from_config()

    def test_get_app(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        self.assertIsInstance(dispatcher.get_app(), web.Application)

    async def test_get_handler(self):
        dispatcher = RestBuilder.from_config(config=self.config)

        observed = dispatcher.get_handler(_Cls._fn)

        observed_response = observed(MockedRequest({"foo": "bar"}))
        response = await observed_response
        self.assertIsInstance(response, web.Response)
        self.assertEqual('[{"foo": "bar"}]', response.text)
        self.assertEqual("application/json", response.content_type)

    async def test_get_action(self):
        Content = ModelType.build("Content", {"foo": str})
        dispatcher = RestBuilder.from_config(config=self.config)

        observed = dispatcher.get_action(f"{__name__}._Cls", "_fn")

        observed_response = observed(HttpRequest(MockedRequest({"foo": "bar"})))
        response = await observed_response
        self.assertIsInstance(response, HttpResponse)
        self.assertEqual([Content(foo="bar")], await response.content())


if __name__ == "__main__":
    unittest.main()
