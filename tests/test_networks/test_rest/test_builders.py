import unittest

from aiohttp import (
    web,
)

from minos.common import (
    Request,
    Response,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
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

    async def test_resolve_action(self):
        dispatcher = RestBuilder.from_config(config=self.config)

        observed = dispatcher.resolve_action(f"{__name__}._Cls", "_fn")

        observed_response = observed(MockedRequest("request"))
        response = await observed_response
        self.assertIsInstance(response, web.Response)
        self.assertEqual('["request"]', response.text)
        self.assertEqual("application/json", response.content_type)


if __name__ == "__main__":
    unittest.main()
