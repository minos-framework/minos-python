import unittest

from aiohttp.test_utils import (
    AioHTTPTestCase,
)

from minos.common import (
    MinosConfig,
)
from minos.plugins.rest_aiohttp import (
    RestService,
)
from tests.utils import (
    BASE_PATH,
)


class TestRestService(AioHTTPTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def get_application(self):
        """
        Override the get_app method to return your application.
        """
        config = MinosConfig(self.CONFIG_FILE_PATH)
        rest_interface = RestService(config=config)

        return await rest_interface.create_application()

    async def test_methods(self):
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

        resp = await self.client.request("GET", "/system/health")
        assert resp.status == 200


if __name__ == "__main__":
    unittest.main()
