from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp import web
from minos.networks.rest_interface import RestInterfaceHandler
from minos.common.configuration.config import (
    MinosConfig,
)
from tests.utils import BASE_PATH


class MyAppTestCase(AioHTTPTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def get_application(self):
        """
        Override the get_app method to return your application.
        """
        rest_interface = RestInterfaceHandler(config=MinosConfig(self.CONFIG_FILE_PATH))

        return rest_interface.get_app()

    @unittest_run_loop
    async def test_methods(self):
        url = "/order"
        resp = await self.client.request("GET", url)
        assert resp.status == 200
        text = await resp.text()
        assert "Order get" in text

        resp = await self.client.request("POST", url)
        assert resp.status == 200
        text = await resp.text()
        assert "Order added" in text
