"""
from aiohttp.test_utils import (
    AioHTTPTestCase,
    unittest_run_loop,
)
from minos.common import (
    MinosConfig,
)
from minos.networks import (
    RestInterfaceHandler,
)
from tests.utils import (
    BASE_PATH,
)

class TestInterface(AioHTTPTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def get_application(self):
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
"""
