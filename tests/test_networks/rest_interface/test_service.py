from aiohttp.test_utils import (
    AioHTTPTestCase,
    unittest_run_loop,
)
from minos.common.configuration.config import (
    MinosConfig,
)
from minos.networks import (
    REST,
)
from tests.utils import (
    BASE_PATH,
)


class TestRestInterfaceService(AioHTTPTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def get_application(self):
        """
        Override the get_app method to return your application.
        """
        config = MinosConfig(self.CONFIG_FILE_PATH)
        rest_interface = REST(config=config)

        return await rest_interface.create_application()

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

        resp = await self.client.request("GET", "/system/health")
        assert resp.status == 200
