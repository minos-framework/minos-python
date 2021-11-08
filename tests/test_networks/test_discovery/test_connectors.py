import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    DiscoveryConnector,
    MinosDiscoveryClient,
    MinosInvalidDiscoveryClient,
    get_host_ip,
)
from tests.utils import (
    BASE_PATH,
)


class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        self.config = MinosConfig(self.CONFIG_FILE_PATH)
        self.ip = get_host_ip()
        self.discovery = DiscoveryConnector.from_config(config=self.config)

    def test_config_minos_client_does_not_exist(self):
        config = MinosConfig(self.CONFIG_FILE_PATH, minos_discovery_client="wrong-client")
        with self.assertRaises(MinosInvalidDiscoveryClient):
            DiscoveryConnector.from_config(config=config)

    def test_config_minos_client_not_supported(self):
        config = MinosConfig(self.CONFIG_FILE_PATH, minos_discovery_client="minos.common.Model")
        with self.assertRaises(MinosInvalidDiscoveryClient):
            DiscoveryConnector.from_config(config)

    def test_client(self):
        self.assertIsInstance(self.discovery.client, MinosDiscoveryClient)

    async def test_subscription(self):
        mock = AsyncMock()
        self.discovery.client.subscribe = mock
        await self.discovery.subscribe()
        self.assertEqual(1, mock.call_count)
        expected = call(
            self.ip, 8080, "Order", [{"url": "/order", "method": "GET"}, {"url": "/ticket", "method": "POST"}]
        )
        self.assertEqual(expected, mock.call_args)

    async def test_unsubscribe(self):
        mock = AsyncMock()
        self.discovery.client.unsubscribe = mock
        await self.discovery.unsubscribe()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("Order"), mock.call_args)

    async def test_async_context_manager(self):
        mock_start = AsyncMock()
        mock_end = AsyncMock()
        self.discovery.subscribe = mock_start
        self.discovery.unsubscribe = mock_end

        self.assertEqual(0, mock_start.call_count)
        self.assertEqual(0, mock_end.call_count)

        async with self.discovery:
            self.assertEqual(1, mock_start.call_count)
            self.assertEqual(0, mock_end.call_count)

        self.assertEqual(1, mock_start.call_count)
        self.assertEqual(1, mock_end.call_count)


if __name__ == "__main__":
    unittest.main()
