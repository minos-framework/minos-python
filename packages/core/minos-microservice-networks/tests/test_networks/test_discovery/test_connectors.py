import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.common import (
    Config,
    MinosImportException,
)
from minos.networks import (
    DiscoveryClient,
    DiscoveryConnector,
    InMemoryDiscoveryClient,
    MinosInvalidDiscoveryClient,
    get_host_ip,
)
from tests.utils import (
    NetworksTestCase,
)


class TestDiscoveryConnector(NetworksTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.ip = get_host_ip()
        self.discovery = DiscoveryConnector.from_config(config=self.config)

    def test_constructor(self):
        connector = DiscoveryConnector(self.discovery.client, "foo", [], "192.168.1.32")

        self.assertEqual(self.discovery.client, connector.client)
        self.assertEqual("foo", connector.name)
        self.assertEqual([], connector.endpoints)
        self.assertEqual("192.168.1.32", connector.host)
        self.assertEqual(8080, connector.port)

    def test_from_config(self):
        connector = DiscoveryConnector.from_config(self.config)
        expected = [
            {"url": "/order", "method": "DELETE"},
            {"url": "/order", "method": "GET", "foo": "bar"},
            {"url": "/ticket", "method": "POST", "foo": "bar"},
        ]

        self.assertEqual(expected, connector.endpoints)
        self.assertIsInstance(connector.client, DiscoveryClient)

    def test_config_minos_client_does_not_exist(self):
        config = Config(self.get_config_file_path(), minos_discovery_client="wrong-client")
        with self.assertRaises(MinosImportException):
            DiscoveryConnector.from_config(config=config)

    def test_config_minos_client_not_supported(self):
        config = Config(self.get_config_file_path(), minos_discovery_client="minos.common.Model")
        with self.assertRaises(MinosInvalidDiscoveryClient):
            DiscoveryConnector.from_config(config)

    def test_client(self):
        self.assertIsInstance(self.discovery.client, InMemoryDiscoveryClient)

    async def test_subscription(self):
        mock = AsyncMock()
        self.discovery.client.subscribe = mock
        await self.discovery.subscribe()
        self.assertEqual(1, mock.call_count)
        expected = call(
            self.ip,
            8080,
            "Order",
            [
                {"url": "/order", "method": "DELETE"},
                {"url": "/order", "method": "GET", "foo": "bar"},
                {"url": "/ticket", "method": "POST", "foo": "bar"},
            ],
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
