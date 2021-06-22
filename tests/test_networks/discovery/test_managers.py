import socket
import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    Discovery,
    MinosDiscoveryClient,
)
from tests.utils import (
    BASE_PATH,
)


async def _fn(*args, **kwargs):
    pass


class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        self.config = MinosConfig(self.CONFIG_FILE_PATH)
        self.host = socket.gethostbyname(socket.getfqdn())
        self.discovery = Discovery.from_config(config=self.config)

    def test_client(self):
        self.assertIsInstance(self.discovery.client, MinosDiscoveryClient)

    async def test_subscription(self):
        mock = MagicMock(side_effect=_fn)
        self.discovery.client.subscribe = mock
        await self.discovery.subscribe()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(self.host, 8080, "Order"), mock.call_args)

    async def test_unsubscribe(self):
        mock = MagicMock(side_effect=_fn)
        self.discovery.client.unsubscribe = mock
        await self.discovery.unsubscribe()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("Order"), mock.call_args)

    async def test_async_context_manager(self):
        mock_start = MagicMock(side_effect=_fn)
        mock_end = MagicMock(side_effect=_fn)
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
