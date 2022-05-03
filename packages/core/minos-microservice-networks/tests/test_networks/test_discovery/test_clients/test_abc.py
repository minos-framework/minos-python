import unittest
from abc import (
    ABC,
)

from minos.common import (
    Config,
    Object,
)
from minos.networks import (
    DiscoveryClient,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _DiscoveryClient(DiscoveryClient):
    """For testing purposes."""

    async def subscribe(
        self,
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        *args,
        **kwargs,
    ) -> None:
        """For testing purposes."""

    async def unsubscribe(self, name: str, *args, **kwargs) -> None:
        """For testing purposes."""


class TestDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_abstract(self):
        self.assertTrue(issubclass(DiscoveryClient, (ABC, Object)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"subscribe", "unsubscribe"}, DiscoveryClient.__abstractmethods__)

    def test_route(self):
        client = _DiscoveryClient("localhost", 9999)
        self.assertEqual("http://localhost:9999", client.route)

    def test_from_config(self):
        self.config = Config(self.CONFIG_FILE_PATH)
        client = _DiscoveryClient.from_config(config=self.config)
        self.assertEqual("discovery-service", client.host)
        self.assertEqual(8080, client.port)


if __name__ == "__main__":
    unittest.main()
