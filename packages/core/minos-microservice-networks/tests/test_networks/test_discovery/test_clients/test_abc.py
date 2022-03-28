import unittest
from abc import (
    ABC,
)

from minos.common import (
    Object,
)
from minos.networks import (
    DiscoveryClient,
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
    def test_abstract(self):
        self.assertTrue(issubclass(DiscoveryClient, (ABC, Object)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"subscribe", "unsubscribe"}, DiscoveryClient.__abstractmethods__)

    def test_route(self):
        client = _DiscoveryClient("localhost", 9999)
        self.assertEqual("http://localhost:9999", client.route)


if __name__ == "__main__":
    unittest.main()
