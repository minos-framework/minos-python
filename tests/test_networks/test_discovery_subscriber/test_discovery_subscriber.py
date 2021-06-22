import unittest

from minos.networks.discovery_subscriber.discovery_subscriber import (
    DiscoverySubscriber,
)


class TestDiscoverySubscriber(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.discovery_subscriber = DiscoverySubscriber()

    @unittest.skip
    async def test_subscription(self):
        await self.discovery_subscriber._setup()

    @unittest.skip
    async def test_unsubscribe(self):
        await self.discovery_subscriber._destroy()
