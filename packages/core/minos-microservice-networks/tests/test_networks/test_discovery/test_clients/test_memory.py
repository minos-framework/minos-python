import unittest

from minos.networks import (
    InMemoryDiscoveryClient,
)


class TestInMemoryDiscoveryConnector(unittest.IsolatedAsyncioTestCase):
    def test_init(self):
        client = InMemoryDiscoveryClient("localhost", 9999)
        self.assertEqual("localhost", client.host)
        self.assertEqual(9999, client.port)
        self.assertFalse(client.is_subscribed)

    async def test_subscribe(self):
        client = InMemoryDiscoveryClient("localhost", 9999)
        await client.subscribe()
        self.assertTrue(client.is_subscribed)

    async def test_unsubscribe(self):
        client = InMemoryDiscoveryClient("localhost", 9999)
        await client.subscribe()
        await client.unsubscribe()
        self.assertFalse(client.is_subscribed)


if __name__ == "__main__":
    unittest.main()
